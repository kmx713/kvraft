// ============================================================================
// TcpClient.cpp - 非阻塞 TCP 客户端实现（带自动重连）
// ============================================================================

#include "net/TcpClient.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/Log.h"
#include "common/Protocol.h"

static const int kReconnectDelayMs = 500; // 断线后重连间隔

static void set_nonblock(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_nodelay(int fd) {
    int on = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
}

// 解析 "127.0.0.1:9002" -> host + port
static void parse_addr(const std::string& addr, std::string* host, uint16_t* port) {
    size_t pos = addr.rfind(':');
    *host = addr.substr(0, pos);
    *port = static_cast<uint16_t>(atoi(addr.c_str() + pos + 1));
}

TcpClient::TcpClient(EventLoop* loop, const std::string& addr)
    : loop_(loop), addr_(addr) {
    parse_addr(addr, &host_, &port_);
}

TcpClient::~TcpClient() {
    // 析构时若有连接，直接关掉（EventLoop 一般同时析构，无需精细注销）
    if (fd_ >= 0) ::close(fd_);
}

void TcpClient::set_callbacks(ConnectedCallback on_connected,
                              MessageCallback on_message,
                              ClosedCallback on_closed) {
    on_connected_ = std::move(on_connected);
    on_message_ = std::move(on_message);
    on_closed_ = std::move(on_closed);
}

// ============================================================================
// connect - 发起非阻塞连接
//
// 非阻塞 connect 立即返回 EINPROGRESS：三次握手在内核后台进行。
// 握手完成后该 fd 变为"可写"，由 EPOLLOUT 事件通知 -> handle_connect_done。
// ============================================================================
void TcpClient::connect() {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        LOG_ERROR("socket() failed: %s", strerror(errno));
        return;
    }
    set_nonblock(fd_);
    set_nodelay(fd_);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    addr.sin_addr.s_addr = ::inet_addr(host_.c_str());

    int r = ::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (r == 0) {
        // 本机回环可能立即成功（极少见）
        handle_connect_done();
        return;
    }
    if (errno == EINPROGRESS) {
        connecting_ = true;
        // 等握手完成：监听 EPOLLOUT（连接建立 -> 可写）
        loop_->add_fd(fd_, EPOLLOUT, [this](uint32_t ev) { handle_events(ev); });
        return;
    }
    // 立即失败（对端未监听等）：定时重试
    ::close(fd_);
    fd_ = -1;
    loop_->run_after(kReconnectDelayMs, [this]() { connect(); });
}

// ============================================================================
// handle_connect_done - 检查非阻塞 connect 的结果
//
// 【关键面试点】非阻塞 connect 不能只看"可写"：
//   握手失败（对端拒绝）同样会触发可写！必须用 getsockopt(SO_ERROR)
//   确认 errno 才能区分成功与失败。
// ============================================================================
void TcpClient::handle_connect_done() {
    int err = 0;
    socklen_t len = sizeof(err);
    ::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        // 握手失败：清理后重试
        ::close(fd_);
        fd_ = -1;
        loop_->run_after(kReconnectDelayMs, [this]() { connect(); });
        return;
    }

    // 连接成功：切到 TcpConn 形态，注册读事件
    connecting_ = false;
    conn_ = std::make_shared<TcpConn>();
    conn_->fd = fd_;
    loop_->mod_fd(fd_, kBaseEvents); // EPOLLOUT -> EPOLLIN|EPOLLRDHUP
    LOG_INFO("connected to %s (fd=%d)", addr_.c_str(), fd_);
    if (on_connected_) on_connected_();
}

// ============================================================================
// send - 发送一帧消息
// ============================================================================
void TcpClient::send(uint8_t type, const std::string& body) {
    if (!is_connected()) {
        // 未连接：丢弃。Raft 心跳每 300ms 重发一轮，丢包不影响正确性
        LOG_DEBUG("send to %s dropped: not connected", addr_.c_str());
        return;
    }
    conn_send(loop_, conn_, encode_frame(type, body));
}

// ============================================================================
// handle_events - fd 事件分发（连接中 / 已连接两种状态）
// ============================================================================
void TcpClient::handle_events(uint32_t events) {
    if (connecting_) {
        handle_connect_done(); // EPOLLOUT = 握手结果（成功或失败）
        return;
    }
    if (!conn_ || conn_->fd < 0) return;

    if (events & (EPOLLERR | EPOLLHUP)) {
        close_and_schedule_retry();
        return;
    }
    if (events & (EPOLLRDHUP | EPOLLIN)) {
        // 对端关闭或发来数据：读处理内部会区分
        handle_read(conn_);
        if (conn_ && conn_->fd < 0) return; // 读处理中已断开
    }
    if (events & EPOLLOUT) {
        flush_conn(loop_, conn_); // 续写积压数据（写完自动摘 EPOLLOUT）
    }
}

// ============================================================================
// handle_read - 读数据 + 拆帧（与 TcpServer 的逻辑一致，处理对端发来的回复）
// ============================================================================
void TcpClient::handle_read(const TcpConn::Ptr& conn) {
    while (true) {
        int saved_errno = 0;
        ssize_t n = conn->in_buf.read_fd(conn->fd, &saved_errno);

        if (n > 0) {
            while (conn->fd >= 0) {
                uint8_t type = 0;
                std::string body;
                int r = try_decode_frame(conn->in_buf.peek(),
                                         conn->in_buf.readable(), &type, &body);
                if (r == 0) break;
                if (r < 0) {
                    LOG_WARN("protocol error from %s, reconnecting", addr_.c_str());
                    close_and_schedule_retry();
                    return;
                }
                conn->in_buf.retrieve(static_cast<size_t>(r));
                if (on_message_) on_message_(conn, type, body);
            }
            continue;
        }
        if (n == 0) { // 对端关闭
            close_and_schedule_retry();
            return;
        }
        if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) return;
        if (saved_errno == EINTR) continue;
        close_and_schedule_retry();
        return;
    }
}

// ============================================================================
// close_and_schedule_retry - 断开清理 + 500ms 后自动重连
//
// 【对端宕机时收到什么？】
//   正常关闭：recv 返回 0（FIN）
//   进程被 kill -9 / 主机崩溃：本机要等内核 TCP 重传超时才发现，
//   期间 send 可能报 EPIPE —— 心跳丢几轮没关系，Raft 靠选举超时兜底。
// ============================================================================
void TcpClient::close_and_schedule_retry() {
    if (fd_ >= 0) {
        loop_->remove_fd(fd_); // close(fd)
        fd_ = -1;
    }
    if (conn_) {
        conn_->fd = -1;
        conn_.reset();
    }
    connecting_ = false;
    if (on_closed_) on_closed_();
    loop_->run_after(kReconnectDelayMs, [this]() { connect(); });
    LOG_INFO("disconnected from %s, retry in %dms", addr_.c_str(), kReconnectDelayMs);
}
