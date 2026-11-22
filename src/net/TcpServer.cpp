// ============================================================================
// TcpServer.cpp - epoll TCP 服务器实现（Part 1：监听与共用发送函数）
// ============================================================================

#include "net/TcpServer.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/Log.h"
#include "common/Protocol.h"

static void set_nonblock(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 开启 TCP_NODELAY：禁用 Nagle 算法，小包（心跳/RPC）立即发出
// Raft 心跳延迟直接决定故障恢复速度，必须关 Nagle
static void set_nodelay(int fd) {
    int on = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
}

// ============================================================================
// flush_conn - 把 conn->out_buf 里的数据尽量写出去（TcpServer/Client 共用）
//
//   写完  -> 取消 EPOLLOUT 监听（避免 busy loop）
//   没写完（EAGAIN）-> 保持 EPOLLOUT，内核缓冲腾出空间后由事件回调续写
// ============================================================================
void flush_conn(EventLoop* loop, const TcpConn::Ptr& conn) {
    if (conn->fd < 0) return;
    while (conn->out_buf.readable() > 0) {
        int saved_errno = 0;
        ssize_t n = conn->out_buf.write_fd(conn->fd, &saved_errno);
        if (n > 0) continue;           // 继续写剩余部分
        if (n < 0 && (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)) {
            // 内核发送缓冲满：监听 EPOLLOUT，等可写事件再续写（背压处理）
            loop->mod_fd(conn->fd, kBaseEvents | EPOLLOUT);
            return;
        }
        if (n < 0 && saved_errno == EINTR) continue; // 信号打断，重试
        // 其他错误（对端 RST 等）：移除 fd 并标记关闭，调用方据此清理
        loop->remove_fd(conn->fd);
        conn->fd = -1;
        return;
    }
    // 全部写完：去掉 EPOLLOUT
    loop->mod_fd(conn->fd, kBaseEvents);
}

// ============================================================================
// conn_send - 发送数据入口（TcpServer 与 TcpClient 共用）
// 流程：追加到 out_buf -> 尝试立即写出
// ============================================================================
void conn_send(EventLoop* loop, const TcpConn::Ptr& conn, const std::string& data) {
    if (conn->fd < 0) return; // 连接已关闭，静默丢弃
    conn->out_buf.append(data);
    flush_conn(loop, conn);
}

// ============================================================================
// TcpServer 构造 / 析构 / 回调注册
// ============================================================================
TcpServer::TcpServer(EventLoop* loop, const std::string& ip, uint16_t port)
    : loop_(loop), ip_(ip), port_(port) {}

TcpServer::~TcpServer() {
    if (listen_fd_ >= 0) ::close(listen_fd_);
}

void TcpServer::set_callbacks(MessageCallback on_message, ConnCallback on_close) {
    on_message_ = std::move(on_message);
    on_close_ = std::move(on_close);
}

// 创建监听 socket：socket -> REUSEADDR -> bind -> listen -> 注册 epoll
bool TcpServer::start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        LOG_ERROR("socket() failed: %s", strerror(errno));
        return false;
    }

    // REUSEADDR：服务重启时避免 "Address already in use"
    // （TIME_WAIT 状态的端口默认不能立即复用）
    int on = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_); // 端口转网络字节序（大端）
    addr.sin_addr.s_addr = ip_.empty() || ip_ == "0.0.0.0"
                               ? INADDR_ANY
                               : ::inet_addr(ip_.c_str());

    if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        LOG_ERROR("bind %s:%u failed: %s", ip_.c_str(), port_, strerror(errno));
        return false;
    }
    if (::listen(listen_fd_, 128) < 0) {
        LOG_ERROR("listen failed: %s", strerror(errno));
        return false;
    }

    set_nonblock(listen_fd_);
    loop_->add_fd(listen_fd_, EPOLLIN,
                  [this](uint32_t ev) { handle_accept(ev); });
    LOG_INFO("TcpServer listening on %s:%u", ip_.c_str(), port_);
    return true;
}

// ============================================================================
// handle_accept - 接收新连接
// LT 模式 + while 循环：一次可能来多个并发建连，全部收完为止（EAGAIN 退出）
// ============================================================================
void TcpServer::handle_accept(uint32_t events) {
    (void)events;
    while (true) {
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        int fd = ::accept(listen_fd_,
                          reinterpret_cast<struct sockaddr*>(&peer), &peer_len);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // 收完了
            if (errno == EINTR) continue;                       // 信号打断，重试
            LOG_ERROR("accept failed: %s", strerror(errno));
            break;
        }

        set_nonblock(fd);
        set_nodelay(fd);

        auto conn = std::make_shared<TcpConn>();
        conn->fd = fd;
        conns_[fd] = conn;

        loop_->add_fd(fd, kBaseEvents, [this, conn](uint32_t ev) {
            handle_conn_events(conn, ev);
        });
    }
}

// ============================================================================
// handle_conn_events - 连接 fd 事件分发
// ============================================================================
void TcpServer::handle_conn_events(const TcpConn::Ptr& conn, uint32_t events) {
    // 对端异常（RST）优先处理
    if (events & (EPOLLERR | EPOLLHUP)) {
        close_conn(conn);
        return;
    }
    // 对端半关闭（发了 FIN）：可能还带最后一段数据，先读再关
    if (events & EPOLLRDHUP) {
        handle_read(conn);
        if (conn->fd >= 0) close_conn(conn);
        return;
    }
    if (events & EPOLLOUT) {
        // 内核发送缓冲可写：续写积压数据（写完自动摘 EPOLLOUT）
        flush_conn(loop_, conn);
    }
    if (conn->fd < 0) return; // 上面可能已关闭
    if (events & EPOLLIN) handle_read(conn);
}

// ============================================================================
// handle_read - 读数据 + 拆帧 + 分发给上层
//
// 粘包处理核心循环：
//   1. recv 到数据进 in_buf；
//   2. try_decode_frame 尝试从 in_buf 头部解出完整帧：
//      - 0   ：数据不够一帧（拆包），跳出等下次 EPOLLIN；
//      - <0  ：协议错误（magic/CRC 不对），断连；
//      - >0  ：解出完整帧，回调上层，消费字节，继续解下一帧
//        （一次 recv 可能收到多条命令，必须循环解完 —— 粘包处理）。
// ============================================================================
void TcpServer::handle_read(const TcpConn::Ptr& conn) {
    while (true) {
        int saved_errno = 0;
        ssize_t n = conn->in_buf.read_fd(conn->fd, &saved_errno);

        if (n > 0) {
            // 循环解帧：把这次收到的数据全部解析成消息
            while (conn->fd >= 0) {
                uint8_t type = 0;
                std::string body;
                int r = try_decode_frame(conn->in_buf.peek(),
                                         conn->in_buf.readable(), &type, &body);
                if (r == 0) break; // 不够一帧，等更多数据
                if (r < 0) {       // 协议错误
                    LOG_WARN("protocol error on fd=%d, closing", conn->fd);
                    close_conn(conn);
                    return;
                }
                conn->in_buf.retrieve(static_cast<size_t>(r)); // 消费本帧
                if (on_message_) on_message_(conn, type, body); // 交给上层
            }
            continue; // LT 模式：再试一次 recv（可能还有数据）
        }
        if (n == 0) { // 对端正常关闭（FIN）
            close_conn(conn);
            return;
        }
        if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
            return; // 内核接收缓冲读空，正常退出等下次触发
        }
        if (saved_errno == EINTR) continue;
        LOG_DEBUG("recv error fd=%d: %s", conn->fd, strerror(saved_errno));
        close_conn(conn);
        return;
    }
}

// ============================================================================
// close_conn - 关闭连接：从 epoll 注销（内部 close fd）、从 conns_ 移除、通知上层
// ============================================================================
void TcpServer::close_conn(const TcpConn::Ptr& conn) {
    if (conn->fd < 0) return;
    int fd = conn->fd;
    loop_->remove_fd(fd);   // 内部 close(fd)
    conn->fd = -1;          // 标记已关闭（上层回调可据此判断）
    conns_.erase(fd);
    if (on_close_) on_close_(conn);
}
