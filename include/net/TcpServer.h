#pragma once
// ============================================================================
// TcpServer.h - 基于 epoll 的 TCP 服务器（Reactor 中的"Acceptor + 连接管理"）
// ============================================================================
//
// 【职责】
//   1. 监听端口，accept 新连接，为每条连接注册 epoll 事件；
//   2. 每条连接挂一个 TcpConn（含读 Buffer / 写 Buffer）；
//   3. 读完的数据用 try_decode_frame 拆帧，把完整消息回调给上层（RaftNode）；
//   4. 发送数据走 out_buf：先尝试直接 write，写不完（内核发送缓冲满）
//      再注册 EPOLLOUT 继续写 —— 这就是"应用层发送队列"。
//
// 【为什么发送要队列化，不能直接 send？】
//   对端收得慢时内核发送缓冲区会满，send 返回 EAGAIN。
//   如果不处理就直接丢数据。正确做法：塞进 out_buf，
//   等 EPOLLOUT（缓冲区腾出空间）再接着写。
//
// 【线程模型】
//   所有回调都在 EventLoop 单线程内执行，全程无锁。
//   上层（RaftNode）也跑在同一个循环里，所以状态机天然线程安全。
// ============================================================================

#include "common/Buffer.h"
#include "net/EventLoop.h"

#include <functional>
#include <map>
#include <memory>
#include <string>

// ----------------------------------------------------------------------------
// TcpConn - 一条 TCP 连接的上下文
//   fd      : 连接 socket（-1 表示已关闭）
//   in_buf  : 收到的原始字节流（攒够一帧再解析，解决粘包）
//   out_buf : 待发送字节流（内核缓冲满时在这里排队，解决背压）
// ----------------------------------------------------------------------------
struct TcpConn {
    int fd = -1;
    Buffer in_buf;
    Buffer out_buf;

    using Ptr = std::shared_ptr<TcpConn>;
};

// 连接的基础监听事件：可读 + 对端关闭（EPOLLOUT 按需动态加减）
const uint32_t kBaseEvents = EPOLLIN | EPOLLRDHUP;

// 把 conn->out_buf 尽量写出去（写不完自动挂 EPOLLOUT，写完自动摘掉）
void flush_conn(EventLoop* loop, const TcpConn::Ptr& conn);

// 把 data 追加到连接的 out_buf 并尝试立即写出
// （内核缓冲满时自动挂起，等 EPOLLOUT 再续写 —— 由 flush_conn 处理）
void conn_send(EventLoop* loop, const TcpConn::Ptr& conn, const std::string& data);

class TcpServer {
public:
    using MessageCallback = std::function<void(const TcpConn::Ptr&,
                                               uint8_t type,
                                               const std::string& body)>;
    using ConnCallback = std::function<void(const TcpConn::Ptr&)>;

    // ip 一般填 "127.0.0.1" 或 "0.0.0.0"；port 为监听端口
    TcpServer(EventLoop* loop, const std::string& ip, uint16_t port);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    // 回调注册：必须在 start() 之前调用
    //   on_message : 收到一条完整协议帧（type + body 已拆好）
    //   on_close   : 连接断开（对端关闭/出错），上层清理关联状态
    void set_callbacks(MessageCallback on_message, ConnCallback on_close);

    bool start(); // 创建监听 socket 并注册到 epoll

private:
    void handle_accept(uint32_t events);               // listen_fd 就绪：accept
    void handle_conn_events(const TcpConn::Ptr& conn, uint32_t events); // 连接 fd 就绪
    void handle_read(const TcpConn::Ptr& conn);        // 读数据 + 拆帧 + 回调
    void handle_write(const TcpConn::Ptr& conn);       // 续写 out_buf
    void close_conn(const TcpConn::Ptr& conn);         // 关闭并清理


    EventLoop* loop_;
    std::string ip_;
    uint16_t port_;
    int listen_fd_ = -1;

    MessageCallback on_message_;
    ConnCallback on_close_;

    std::map<int, TcpConn::Ptr> conns_; // fd -> 连接（生命周期由本类管理）
};
