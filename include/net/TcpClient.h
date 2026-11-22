#pragma once
// ============================================================================
// TcpClient.h - 非阻塞 TCP 客户端（Raft 节点间的"主动连接"通道）
// ============================================================================
//
// 【Raft 集群的连接拓扑】
//   每个节点对其他 N-1 个节点各维持一条"主动"连接（本类），
//   同时监听端口接收别人的主动连接（TcpServer）。
//   -> 节点 i 发给节点 j 的 RPC 走 i 的 client -> j 的 server，
//      回复走同一条连接（TCP 全双工），无需单独的回复连接。
//
// 【自动重连】
//   对端宕机/网络抖动时连接断开 -> run_after(500ms) 自动重连，
//   无限重试直到成功。Raft 心跳本身就有重试语义（每 300ms 一轮），
//   所以连接期间丢失的 RPC 会被下一轮心跳补上，不需要队列缓存。
//
// 【与 TcpServer 的复用】
//   连接建立后同样用 TcpConn 承载数据，收发/拆帧逻辑与服务器侧完全一致，
//   flush_conn/conn_send 两个自由函数直接复用（定义在 TcpServer.cpp）。
// ============================================================================

#include "net/TcpServer.h"

#include <functional>

class TcpClient {
public:
    using ConnectedCallback = std::function<void()>;
    // 收到完整协议帧（type + body 已拆好）
    using MessageCallback = std::function<void(const TcpConn::Ptr&,
                                               uint8_t type,
                                               const std::string& body)>;
    using ClosedCallback = std::function<void()>;

    // addr 格式 "127.0.0.1:9002"（Raft 集群配置里就是这个格式）
    TcpClient(EventLoop* loop, const std::string& addr);
    ~TcpClient();

    TcpClient(const TcpClient&) = delete;
    TcpClient& operator=(const TcpClient&) = delete;

    void set_callbacks(ConnectedCallback on_connected,
                       MessageCallback on_message,
                       ClosedCallback on_closed);

    void connect();          // 发起（重）连接，内部非阻塞 connect
    bool is_connected() const { return conn_ && conn_->fd >= 0; }

    // 发送一帧消息；未连接时静默丢弃（上层靠周期重试保证最终送达）
    void send(uint8_t type, const std::string& body);

    const std::string& addr() const { return addr_; }

private:
    void handle_events(uint32_t events);   // fd 就绪分发
    void handle_connect_done();            // 非阻塞 connect 完成检查
    void handle_read(const TcpConn::Ptr& conn);
    void close_and_schedule_retry();       // 清理 + 定时重连

    EventLoop* loop_;
    std::string addr_;
    std::string host_;
    uint16_t port_ = 0;

    int fd_ = -1;                  // socket fd（连接中或已连接）
    bool connecting_ = false;      // 正在三次握手（未完成）
    TcpConn::Ptr conn_;            // 连接成功后创建

    ConnectedCallback on_connected_;
    MessageCallback on_message_;
    ClosedCallback on_closed_;
};
