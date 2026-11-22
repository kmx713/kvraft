#pragma once
// ============================================================================
// RaftNode.h - Raft 一致性算法实现（P3 核心：选举 + 日志复制）
// ============================================================================

#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "net/TcpClient.h"
#include "net/TcpServer.h"
#include "storage/SkipList.h"

class RaftNode {
public:
    enum Role { FOLLOWER = 0, CANDIDATE = 1, LEADER = 2 };

    // my_id: 本节点编号（0~N-1）；peer_addrs: 全部节点 "ip:port"（含自己）；
    // data_dir: 数据目录（raft.wal / role / pid）
    RaftNode(int my_id, const std::vector<std::string>& peer_addrs,
             const std::string& data_dir);
    ~RaftNode();

    // 初始化网络、重放持久化日志、启动选举定时器，进入事件循环（阻塞）
    bool start();

private:
    // ==================== Raft 状态 ====================
    int my_id_;
    std::vector<std::string> addrs_;      // 全部节点地址（下标 = node id）
    std::string data_dir_;

    Role role_;                           // 当前角色
    uint64_t current_term_;               // 当前任期（单调递增）
    int voted_for_;                       // 本任期投给谁（-1 = 未投）
    // 日志。Raft 日志索引从 1 开始，log_[0] 是占位哨兵：log_[i] = 第 i 条
    std::vector<LogEntry> log_;
    uint64_t commit_index_;               // 已提交（多数派复制）的最高索引
    uint64_t last_applied_;               // 已应用到状态机的最高索引
    int leader_id_;                       // 当前认定的 Leader（-1 = 未知）
    int votes_granted_;                   // 竞选中已获得票数

    // Leader 专用：每个 Follower 的复制进度
    std::vector<uint64_t> next_index_;    // 下一个要发给 Follower i 的日志索引
    std::vector<uint64_t> match_index_;   // Follower i 已确认的最高日志索引

    // ==================== 状态机与挂起的客户端请求 ====================
    SkipList<std::string, std::string> sm_;
    // 日志索引 -> 等待提交结果的客户端连接（SET/DEL 提交成功后回复 OK）
    std::map<uint64_t, TcpConn::Ptr> pending_;

    // ==================== 定时器 ====================
    uint64_t election_timer_id_;    // 选举超时（单次，到点即竞选）
    uint64_t heartbeat_timer_id_;   // 心跳周期定时器（仅 Leader 持有）
    std::mt19937 rng_;              // 随机选举超时

    // ==================== 网络 ====================
    EventLoop loop_;
    TcpServer server_;                              // 接收客户端 + 其他节点
    std::vector<std::unique_ptr<TcpClient>> peers_; // 到每个节点的主动连接

    FILE* rwal_;                 // raft.wal 句柄
    std::string rwal_path_;

    // ==================== 持久化 ====================
    bool load_persisted_state();           // 启动时重放 raft.wal
    bool persist_meta();                   // 写 META（term/voted_for/commit）
    bool persist_entry(const LogEntry& e); // 追加 ENTRY
    void write_status_files();             // 更新 data/role（运维脚本用）

    // ==================== 角色转换 ====================
    void reset_election_timer();           // 重设随机选举超时
    void start_election();                 // 超时 -> 竞选拉票
    void become_follower(uint64_t term, int new_leader = -1);
    void become_leader();
    void fail_all_pending();               // 失去 Leader 身份时回复挂起客户端

    // ==================== RPC 处理 ====================
    void on_frame(const TcpConn::Ptr& conn, uint8_t type, const std::string& body);
    void handle_vote_req(const VoteReq& req, const TcpConn::Ptr& conn);
    void handle_vote_reply(const VoteReply& rep);
    void handle_append_req(const AppendReq& req, const TcpConn::Ptr& conn);
    void handle_append_reply(uint32_t peer_id, const AppendReply& rep);

    // ==================== 日志复制 ====================
    void replicate_to(uint32_t peer_id);   // 从 next_index_[i] 起发 AppendEntries
    void broadcast_append();               // 全量广播（心跳 + 复制）
    void advance_commit();                 // 多数派 match_index -> 推进 commit
    void apply_committed();                // 应用 [last_applied+1, commit] 到状态机

    // ==================== 客户端命令 ====================
    void handle_client(const TcpConn::Ptr& conn, const std::string& body);
    void send_resp(const TcpConn::Ptr& conn, const std::string& text);
    void send_redirect(const TcpConn::Ptr& conn);

    // ==================== 辅助 ====================
    uint64_t last_log_index() const { return log_.size() - 1; }
    uint64_t last_log_term() const { return log_.back().term; }
    bool is_leader() const { return role_ == LEADER; }
    int majority() const { return static_cast<int>(addrs_.size()) / 2 + 1; }
};
