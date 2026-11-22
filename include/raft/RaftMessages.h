#pragma once
// ============================================================================
// RaftMessages.h - Raft 节点间 RPC 的消息结构与二进制序列化
// ============================================================================
//
// 【两类 RPC】（Raft 论文定义）
//   1. RequestVote  ：选举时拉票
//   2. AppendEntries：心跳 + 日志复制（复用同一 RPC，entries 为空就是心跳）
//
// 【请求/回复如何区分？】
//   复用同一个消息类型（MSG_RAFT_VOTE / MSG_RAFT_APPEND），
//   body 首字节作 flag：0 = 请求，1 = 回复。
//
// 【序列化格式】全部小端（与帧协议、WAL 保持同一套约定）：
//   VoteReq    : flag(1) term(8) candidate_id(4) last_log_index(8) last_log_term(8)
//   VoteReply  : flag(1) term(8) granted(1)
//   AppendReq  : flag(1) term(8) leader_id(4) prev_log_index(8) prev_log_term(8)
//                leader_commit(8) entry_count(4) [entry]...
//   entry      : term(8) op(1) klen(4) vlen(4) key value
//   AppendReply: flag(1) term(8) success(1) match_index(8)
//
// 【为什么不用 protobuf？】
//   手写序列化没有外部依赖、体量可控，且能展示对二进制布局的掌控力；
//   生产环境当然推荐 protobuf（版本兼容、代码生成）。
// ============================================================================

#include <stdint.h>

#include <string>
#include <vector>

// 日志条目的操作类型
enum RaftOp : uint8_t {
    RAFT_OP_SET  = 0, // SET key value
    RAFT_OP_DEL  = 1, // DEL key
    RAFT_OP_NOOP = 2, // 领导者上任后的空操作（用于快速提交，见 RaftNode）
};

// LogEntry - Raft 日志条目（分布式系统的"操作记录"）
//   term : 产生该条目的任期；op : SET/DEL/NOOP；key/value : 状态机命令
struct LogEntry {
    uint64_t term = 0;
    uint8_t op = RAFT_OP_NOOP;
    std::string key;
    std::string value;
};

// ---------------------------------------------------------------------------
// RequestVote RPC
// ---------------------------------------------------------------------------
struct VoteReq {
    uint64_t term = 0;
    uint32_t candidate_id = 0;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
};

struct VoteReply {
    uint64_t term = 0;
    bool granted = false;
};

// ---------------------------------------------------------------------------
// AppendEntries RPC
//   prev_log_index/prev_log_term : 日志一致性检查锚点
//     - 要求接收者本地日志在 prev 位置之前的部分必须一致（日志匹配特性）
//     - 不一致就回复失败，领导者回退 next_index 重发 —— 日志修复机制
//   leader_commit : 领导者已提交的索引，跟随者据此推进自己的 commit
// ---------------------------------------------------------------------------
struct AppendReq {
    uint64_t term = 0;
    uint32_t leader_id = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;
    std::vector<LogEntry> entries;
};

struct AppendReply {
    uint64_t term = 0;
    bool success = false;
    uint64_t match_index = 0; // 成功时：接收者已拥有到 match_index 的日志
};

// ---------------------------------------------------------------------------
// 小端读写辅助（与 Protocol.cpp / WAL 同一套约定）
// ---------------------------------------------------------------------------
inline void raft_put_le16(std::string& b, uint16_t v) {
    b.push_back(static_cast<char>(v & 0xFF));
    b.push_back(static_cast<char>((v >> 8) & 0xFF));
}

inline void raft_put_le32(std::string& b, uint32_t v) {
    b.push_back(static_cast<char>(v & 0xFF));
    b.push_back(static_cast<char>((v >> 8) & 0xFF));
    b.push_back(static_cast<char>((v >> 16) & 0xFF));
    b.push_back(static_cast<char>((v >> 24) & 0xFF));
}

inline void raft_put_le64(std::string& b, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        b.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
    }
}

inline uint32_t raft_get_le32(const char* p, size_t* off) {
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p) + *off;
    *off += 4;
    return static_cast<uint32_t>(q[0]) | (static_cast<uint32_t>(q[1]) << 8) |
           (static_cast<uint32_t>(q[2]) << 16) | (static_cast<uint32_t>(q[3]) << 24);
}

inline uint64_t raft_get_le64(const char* p, size_t* off) {
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p) + *off;
    *off += 8;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(q[i]) << (8 * i);
    }
    return v;
}

// 追加字符串：长度前缀（4B）+ 内容
inline void raft_put_str(std::string& b, const std::string& s) {
    raft_put_le32(b, static_cast<uint32_t>(s.size()));
    b.append(s);
}

inline std::string raft_get_str(const char* p, size_t len, size_t* off) {
    uint32_t n = raft_get_le32(p, off);
    if (*off + n > len) n = static_cast<uint32_t>(len > *off ? len - *off : 0);
    std::string s(p + *off, n);
    *off += n;
    return s;
}

// ---------------------------------------------------------------------------
// 序列化 / 反序列化
// ---------------------------------------------------------------------------
inline std::string encode_vote_req(const VoteReq& m) {
    std::string b;
    b.push_back(static_cast<char>(0)); // 0 = 请求
    raft_put_le64(b, m.term);
    raft_put_le32(b, m.candidate_id);
    raft_put_le64(b, m.last_log_index);
    raft_put_le64(b, m.last_log_term);
    return b;
}

inline bool decode_vote_msg(const std::string& body, bool* is_reply,
                            VoteReq* req, VoteReply* rep) {
    if (body.size() < 1) return false;
    *is_reply = (body[0] != 0);
    size_t off = 1;
    if (*is_reply) {
        rep->term = raft_get_le64(body.data(), &off);
        rep->granted = raft_get_le32(body.data(), &off) != 0;
    } else {
        req->term = raft_get_le64(body.data(), &off);
        req->candidate_id = raft_get_le32(body.data(), &off);
        req->last_log_index = raft_get_le64(body.data(), &off);
        req->last_log_term = raft_get_le64(body.data(), &off);
    }
    return true;
}

inline std::string encode_vote_reply(const VoteReply& m) {
    std::string b;
    b.push_back(static_cast<char>(1)); // 1 = 回复
    raft_put_le64(b, m.term);
    raft_put_le32(b, m.granted ? 1 : 0);
    return b;
}

inline std::string encode_append_req(const AppendReq& m) {
    std::string b;
    b.push_back(static_cast<char>(0));
    raft_put_le64(b, m.term);
    raft_put_le32(b, m.leader_id);
    raft_put_le64(b, m.prev_log_index);
    raft_put_le64(b, m.prev_log_term);
    raft_put_le64(b, m.leader_commit);
    raft_put_le32(b, static_cast<uint32_t>(m.entries.size()));
    for (const auto& e : m.entries) {
        raft_put_le64(b, e.term);
        b.push_back(static_cast<char>(e.op));
        raft_put_str(b, e.key);
        raft_put_str(b, e.value);
    }
    return b;
}

inline std::string encode_append_reply(const AppendReply& m) {
    std::string b;
    b.push_back(static_cast<char>(1));
    raft_put_le64(b, m.term);
    raft_put_le32(b, m.success ? 1 : 0);
    raft_put_le64(b, m.match_index);
    return b;
}

inline bool decode_append_msg(const std::string& body, bool* is_reply,
                              AppendReq* req, AppendReply* rep) {
    if (body.size() < 1) return false;
    *is_reply = (body[0] != 0);
    size_t off = 1;
    if (*is_reply) {
        rep->term = raft_get_le64(body.data(), &off);
        rep->success = raft_get_le32(body.data(), &off) != 0;
        rep->match_index = raft_get_le64(body.data(), &off);
    } else {
        req->term = raft_get_le64(body.data(), &off);
        req->leader_id = raft_get_le32(body.data(), &off);
        req->prev_log_index = raft_get_le64(body.data(), &off);
        req->prev_log_term = raft_get_le64(body.data(), &off);
        req->leader_commit = raft_get_le64(body.data(), &off);
        uint32_t n = raft_get_le32(body.data(), &off);
        req->entries.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
            req->entries[i].term = raft_get_le64(body.data(), &off);
            req->entries[i].op = static_cast<uint8_t>(body[off++]);
            req->entries[i].key = raft_get_str(body.data(), body.size(), &off);
            req->entries[i].value = raft_get_str(body.data(), body.size(), &off);
        }
    }
    return true;
}
