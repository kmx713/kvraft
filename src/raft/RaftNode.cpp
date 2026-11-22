// ============================================================================
// RaftNode.cpp - Raft 一致性算法实现（P3 核心）
// ============================================================================
//
// 【实现范围】（对应 Raft 论文第 5 节）
//   1. 领导者选举：随机选举超时（800~1500ms）+ 任期号 + 多数派投票
//   2. 日志复制：AppendEntries 心跳 + 一致性检查 + next_index 回退修复
//   3. 安全性：选举限制（日志新旧检查）、只提交本任期条目、状态机按序应用
//
// 【线程模型】
//   全部状态在 EventLoop 单线程内读写（定时器回调 / 网络回调都在循环线程
//   串行执行），无锁。唯一例外是 running_ 停止标志（atomic）。
//
// 【持久化】（raft.wal，二进制记录流，全部小端）
//   'M' META  : term(8) voted_for(4) commit_index(8)
//   'E' ENTRY : term(8) op(1) klen(4) vlen(4) key value
//   追加写 + fsync。重放时"最后一条 META"生效；尾部残缺记录直接截断
//   （与 P1 KVStore WAL 同一套崩溃安全策略）。
//   注意：Raft 要求 current_term/voted_for 在回答 RPC 前必须落盘，
//   本实现 persist_meta() 在投票/投票前同步 fsync，满足该约束。
// ============================================================================

#include "raft/RaftNode.h"

#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <chrono>

#include "common/Log.h"
#include "common/Protocol.h"
#include "raft/RaftMessages.h"

// ---- 持久化记录标签 ----
static const uint8_t kRecMeta  = 'M';
static const uint8_t kRecEntry = 'E';

// ---- 可调参数 ----
static const uint64_t kHeartbeatMs     = 150;  // Leader 心跳间隔
static const uint64_t kElectMinMs      = 800;  // 选举超时下限
static const uint64_t kElectRangeMs    = 700;  // 随机范围 [800, 1500)

// 从 "ip:port" 里解析端口（TcpServer 是不可拷贝对象，必须在初始化列表
// 里一次性构造好，不能先默认构造再赋值）
static uint16_t parse_port(const std::string& addr) {
    size_t pos = addr.rfind(':');
    return static_cast<uint16_t>(std::stoi(addr.substr(pos + 1)));
}

// ============================================================================
// 构造：解析自身地址、创建服务器与到各 peer 的客户端
// ============================================================================
RaftNode::RaftNode(int my_id, const std::vector<std::string>& peer_addrs,
                   const std::string& data_dir)
    : my_id_(my_id),
      addrs_(peer_addrs),
      data_dir_(data_dir),
      role_(FOLLOWER),
      current_term_(0),
      voted_for_(-1),
      commit_index_(0),
      last_applied_(0),
      leader_id_(-1),
      votes_granted_(0),
      election_timer_id_(0),
      heartbeat_timer_id_(0),
      loop_(),
      server_(&loop_, "0.0.0.0", parse_port(peer_addrs[my_id])),
      rwal_(nullptr) {
    // 日志哨兵：log_[i] 表示第 i 条日志（下标从 1 开始，0 号是占位符）
    log_.emplace_back(); // term=0, op=NOOP, key/value 空

    // next_index/match_index 按 peer 数量初始化（become_leader 时再精确重置）
    next_index_.assign(addrs_.size(), 1);
    match_index_.assign(addrs_.size(), 0);

    // 随机数：时间 + 节点 id 做种子，保证三个节点不会以相同节奏竞选
    rng_.seed(static_cast<uint32_t>(time(nullptr)) * 7919u +
              static_cast<uint32_t>(my_id_) + 1u);

    rwal_path_ = data_dir_ + "/raft.wal";
}

RaftNode::~RaftNode() {
    loop_.stop();
    if (rwal_) {
        ::fclose(rwal_);
        rwal_ = nullptr;
    }
}

// ============================================================================
// start - 初始化并进入事件循环（阻塞）
//   顺序很重要：先重放日志恢复状态，再开网络（否则会漏处理新请求）
// ============================================================================
bool RaftNode::start() {
    // 数据目录不存在则创建（0755：owner 可写，其他只读）
    ::mkdir(data_dir_.c_str(), 0755);

    // 1) 崩溃恢复：重放 raft.wal（term/voted_for/commit + 全部日志）
    if (!load_persisted_state()) {
        LOG_ERROR("node %d: load persisted state failed", my_id_);
        return false;
    }

    // 2) 打开 raft.wal 追加句柄（"ab" 保证写指针永远在文件末尾）
    rwal_ = ::fopen(rwal_path_.c_str(), "ab");
    if (!rwal_) {
        LOG_ERROR("node %d: open %s failed: %s", my_id_, rwal_path_.c_str(),
                  strerror(errno));
        return false;
    }

    // 3) 注册消息分发：TcpServer 收到的帧（客户端命令 + 其他节点的 RPC 请求）
    server_.set_callbacks(
        [this](const TcpConn::Ptr& c, uint8_t type, const std::string& body) {
            on_frame(c, type, body);
        },
        nullptr);
    if (!server_.start()) return false;

    // 4) 到每个 peer 建立主动连接；回复帧从这条连接回来，所以在这里
    //    按 peer 编号分发 RPC 回复（回复消息本身不带发送者 id）
    peers_.reserve(addrs_.size());
    for (size_t i = 0; i < addrs_.size(); ++i) {
        if (static_cast<int>(i) == my_id_) {
            peers_.emplace_back(nullptr); // 自己不建连接，占位保持下标对齐
            continue;
        }
        auto cli = std::make_unique<TcpClient>(&loop_, addrs_[i]);
        cli->set_callbacks(
            nullptr,
            [this, i](const TcpConn::Ptr& c, uint8_t type,
                      const std::string& body) {
                (void)c;
                if (type == MSG_RAFT_VOTE) {
                    bool is_reply = false;
                    VoteReq req;
                    VoteReply rep;
                    if (decode_vote_msg(body, &is_reply, &req, &rep) && is_reply) {
                        handle_vote_reply(rep);
                    }
                } else if (type == MSG_RAFT_APPEND) {
                    bool is_reply = false;
                    AppendReq req;
                    AppendReply rep;
                    if (decode_append_msg(body, &is_reply, &req, &rep) && is_reply) {
                        handle_append_reply(static_cast<uint32_t>(i), rep);
                    }
                }
            },
            nullptr);
        cli->connect();
        peers_.emplace_back(std::move(cli));
    }

    // 5) 启动选举定时器（第一次超时后开始竞选）
    reset_election_timer();

    LOG_INFO("node %d started: addr=%s peers=%zu data=%s", my_id_,
             addrs_[my_id_].c_str(), addrs_.size(), data_dir_.c_str());

    // 6) 进入事件循环（阻塞直到 stop）
    loop_.run();
    return true;
}

// ============================================================================
// 持久化：raft.wal 重放 / META 追加 / ENTRY 追加 / 运维状态文件
// ============================================================================

// 追加一条记录并 fsync 落盘。返回 false = 磁盘故障（致命，直接退出循环）
static bool wal_append(FILE* f, const std::string& rec) {
    if (::fwrite(rec.data(), 1, rec.size(), f) != rec.size()) return false;
    ::fflush(f);
    if (::fsync(::fileno(f)) != 0) return false; // 崩溃安全的生命线
    return true;
}

bool RaftNode::load_persisted_state() {
    FILE* f = ::fopen(rwal_path_.c_str(), "rb");
    if (!f) return true; // 首次启动，文件还不存在，从零开始

    // 重放：逐条读记录。META 用局部变量收着，读完统一赋值
    //（commit_index 可能出现在日志条目之前，不能边读边用）
    uint64_t meta_term = 0, meta_commit = 0;
    int meta_voted = -1;
    bool corrupt_tail = false;

    while (true) {
        uint8_t tag = 0;
        if (::fread(&tag, 1, 1, f) != 1) break; // 正常读完（EOF）

        if (tag == kRecMeta) {
            size_t off = 0;
            char buf[20];
            if (::fread(buf, 1, 20, f) != 20) { corrupt_tail = true; break; }
            meta_term    = raft_get_le64(buf, &off);
            uint32_t v   = raft_get_le32(buf, &off);
            meta_voted   = static_cast<int>(v) - 1; // 存盘时 +1 避免 -1 歧义
            meta_commit  = raft_get_le64(buf, &off);
        } else if (tag == kRecEntry) {
            size_t off = 0;
            char head[13];
            if (::fread(head, 1, 13, f) != 13) { corrupt_tail = true; break; }
            LogEntry e;
            e.term = raft_get_le64(head, &off);
            e.op   = static_cast<uint8_t>(head[off++]);
            uint32_t klen = raft_get_le32(head, &off);
            uint32_t vlen = raft_get_le32(head, &off);
            e.key.resize(klen);
            e.value.resize(vlen);
            if (klen && ::fread(&e.key[0], 1, klen, f) != klen) { corrupt_tail = true; break; }
            if (vlen && ::fread(&e.value[0], 1, vlen, f) != vlen) { corrupt_tail = true; break; }
            log_.push_back(std::move(e));
        } else {
            corrupt_tail = true; // 未知标签：写坏的记录，停止重放
            break;
        }
    }

    long good = ::ftell(f); // 最后一个完整记录的末尾
    ::fclose(f);

    // 尾部残缺：截掉（崩溃时写到一半的记录，Raft 语义下等于从未发生）
    if (corrupt_tail) {
        LOG_WARN("node %d: truncated corrupt tail of %s at %ld", my_id_,
                 rwal_path_.c_str(), good);
        ::truncate(rwal_path_.c_str(), good);
    }

    // 应用恢复出的状态
    current_term_ = meta_term;
    voted_for_    = meta_voted;
    commit_index_ = meta_commit < last_log_index() ? meta_commit : last_log_index();
    last_applied_ = 0;
    apply_committed(); // 把 [1, commit] 的日志应用到状态机（恢复内存数据）

    LOG_INFO("node %d: recovered term=%lu voted_for=%d log=%zu commit=%lu",
             my_id_, (unsigned long)current_term_, voted_for_, log_.size() - 1,
             (unsigned long)commit_index_);
    return true;
}

bool RaftNode::persist_meta() {
    // voted_for 存盘时 +1：-1（未投票）转成 0，避免负数序列化歧义
    std::string b;
    b.push_back(static_cast<char>(kRecMeta));
    raft_put_le64(b, current_term_);
    raft_put_le32(b, static_cast<uint32_t>(voted_for_ + 1));
    raft_put_le64(b, commit_index_);
    if (!wal_append(rwal_, b)) {
        LOG_ERROR("node %d: persist meta failed: %s", my_id_, strerror(errno));
        return false;
    }
    return true;
}

bool RaftNode::persist_entry(const LogEntry& e) {
    std::string b;
    b.push_back(static_cast<char>(kRecEntry));
    raft_put_le64(b, e.term);
    b.push_back(static_cast<char>(e.op));
    raft_put_str(b, e.key);
    raft_put_str(b, e.value);
    return wal_append(rwal_, b);
}

// data/role + data/pid：运维/演示脚本用（比如故障演练时找 Leader）
void RaftNode::write_status_files() {
    static const char* names[] = {"FOLLOWER", "CANDIDATE", "LEADER"};
    FILE* f = ::fopen((data_dir_ + "/role").c_str(), "w");
    if (f) {
        ::fprintf(f, "%s term=%lu\n", names[role_], (unsigned long)current_term_);
        ::fclose(f);
    }
    f = ::fopen((data_dir_ + "/pid").c_str(), "w");
    if (f) {
        ::fprintf(f, "%d\n", static_cast<int>(::getpid()));
        ::fclose(f);
    }
}

// ============================================================================
// 定时器与角色转换
// ============================================================================

// 重设随机选举超时：FOLLOWER 收不到心跳 / CANDIDATE 竞选失败都会走到这里。
// 随机化是为了避免"三个节点同时超时、互相拉票、永远选不出"（活锁）
void RaftNode::reset_election_timer() {
    if (election_timer_id_) loop_.cancel_timer(election_timer_id_);
    std::uniform_int_distribution<uint64_t> dist(kElectMinMs,
                                                 kElectMinMs + kElectRangeMs);
    election_timer_id_ = loop_.run_after(dist(rng_), [this] { start_election(); });
}

// 选举超时 -> 发起竞选
void RaftNode::start_election() {
    election_timer_id_ = 0;
    role_ = CANDIDATE;
    ++current_term_;        // 竞选一次，任期 +1
    voted_for_ = my_id_;    // 先投自己
    votes_granted_ = 1;
    persist_meta();         // Raft 约束：投票前必须持久化
    write_status_files();

    LOG_INFO("node %d: starting election for term %lu", my_id_,
             (unsigned long)current_term_);

    // 向所有 peer 广播拉票
    VoteReq req;
    req.term = current_term_;
    req.candidate_id = static_cast<uint32_t>(my_id_);
    req.last_log_index = last_log_index();
    req.last_log_term = last_log_term();
    std::string body = encode_vote_req(req);
    for (auto& p : peers_) {
        if (p) p->send(MSG_RAFT_VOTE, body);
    }

    // 竞选也可能失败（网络分区/别人先赢），继续安排下一次超时
    reset_election_timer();
}

void RaftNode::become_follower(uint64_t term, int new_leader) {
    bool term_changed = (term > current_term_);
    if (term_changed) {
        current_term_ = term;
        voted_for_ = -1;
        persist_meta();
    }
    role_ = FOLLOWER;
    leader_id_ = new_leader;

    // 之前是 Leader：作废心跳定时器，通知挂起的客户端"换人了"
    if (heartbeat_timer_id_) {
        loop_.cancel_timer(heartbeat_timer_id_);
        heartbeat_timer_id_ = 0;
    }
    fail_all_pending();
    reset_election_timer();
    write_status_files();
    if (term_changed) {
        LOG_INFO("node %d: step down to FOLLOWER, term=%lu", my_id_,
                 (unsigned long)current_term_);
    }
}

void RaftNode::become_leader() {
    role_ = LEADER;
    leader_id_ = my_id_;
    votes_granted_ = 0;
    if (election_timer_id_) { // Leader 不需要选举超时
        loop_.cancel_timer(election_timer_id_);
        election_timer_id_ = 0;
    }

    // 初始化复制进度：从"假设 follower 与我一致"开始（next = 我的最后一条+1）
    // 不一致会在一致性检查失败后回退 next_index 逐条修复
    for (size_t i = 0; i < addrs_.size(); ++i) {
        next_index_[i] = last_log_index() + 1;
        match_index_[i] = 0;
    }

    // 追加一条 NOOP（空操作）：
    // 目的 - 上任后立刻提交一条"本任期"的日志。因为 Raft 规定 Leader
    // 只能提交"自己任期"的条目，NOOP 提交后，之前任期积压的条目
    // 会间接跟着一起提交（commit 前移把它们全部覆盖）
    LogEntry noop;
    noop.term = current_term_;
    noop.op = RAFT_OP_NOOP;
    log_.push_back(noop);
    persist_entry(noop);

    // 启动心跳（AppendEntries 复用：entries 为空的心跳 + 带日志的复制）
    heartbeat_timer_id_ = loop_.run_every(kHeartbeatMs, [this] { broadcast_append(); });

    write_status_files();
    LOG_INFO("node %d: became LEADER of term %lu", my_id_,
             (unsigned long)current_term_);
    broadcast_append(); // 上任立刻广播一轮，不用等第一个心跳周期
}

// 失去 Leader 身份（发现更高任期）时，回复所有挂起的写请求
void RaftNode::fail_all_pending() {
    for (auto& kv : pending_) {
        send_resp(kv.second, "ERR leader changed, retry");
    }
    pending_.clear();
}

// ============================================================================
// RPC 分发与处理
// ============================================================================

// TcpServer 侧消息入口：客户端命令 + 其他节点发来的 RPC 请求
//（RPC 回复走 TcpClient 通道，在 start() 里的 lambda 分发，不经过这里）
void RaftNode::on_frame(const TcpConn::Ptr& conn, uint8_t type,
                        const std::string& body) {
    switch (type) {
        case MSG_CLIENT_REQ:
            handle_client(conn, body);
            break;
        case MSG_RAFT_VOTE: {
            bool is_reply = false;
            VoteReq req;
            VoteReply rep;
            if (decode_vote_msg(body, &is_reply, &req, &rep) && !is_reply) {
                handle_vote_req(req, conn);
            }
            break;
        }
        case MSG_RAFT_APPEND: {
            bool is_reply = false;
            AppendReq req;
            AppendReply rep;
            if (decode_append_msg(body, &is_reply, &req, &rep) && !is_reply) {
                handle_append_req(req, conn);
            }
            break;
        }
        default:
            LOG_WARN("node %d: unknown frame type %u", my_id_, type);
    }
}

// ----------------------------------------------------------------------------
// RequestVote 请求处理（Follower 侧投票决策）
// 两条硬性规则：
//   1. 任期小的请求直接拒绝（旧 Leader/旧候选人还活着）
//   2. 日志新旧检查：候选人日志的"最后条目"必须比我新（先比任期，任期相同
//      再比索引），防止"日志落后的节点当选，把已提交的数据覆盖掉"
// ----------------------------------------------------------------------------
void RaftNode::handle_vote_req(const VoteReq& req, const TcpConn::Ptr& conn) {
    bool grant = false;

    if (req.term < current_term_) {
        // 旧任期：拒绝（顺便告知我的新任期，让它立刻下台）
    } else {
        if (req.term > current_term_) {
            // 看到更高任期：无条件变 Follower（哪怕自己之前是 Leader）
            become_follower(req.term);
        }
        bool not_voted = (voted_for_ == -1 || voted_for_ == static_cast<int>(req.candidate_id));
        bool log_ok = (req.last_log_term > last_log_term()) ||
                      (req.last_log_term == last_log_term() &&
                       req.last_log_index >= last_log_index());
        if (not_voted && log_ok) {
            grant = true;
            voted_for_ = static_cast<int>(req.candidate_id);
            persist_meta(); // 投票必须先落盘，才能回复（崩溃后不能改票）
            reset_election_timer(); // 投了票 = 认可它的领导权，推迟自己竞选
        }
    }

    VoteReply rep;
    rep.term = current_term_;
    rep.granted = grant;
    conn_send(&loop_, conn, encode_frame(MSG_RAFT_VOTE, encode_vote_reply(rep)));
}

// ----------------------------------------------------------------------------
// RequestVote 回复处理（Candidate 侧计票）
// ----------------------------------------------------------------------------
void RaftNode::handle_vote_reply(const VoteReply& rep) {
    if (rep.term > current_term_) {
        // 有人任期比我新：我落选，立刻让位
        become_follower(rep.term);
        return;
    }
    if (role_ != CANDIDATE || rep.term != current_term_ || !rep.granted) return;

    ++votes_granted_;
    LOG_DEBUG("node %d: vote granted, %d/%d", my_id_, votes_granted_, majority());
    if (votes_granted_ >= majority()) {
        become_leader(); // 多数派到手，直接上位（多余的票不用等）
    }
}

// ----------------------------------------------------------------------------
// AppendEntries 请求处理（Follower 侧）
//   1. 任期检查（过期 Leader 直接拒绝）
//   2. 一致性检查：prev_log_index 处的日志任期必须与 Leader 说的相同
//      —— 这是 Raft 日志匹配特性的核心，保证两边日志前缀完全一致
//   3. 追加新条目（本地冲突时截断重写）
//   4. 推进 commit 并应用到状态机
// ----------------------------------------------------------------------------
void RaftNode::handle_append_req(const AppendReq& req, const TcpConn::Ptr& conn) {
    if (req.term < current_term_) {
        // 过期 Leader 的心跳：拒绝并告知我的任期，让它下台
        AppendReply rep;
        rep.term = current_term_;
        rep.success = false;
        conn_send(&loop_, conn, encode_frame(MSG_RAFT_APPEND, encode_append_reply(rep)));
        return;
    }
    if (req.term > current_term_) {
        become_follower(req.term);
    } else {
        reset_election_timer(); // 合法 Leader 在敲门：推迟自己的竞选
    }
    leader_id_ = static_cast<int>(req.leader_id);

    // ---- 一致性检查 ----
    bool match = (req.prev_log_index <= last_log_index()) &&
                 (req.prev_log_index == 0 ||
                  log_[req.prev_log_index].term == req.prev_log_term);

    AppendReply rep;
    rep.term = current_term_;
    if (!match) {
        // 前缀不一致：拒绝。Leader 收到后会回退 next_index 重发更早的日志，
        // 直到找到双方一致的锚点，再把之后的差异部分整段重传（日志修复）
        rep.success = false;
        rep.match_index = 0;
        conn_send(&loop_, conn, encode_frame(MSG_RAFT_APPEND, encode_append_reply(rep)));
        return;
    }

    // ---- 追加条目（幂等：重复收到同一条按"已存在且相同"跳过）----
    for (size_t i = 0; i < req.entries.size(); ++i) {
        uint64_t idx = req.prev_log_index + 1 + i;
        const LogEntry& e = req.entries[i];
        if (idx <= last_log_index()) {
            if (log_[idx].term == e.term) continue; // 已有且相同，跳过
            // 同一位置任期不同：本地日志是"废案"，从这截断，用 Leader 的覆盖
            LOG_WARN("node %d: conflict at idx %lu, truncating %zu entries",
                     my_id_, (unsigned long)idx, log_.size() - idx);
            log_.resize(idx);
        }
        log_.push_back(e);
        persist_entry(e);
    }

    // ---- 推进提交点（不能超过自己日志长度）----
    if (req.leader_commit > commit_index_) {
        commit_index_ = req.leader_commit < last_log_index()
                            ? req.leader_commit
                            : last_log_index();
        persist_meta();
        apply_committed();
    }

    rep.success = true;
    rep.match_index = req.prev_log_index + req.entries.size();
    conn_send(&loop_, conn, encode_frame(MSG_RAFT_APPEND, encode_append_reply(rep)));
}

// ----------------------------------------------------------------------------
// AppendEntries 回复处理（Leader 侧复制进度更新）
// ----------------------------------------------------------------------------
void RaftNode::handle_append_reply(uint32_t peer_id, const AppendReply& rep) {
    if (rep.term > current_term_) {
        become_follower(rep.term); // 发现新 Leader，让位
        return;
    }
    if (role_ != LEADER || rep.term != current_term_) return;

    if (rep.success) {
        // 该 follower 已复制到 match_index：更新进度
        if (rep.match_index > match_index_[peer_id]) {
            match_index_[peer_id] = rep.match_index;
            next_index_[peer_id] = rep.match_index + 1;
        }
        advance_commit(); // 可能凑齐多数派了，尝试推进提交
        // 如果还有没发完的日志，立刻补发（不等下个心跳周期，降低延迟）
        if (next_index_[peer_id] <= last_log_index()) {
            replicate_to(peer_id);
        }
    } else {
        // 一致性检查失败：回退一个索引重试。
        // （优化方向：回复里带冲突任期/索引，一次跳过整段，此处用保守版）
        if (next_index_[peer_id] > 1) --next_index_[peer_id];
        replicate_to(peer_id);
    }
}

// ============================================================================
// 日志复制与状态机应用
// ============================================================================

// 从 next_index_[peer] 起把日志发给该 follower（心跳/复制共用一条路径）
void RaftNode::replicate_to(uint32_t peer_id) {
    if (role_ != LEADER) return;
    if (!peers_[peer_id] || !peers_[peer_id]->is_connected()) return; // 断线，心跳重试

    AppendReq req;
    req.term = current_term_;
    req.leader_id = static_cast<uint32_t>(my_id_);
    req.leader_commit = commit_index_;
    req.prev_log_index = next_index_[peer_id] - 1; // 一致性检查锚点
    req.prev_log_term = log_[req.prev_log_index].term;
    for (uint64_t i = next_index_[peer_id]; i <= last_log_index(); ++i) {
        req.entries.push_back(log_[i]);
    }
    peers_[peer_id]->send(MSG_RAFT_APPEND, encode_append_req(req));
}

// 广播给所有 follower（心跳定时器每 150ms 调一次；新写请求也调）
void RaftNode::broadcast_append() {
    if (role_ != LEADER) return;
    for (size_t i = 0; i < peers_.size(); ++i) {
        if (peers_[i]) replicate_to(static_cast<uint32_t>(i));
    }
}

// 推进 commit：统计每个日志索引被多少节点确认（自己算一票），
// 凑够多数派即可提交。
// 安全性约束：只能直接提交"本任期"的条目（论文 5.4.2）——旧任期条目
// 只能随新任期条目一起被间接提交，否则已覆盖的废案可能复活
void RaftNode::advance_commit() {
    for (uint64_t n = last_log_index(); n > commit_index_; --n) {
        if (log_[n].term != current_term_) continue;

        int count = 1; // Leader 自己
        for (size_t i = 0; i < match_index_.size(); ++i) {
            if (static_cast<int>(i) == my_id_) continue;
            if (match_index_[i] >= n) ++count;
        }
        if (count >= majority()) {
            commit_index_ = n;
            persist_meta(); // commit 也要落盘：重启后才能跳过重投票
            apply_committed();
            break; // 从高往低找，第一个凑够多数派的就是新 commit
        }
    }
}

// 把 [last_applied+1, commit] 的日志按序应用到状态机，并回复等待中的客户端
void RaftNode::apply_committed() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        const LogEntry& e = log_[last_applied_];
        switch (e.op) {
            case RAFT_OP_SET:
                sm_.insert_or_update(e.key, e.value);
                break;
            case RAFT_OP_DEL:
                sm_.erase(e.key);
                break;
            case RAFT_OP_NOOP:
                break; // 上任占位条目，不动状态机
        }
        // 该索引挂着一个等提交结果的客户端？回复 OK
        auto it = pending_.find(last_applied_);
        if (it != pending_.end()) {
            send_resp(it->second, "OK");
            pending_.erase(it);
        }
    }
}

// ============================================================================
// 客户端命令（文本协议：SET k v / GET k / DEL k）
// ============================================================================
//
// 【写请求路径】（强一致）
//   客户端 -> Leader -> 追加日志+fsync -> 广播 -> 多数派确认 -> commit
//   -> 应用到状态机 -> 回复 OK
//   挂起连接存在 pending_[日志索引] 里，提交成功/失败时统一收尾
//
// 【读请求路径】
//   GET 直接读本节点已应用的状态机。注意：Follower 的状态机可能落后
//   Leader 几毫秒（心跳间隔内），所以读是"最终一致"的；严格线性一致读
//   需要 ReadIndex/Lease 协议，作为演进方向写在 README
// ----------------------------------------------------------------------------
void RaftNode::handle_client(const TcpConn::Ptr& conn, const std::string& body) {
    // 按空白切分：第一个词是命令
    std::vector<std::string> tok;
    size_t i = 0;
    while (i < body.size()) {
        while (i < body.size() && (body[i] == ' ' || body[i] == '\t' ||
                                   body[i] == '\r' || body[i] == '\n')) ++i;
        size_t j = i;
        while (j < body.size() && body[j] != ' ' && body[j] != '\t' &&
               body[j] != '\r' && body[j] != '\n') ++j;
        if (j > i) tok.push_back(body.substr(i, j - i));
        i = j;
    }
    if (tok.empty()) { send_resp(conn, "ERR empty"); return; }

    const std::string& cmd = tok[0];

    // ---- 非 Leader：告诉客户端 Leader 在哪（客户端重连即可）----
    if (!is_leader()) {
        if (leader_id_ >= 0) {
            send_redirect(conn); // body 里带 Leader 地址
        } else {
            send_resp(conn, "ERR no leader now, retry"); // 集群正在选主
        }
        return;
    }

    if (cmd == "SET" && tok.size() >= 3) {
        // value = 第 3 个词之后的全部原文（value 允许带空格）
        std::string value = body;
        // 找到 tok[2] 起始位置之后的剩余内容
        size_t pos = 0;
        for (int t = 0; t < 3; ++t) {
            while (pos < value.size() && isspace(static_cast<unsigned char>(value[pos]))) ++pos;
            while (pos < value.size() && !isspace(static_cast<unsigned char>(value[pos]))) ++pos;
        }
        while (pos < value.size() && isspace(static_cast<unsigned char>(value[pos]))) ++pos;
        value = value.substr(pos);

        LogEntry e;
        e.term = current_term_;
        e.op = RAFT_OP_SET;
        e.key = tok[1];
        e.value = value;
        log_.push_back(e);
        if (!persist_entry(e)) { // fsync 失败：磁盘故障，这条不能算数
            log_.pop_back();
            send_resp(conn, "ERR persist failed");
            return;
        }
        pending_[last_log_index()] = conn; // 提交成功后由 apply_committed 回复
        broadcast_append();                // 立刻复制，不等心跳周期
    } else if (cmd == "DEL" && tok.size() >= 2) {
        LogEntry e;
        e.term = current_term_;
        e.op = RAFT_OP_DEL;
        e.key = tok[1];
        log_.push_back(e);
        if (!persist_entry(e)) {
            log_.pop_back();
            send_resp(conn, "ERR persist failed");
            return;
        }
        pending_[last_log_index()] = conn;
        broadcast_append();
    } else if (cmd == "GET" && tok.size() >= 2) {
        std::string* v = sm_.find(tok[1]);
        send_resp(conn, v ? *v : "NOT_FOUND");
    } else if (cmd == "PING") {
        send_resp(conn, "PONG " + addrs_[my_id_]);
    } else if (cmd == "INFO") {
        static const char* names[] = {"FOLLOWER", "CANDIDATE", "LEADER"};
        char buf[256];
        ::snprintf(buf, sizeof(buf),
                   "role=%s term=%lu leader=%s log=%zu commit=%lu applied=%lu",
                   names[role_], (unsigned long)current_term_,
                   leader_id_ >= 0 ? addrs_[leader_id_].c_str() : "none",
                   log_.size() - 1, (unsigned long)commit_index_,
                   (unsigned long)last_applied_);
        send_resp(conn, buf);
    } else {
        send_resp(conn, "ERR bad command (SET/GET/DEL/PING/INFO)");
    }
}

// 发文本响应给客户端连接
void RaftNode::send_resp(const TcpConn::Ptr& conn, const std::string& text) {
    conn_send(&loop_, conn, encode_frame(MSG_CLIENT_RESP, text));
}

// 重定向：body = Leader 的 "ip:port"，客户端收到后直接重连
void RaftNode::send_redirect(const TcpConn::Ptr& conn) {
    if (leader_id_ >= 0) {
        conn_send(&loop_, conn, encode_frame(MSG_REDIRECT, addrs_[leader_id_]));
    }
}
