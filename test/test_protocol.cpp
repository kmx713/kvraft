// ============================================================================
// test_protocol.cpp - 帧协议 + Raft 消息序列化单元测试（P3）
// ============================================================================
// 编译进 test_protocol 目标，make 后运行：
//   ./test_protocol
// 全部断言通过输出 "ALL TESTS PASSED"，任何一项失败 assert 直接终止。
// ============================================================================

#include <assert.h>
#include <string.h>

#include <string>
#include <vector>

#include "common/Protocol.h"
#include "raft/RaftMessages.h"

// ---------------------------------------------------------------- 帧协议 ----
static void test_frame_roundtrip() {
    // 空帧
    std::string f0 = encode_frame(MSG_CLIENT_REQ, "");
    assert(f0.size() == kFrameHeaderSize);
    uint8_t t = 0;
    std::string b;
    assert(try_decode_frame(f0.data(), f0.size(), &t, &b) == kFrameHeaderSize);
    assert(t == MSG_CLIENT_REQ && b.empty());

    // 正常帧（body 允许 \0 和中文 UTF-8，二进制安全）
    std::string body = "SET key value\xc3\xa9";
    std::string f1 = encode_frame(MSG_CLIENT_RESP, body);
    assert(try_decode_frame(f1.data(), f1.size(), &t, &b) ==
           static_cast<int>(f1.size()));
    assert(t == MSG_CLIENT_RESP && b == body);
}
static void test_frame_sticky() {
    std::string a = encode_frame(MSG_CLIENT_REQ, "SET a 1");
    std::string c = encode_frame(MSG_CLIENT_RESP, "OK");
    std::string stream = a + c; // 模拟一次 recv 收到两帧

    uint8_t t = 0;
    std::string b;
    int r = try_decode_frame(stream.data(), stream.size(), &t, &b);
    assert(r == static_cast<int>(a.size()) && t == MSG_CLIENT_REQ && b == "SET a 1");

    r = try_decode_frame(stream.data() + r, stream.size() - r, &t, &b);
    assert(r == static_cast<int>(c.size()) && t == MSG_CLIENT_RESP && b == "OK");
}

static void test_frame_partial_and_bad() {
    std::string f = encode_frame(MSG_CLIENT_REQ, "hello world");
    uint8_t t = 0;
    std::string b;

    // 拆包：一个字节一个字节喂，只有凑满才解出（返回 0）
    std::string acc;
    for (size_t i = 0; i < f.size(); ++i) {
        acc.push_back(f[i]);
        int r = try_decode_frame(acc.data(), acc.size(), &t, &b);
        if (i + 1 < f.size()) {
            assert(r == 0); // 还没凑满一帧
        } else {
            assert(r == static_cast<int>(f.size()) && b == "hello world");
        }
    }

    // 非法 magic：直接判协议错误
    std::string bad = f;
    bad[0] = 0x00;
    assert(try_decode_frame(bad.data(), bad.size(), &t, &b) < 0);

    // CRC 被破坏：判协议错误
    std::string bad2 = f;
    bad2[kFrameHeaderSize - 1] ^= 0xFF;
    assert(try_decode_frame(bad2.data(), bad2.size(), &t, &b) < 0);
}

// ------------------------------------------------------------ Raft 消息 ----
static void test_vote_msg() {
    VoteReq req;
    req.term = 42;
    req.candidate_id = 1;
    req.last_log_index = 7;
    req.last_log_term = 5;
    std::string body = encode_vote_req(req);

    bool is_reply = true;
    VoteReq r1;
    VoteReply r2;
    assert(decode_vote_msg(body, &is_reply, &r1, &r2));
    assert(!is_reply);
    assert(r1.term == 42 && r1.candidate_id == 1 &&
           r1.last_log_index == 7 && r1.last_log_term == 5);

    VoteReply rep;
    rep.term = 43;
    rep.granted = true;
    std::string body2 = encode_vote_reply(rep);
    assert(decode_vote_msg(body2, &is_reply, &r1, &r2));
    assert(is_reply && r2.term == 43 && r2.granted);
}

static void test_append_msg() {
    AppendReq req;
    req.term = 9;
    req.leader_id = 2;
    req.prev_log_index = 3;
    req.prev_log_term = 2;
    req.leader_commit = 2;

    LogEntry e1;
    e1.term = 3;
    e1.op = RAFT_OP_SET;
    e1.key = "name";
    e1.value = "匡明星"; // UTF-8 中文 + 二进制安全
    LogEntry e2;
    e2.term = 3;
    e2.op = RAFT_OP_DEL;
    e2.key = "tmp";
    req.entries = {e1, e2};

    std::string body = encode_append_req(req);
    bool is_reply = true;
    AppendReq r1;
    AppendReply r2;
    assert(decode_append_msg(body, &is_reply, &r1, &r2));
    assert(!is_reply);
    assert(r1.term == 9 && r1.leader_id == 2 && r1.prev_log_index == 3 &&
           r1.prev_log_term == 2 && r1.leader_commit == 2);
    assert(r1.entries.size() == 2);
    assert(r1.entries[0].term == 3 && r1.entries[0].op == RAFT_OP_SET &&
           r1.entries[0].key == "name" && r1.entries[0].value == "匡明星");
    assert(r1.entries[1].op == RAFT_OP_DEL && r1.entries[1].key == "tmp");

    AppendReply rep;
    rep.term = 10;
    rep.success = true;
    rep.match_index = 5;
    std::string body2 = encode_append_reply(rep);
    assert(decode_append_msg(body2, &is_reply, &r1, &r2));
    assert(is_reply && r2.term == 10 && r2.success && r2.match_index == 5);
}

int main() {
    test_frame_roundtrip();
    test_frame_sticky();
    test_frame_partial_and_bad();
    test_vote_msg();
    test_append_msg();
    printf("ALL TESTS PASSED\n");
    return 0;
}
