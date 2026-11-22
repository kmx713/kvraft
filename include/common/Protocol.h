#pragma once
// ============================================================================
// Protocol.h - 自定义二进制应用层协议（客户端命令 + P3 Raft RPC 共用）
// ============================================================================
//
// 【帧格式】一条完整消息称为"帧"（frame），固定头 + 变长 body：
//
//   +--------+------+----------+-------+--------------+
//   | magic  | type | body_len | crc16 | body         |
//   | 2B     | 1B   | 4B       | 2B    | body_len B   |
//   +--------+------+----------+-------+--------------+
//
//   magic    : 0x4B56（'K''V'），识别字节流中的帧边界，非法值直接断连
//              -- 拿错协议的客户端连上来能第一时间发现
//   type     : 消息类型（见 MsgType）
//   body_len : body 长度（小端 32 位，与 WAL 一致用 LE）
//   crc16    : body 的 CRC16-CCITT 校验，传输层已保证可靠，这里防
//              "实现 bug 写坏帧"，解析错位时能快速发现
//
// 【为什么不直接用纯文本协议（如 Redis RESP）？】
//   文本协议调试友好，但解析慢、体积大；P3 Raft 节点间要传结构化的
//   日志条目/任期号，二进制帧是必经之路。统一一套帧协议，P2/P3 复用。
//
// 【上层消息内容（body）约定】
//   CLIENT_REQ  : 文本命令，如 "SET name kmx"
//   CLIENT_RESP : 文本响应，如 "OK" / "kmx" / "ERR ..."
//   REDIRECT    : "host:port"，P3 非 Leader 节点让客户端重连 Leader
//   RAFT_VOTE   : P3 RequestVote RPC（P3 用）
//   RAFT_APPEND : P3 AppendEntries RPC（P3 用）
// ============================================================================

#include <stdint.h>

#include <string>

// 消息类型（1 字节）
enum MsgType : uint8_t {
    MSG_CLIENT_REQ  = 1,  // 客户端 -> 服务器：命令请求
    MSG_CLIENT_RESP = 2,  // 服务器 -> 客户端：命令响应
    MSG_REDIRECT    = 3,  // 服务器 -> 客户端：重定向到 Leader（P3 用）
    MSG_RAFT_VOTE   = 4,  // 节点间：RequestVote RPC（P3 用）
    MSG_RAFT_APPEND = 5,  // 节点间：AppendEntries RPC（P3 用）
};

const uint16_t kFrameMagic      = 0x4B56; // 'K''V'
const size_t   kFrameHeaderSize = 9;      // 2+1+4+2

// 计算 body 的 CRC16-CCITT 校验值
uint16_t crc16_ccitt(const void* data, size_t len);

// 编码一帧完整消息（头 + body），返回可直接发送的字节串
std::string encode_frame(uint8_t type, const std::string& body);

// 尝试从字节流头部解码一帧（粘包处理核心函数）
// data/len : 当前累积字节流；type/body : 出参
// 返回：>0 成功，返回本帧总字节数；=0 数据不足；<0 协议错误（应断连）
int try_decode_frame(const char* data, size_t len, uint8_t* type,
                     std::string* body);
