// ============================================================================
// Protocol.cpp - 帧编解码与 CRC16 实现
// ============================================================================

#include "common/Protocol.h"

#include <string.h>

// CRC16-CCITT（多项式 0x1021，初始值 0xFFFF）
// 用途：帧校验。CRC16 对短 body（命令消息）足够，比 CRC32 少 2 字节；
// 大数据块（P3 日志同步）如需更强校验可升级 CRC32。
uint16_t crc16_ccitt(const void* data, size_t len) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint16_t>(p[i]) << 8; // 字节移到高 8 位参与运算
        for (int j = 0; j < 8; ++j) {
            // 最高位为 1 则左移并异或多项式，否则只左移
            crc = (crc & 0x8000) ? static_cast<uint16_t>((crc << 1) ^ 0x1021)
                                 : static_cast<uint16_t>(crc << 1);
        }
    }
    return crc;
}

// 小端读写辅助（与 WAL 的 put_le32/get_le32 同一套约定）
static void put_le16(std::string& buf, uint16_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
}

static void put_le32(std::string& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
}

static uint16_t get_le16(const char* p) {
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p);
    return static_cast<uint16_t>(q[0]) | static_cast<uint16_t>(q[1] << 8);
}

static uint32_t get_le32(const char* p) {
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p);
    return static_cast<uint32_t>(q[0]) | (static_cast<uint32_t>(q[1]) << 8) |
           (static_cast<uint32_t>(q[2]) << 16) | (static_cast<uint32_t>(q[3]) << 24);
}

// 编码：组装 header（magic/type/len/crc）+ body
std::string encode_frame(uint8_t type, const std::string& body) {
    std::string frame;
    frame.reserve(kFrameHeaderSize + body.size());
    put_le16(frame, kFrameMagic);                            // magic 2B
    frame.push_back(static_cast<char>(type));                // type 1B
    put_le32(frame, static_cast<uint32_t>(body.size()));     // body_len 4B
    put_le16(frame, crc16_ccitt(body.data(), body.size()));  // crc16 2B
    frame.append(body);
    return frame;
}

// 解码：头部逐字段校验，全部通过才算一帧
// 假设 data 指向的字节流从某个帧的 magic 开始：
// 每个连接有独立 Buffer，发送方总是整帧整帧地写，字节流天然帧对齐
int try_decode_frame(const char* data, size_t len, uint8_t* type,
                     std::string* body) {
    // 1) 头部完整性检查
    if (len < kFrameHeaderSize) return 0;

    // 2) magic 校验：错协议/脏数据立刻识别
    uint16_t magic = get_le16(data);
    if (magic != kFrameMagic) return -1;

    // 3) body_len 合法性：上限 32MB，防止脏数据撑爆内存
    uint32_t body_len = get_le32(data + 3);
    if (body_len > (32u << 20)) return -1;

    size_t total = kFrameHeaderSize + body_len;
    // 4) 整帧完整性：不够就返回 0 等 TCP 凑齐（拆包处理）
    if (len < total) return 0;

    // 5) CRC 校验 body
    uint16_t expect = get_le16(data + 7);
    uint16_t actual = crc16_ccitt(data + kFrameHeaderSize, body_len);
    if (expect != actual) return -1;

    // 6) 全部通过 -> 填出参，返回本帧总长度
    if (type) *type = static_cast<uint8_t>(data[2]);
    if (body) body->assign(data + kFrameHeaderSize, body_len);
    return static_cast<int>(total);
}
