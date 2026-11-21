#pragma once
// KVStore.h - 单机 KV 存储引擎
// 写路径：SET -> WAL 追加 + fsync -> SkipList 内存应用 -> 响应
// 启动时重放 WAL 恢复数据；WAL 超过阈值自动压缩（全量重写）

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "storage/SkipList.h"

class KVStore {
public:
    // dir: 数据目录（存放 wal 文件）；fsync_on_write: 每次写是否强制刷盘
    explicit KVStore(const std::string& dir, bool fsync_on_write = true);
    ~KVStore();

    KVStore(const KVStore&) = delete;
    KVStore& operator=(const KVStore&) = delete;

    bool set(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string* value); // value 为出参，返回 false 表示不存在
    bool del(const std::string& key);
    void scan(const std::string& begin, const std::string& end,
              std::vector<std::pair<std::string, std::string>>& out);
    void dump_all(std::vector<std::pair<std::string, std::string>>& out);
    size_t size();
    void compact(); // 手动触发 WAL 压缩

private:
    // WAL 记录格式：op(1B) | klen(4B LE) | vlen(4B LE) | key | value | crc32(4B LE)
    static const uint8_t OP_SET = 0;
    static const uint8_t OP_DEL = 1;

    bool append_record(uint8_t op, const std::string& key, const std::string& value);
    void replay(); // 启动时重放 WAL
    void maybe_compact();

    std::string wal_path_;
    FILE* wal_;
    bool fsync_;
    uint64_t wal_size_;
    uint64_t compact_threshold_; // WAL 超过此大小触发压缩
    SkipList<std::string, std::string> list_;
    std::mutex mu_;
};
