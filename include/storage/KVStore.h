#pragma once
// ============================================================================
// KVStore.h - 单机 KV 存储引擎（P1 核心模块）
// ============================================================================
//
// 【整体架构】
//   内存层：SkipList（所有数据的全量内存镜像，O(log n) 读写）
//   磁盘层：WAL 预写日志（append-only 追加写，保证持久性）
//
// 【写路径】SET key value
//   1. 组装记录（op + key + value + CRC32 校验）
//   2. fwrite 追加写入 WAL
//   3. fsync 强制刷盘（内核页缓存 -> 磁盘，掉电不丢）     <-- 可配置
//   4. 更新内存 SkipList
//   5. 检查 WAL 大小，超过阈值（64MB）触发自动压缩
//
// 【读路径】GET key
//   纯内存操作：SkipList::find，O(log n)，不碰磁盘（这就是内存引擎快的原因）
//
// 【启动恢复】
//   进程重启时重放（replay）整个 WAL：
//   - 逐条读取记录，校验 CRC32
//   - SET -> 重新插入跳表；DEL -> 从跳表删除
//   - 尾部残缺 / CRC 不通过 -> 说明上次是崩溃（写到一半），直接截断
//     （WAL 只有"完整记录"才生效，天然抗半写）
//
// 【WAL 压缩（compaction）】
//   append-only 的问题：一个 key 改 100 次 WAL 里就有 100 条记录，文件无限膨胀。
//   压缩方案：全量 dump 内存跳表 -> 只写"每 key 最新值"到临时文件 ->
//   fsync -> 原子 rename 替换旧 WAL。rename 在同一文件系统内是原子操作，
//   压缩中途崩溃也不会丢数据（旧 WAL 完好）。
//
// 【面试高频】
//   Q: 为什么必须 fsync？不开会怎样？
//   A: fwrite 只写到 glibc 用户态缓冲 + 内核页缓存，进程崩溃数据还在
//      （内核会刷），但机器断电/内核崩溃就丢了。fsync 强制落盘，
//      代价是每次写多一次磁盘 IO，吞吐下降 —— 可通过 fsync_on_write 开关
//      做吞吐/安全的取舍（类似 MySQL innodb_flush_log_at_trx_commit）。
//
//   Q: WAL 和"直接改数据文件"的区别？
//   A: 顺序追加写 vs 随机写。磁盘（尤其 HDD）顺序写吞吐是随机写的
//      几十上百倍，WAL 把随机写转成顺序写，这也是 LSM-Tree 的核心思想。
// ============================================================================

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

    // ---- 基础 CRUD（全部线程安全）----
    bool set(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string* value); // value 为出参，返回 false 表示不存在
    bool del(const std::string& key);

    // ---- 范围/全量查询（供 SCAN 命令与压缩使用）----
    void scan(const std::string& begin, const std::string& end,
              std::vector<std::pair<std::string, std::string>>& out);
    void dump_all(std::vector<std::pair<std::string, std::string>>& out);
    size_t size();

    // 手动触发 WAL 压缩（内部还会按阈值自动触发）
    void compact();

private:
    // ------------------------------------------------------------------
    // WAL 记录二进制格式（紧凑，节省磁盘 IO 字节）：
    //
    //   +------+------+------+--------+---------+-------+
    //   | op   | klen | vlen | key    | value   | crc32 |
    //   | 1B   | 4B   | 4B   | klen B | vlen B  | 4B    |
    //   +------+------+------+--------+---------+-------+
    //
    //   op     : 0 = SET, 1 = DEL
    //   klen   : key 长度，小端 32 位（小端 = x86 原生序，读写零转换）
    //   vlen   : value 长度，小端 32 位
    //   crc32  : 对前面全部字节（op+klen+vlen+key+value）的 CRC 校验，
    //            用于崩溃后识别"写到一半"的残缺记录
    // ------------------------------------------------------------------
    static const uint8_t OP_SET = 0;
    static const uint8_t OP_DEL = 1;

    // 追加一条记录到 WAL 并（可选）fsync，返回是否成功
    bool append_record(uint8_t op, const std::string& key, const std::string& value);

    // 启动时重放 WAL 恢复内存状态
    void replay();

    // WAL 超过 compact_threshold_ 时触发压缩
    void maybe_compact();

    std::string wal_path_;        // WAL 文件路径（<dir>/data.wal）
    FILE* wal_;                   // WAL 文件句柄（r+b 模式，追加定位）
    bool fsync_;                  // 是否每次写都 fsync（吞吐/安全开关）
    uint64_t wal_size_;           // 当前 WAL 字节数（决定是否压缩）
    uint64_t compact_threshold_;  // 压缩阈值，默认 64MB
    SkipList<std::string, std::string> list_;  // 内存引擎：全量数据镜像
    std::mutex mu_;               // 粗粒度锁：保护 WAL 写与跳表读写的原子性
};
