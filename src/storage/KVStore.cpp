// ============================================================================
// KVStore.cpp - WAL 预写日志 + SkipList 内存引擎实现
// ============================================================================

#include "storage/KVStore.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/Log.h"

// ----------------------------------------------------------------------------
// CRC32 实现（生成多项式 0xEDB88320，反射形式，表驱动）
//
// 【为什么 WAL 需要 CRC32？】
//   崩溃场景：fwrite 写到一半进程被 kill -9 / 断电，文件尾部留下半条记录。
//   下次启动重放时，如果只按长度解析，可能把"残缺数据"当合法记录应用，
//   导致脏数据。CRC32 对记录内容计算校验和存在尾部 —— 残缺记录必然校验失败，
//   重放程序据此识别边界并停止/截断。
//
// 【为什么静态局部变量 + inited 标志？】
//   256 项的查表只初始化一次；C++11 起静态局部变量初始化本身线程安全
//   （magic statics），但 inited 是普通 bool，首次初始化竞态无害
//   （多线程重复算表结果一致）。若要绝对严谨可用 std::call_once。
// ----------------------------------------------------------------------------
static uint32_t crc32_calc(const void* data, size_t len) {
    static uint32_t table[256];
    static bool inited = false;
    if (!inited) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                // 多项式除法：最低位为 1 则异或生成多项式，否则右移
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        inited = true;
    }
    // 标准流程：初始值取反 -> 逐字节查表 -> 结果取反
    uint32_t crc = 0xFFFFFFFFu;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

// ----------------------------------------------------------------------------
// 小端序读写辅助
// 【为什么不用 memcpy 到 uint32_t？】
//   虽然目标平台（x86-64）是小端，但逐字节拼装是可移植、无对齐问题的
//   标准写法 —— 不依赖宿主机字节序，也不触发未对齐访问 UB。
// ----------------------------------------------------------------------------
static void put_le32(std::string& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
}

static uint32_t get_le32(const char* p) {
    const uint8_t* q = reinterpret_cast<const uint8_t*>(p);
    return static_cast<uint32_t>(q[0]) | (static_cast<uint32_t>(q[1]) << 8) |
           (static_cast<uint32_t>(q[2]) << 16) | (static_cast<uint32_t>(q[3]) << 24);
}

// ----------------------------------------------------------------------------
// 构造：建目录 -> 重放 WAL 恢复内存 -> 打开 WAL 追加句柄
// ----------------------------------------------------------------------------
KVStore::KVStore(const std::string& dir, bool fsync_on_write)
    : wal_(nullptr), fsync_(fsync_on_write), wal_size_(0),
      compact_threshold_(64ull * 1024 * 1024) {  // 64MB 触发压缩
    // 确保数据目录存在（EEXIST = 目录已存在，不算错误）
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG_ERROR("mkdir %s failed: %s", dir.c_str(), strerror(errno));
    }
    wal_path_ = dir + "/data.wal";

    // 先重放：WAL 里已有的数据恢复到内存跳表
    replay();

    // 再打开 WAL 句柄。r+b = 可读可写不截断；
    // 若文件不存在（首次启动）则 w+b 创建
    wal_ = ::fopen(wal_path_.c_str(), "r+b");
    if (!wal_) {
        wal_ = ::fopen(wal_path_.c_str(), "w+b");
    } else {
        // 定位到文件末尾，后续 fwrite 就是追加写
        ::fseek(wal_, 0, SEEK_END);
        wal_size_ = static_cast<uint64_t>(::ftell(wal_));
    }
    if (!wal_) {
        LOG_ERROR("open wal %s failed: %s", wal_path_.c_str(), strerror(errno));
    }
    LOG_INFO("KVStore ready, wal=%s, entries=%zu", wal_path_.c_str(), list_.size());
}

KVStore::~KVStore() {
    if (wal_) {
        ::fflush(wal_);   // 把用户态缓冲刷到内核
        ::fclose(wal_);
    }
    // 注意：这里不做 fsync —— 正常退出由 fflush+内核回写兜底，
    // 极端掉电丢失上限由 fsync_ 策略决定
}

// ----------------------------------------------------------------------------
// 追加一条 WAL 记录
// 流程：组装 body -> 计算 CRC -> 拼完整记录 -> fwrite -> (可选) fsync
// ----------------------------------------------------------------------------
bool KVStore::append_record(uint8_t op, const std::string& key,
                            const std::string& value) {
    if (!wal_) return false;

    // body = op + klen + vlen + key + value（CRC 的计算范围）
    std::string body;
    body.push_back(static_cast<char>(op));
    put_le32(body, static_cast<uint32_t>(key.size()));
    put_le32(body, static_cast<uint32_t>(value.size()));
    body.append(key);
    body.append(value);

    // 完整记录 = body + crc32(body)
    std::string rec = body;
    put_le32(rec, crc32_calc(body.data(), body.size()));

    size_t n = ::fwrite(rec.data(), 1, rec.size(), wal_);
    if (n != rec.size()) {
        LOG_ERROR("wal fwrite failed: %s", strerror(errno));
        return false;
    }
    if (fsync_) {
        // 两步落盘：
        //   fflush : 用户态缓冲 -> 内核页缓存
        //   fsync  : 内核页缓存 -> 物理磁盘（掉电也不丢）
        ::fflush(wal_);
        ::fsync(::fileno(wal_));
    }
    wal_size_ += rec.size();
    return true;
}

// ----------------------------------------------------------------------------
// 重放 WAL：崩溃恢复的核心
// 策略：整文件读入内存（P1 数据量小可接受；数据量大时应改流式分段读），
//       逐条解析 + CRC 校验，任何异常立即停止并把文件截断到最后一条完整记录。
// 【为什么"遇到坏记录就停止"而不是"跳过继续"？】
//   记录损坏通常意味着崩溃点，后面的数据即便解析"成功"也不可信
//   （可能错位解析成看似合法的垃圾）。从坏点起全部丢弃是安全保守的做法。
// ----------------------------------------------------------------------------
void KVStore::replay() {
    FILE* f = ::fopen(wal_path_.c_str(), "rb");
    if (!f) return; // 首次启动，无 WAL

    std::string buf;
    char chunk[4096];
    size_t n;
    while ((n = ::fread(chunk, 1, sizeof(chunk), f)) > 0) {
        buf.append(chunk, n);
    }
    ::fclose(f);

    size_t off = 0;       // 当前解析位置
    size_t last_good = 0; // 最后一条完整记录的结束偏移（截断点）
    while (true) {
        const size_t kHeader = 1 + 4 + 4; // op(1) + klen(4) + vlen(4)
        if (buf.size() - off < kHeader) break; // 头都不完整 -> 崩溃尾巴

        uint8_t op = static_cast<uint8_t>(buf[off]);
        uint32_t klen = get_le32(buf.data() + off + 1);
        uint32_t vlen = get_le32(buf.data() + off + 5);
        // 长度合法性防御：非法值说明解析错位，立即停止
        // （上限 64MB 防止把脏数据误读成超大长度导致内存耗尽）
        if (op > OP_DEL || klen > (64u << 20) || vlen > (64u << 20)) break;

        size_t total = kHeader + klen + vlen + 4; // 整条记录大小（含 CRC）
        if (buf.size() - off < total) break;      // 记录体不完整 -> 崩溃尾巴

        // CRC 校验：残缺/错位的记录在这里被拦截
        const char* body = buf.data() + off;
        uint32_t expect = get_le32(buf.data() + off + kHeader + klen + vlen);
        uint32_t actual = crc32_calc(body, kHeader + klen + vlen);
        if (expect != actual) break;

        // 应用到内存跳表（与正常写入路径的内存效果完全一致）
        std::string key(buf.data() + off + kHeader, klen);
        std::string value(buf.data() + off + kHeader + klen, vlen);
        if (op == OP_SET) {
            list_.insert_or_update(key, value);
        } else {
            list_.erase(key);
        }
        off += total;
        last_good = off;
    }

    // 有残缺尾巴：用 truncate 截掉，保证下次启动不再看到坏数据
    if (last_good < buf.size()) {
        LOG_WARN("wal has %zu corrupt/truncated bytes, truncate to %zu",
                 buf.size() - last_good, last_good);
        ::truncate(wal_path_.c_str(), static_cast<off_t>(last_good));
    }
    wal_size_ = last_good;
}

// ----------------------------------------------------------------------------
// SET：先落 WAL（持久性），再改内存（可用性）—— 顺序不能反！
// 若先改内存后写 WAL，崩溃窗口内的写入会"内存有、磁盘无"，恢复即丢数据
// ----------------------------------------------------------------------------
bool KVStore::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!append_record(OP_SET, key, value)) return false;
    list_.insert_or_update(key, value);
    maybe_compact(); // 写入可能让 WAL 突破阈值
    return true;
}

// GET：纯内存读，O(log n)
bool KVStore::get(const std::string& key, std::string* value) {
    std::lock_guard<std::mutex> lk(mu_);
    std::string* v = list_.find(key);
    if (!v) return false;
    if (value) *value = *v; // 拷贝出参：锁外使用安全
    return true;
}

// ----------------------------------------------------------------------------
// DEL：key 不存在直接返回 false（不产生 WAL 记录，避免垃圾日志）；
//      存在则先记 DEL 日志再删内存，与 SET 同样的"先 WAL 后内存"顺序
// ----------------------------------------------------------------------------
bool KVStore::del(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!list_.erase(key)) return false;
    append_record(OP_DEL, key, ""); // DEL 记录 value 为空串
    return true;
}

void KVStore::scan(const std::string& begin, const std::string& end,
                   std::vector<std::pair<std::string, std::string>>& out) {
    std::lock_guard<std::mutex> lk(mu_);
    list_.scan(begin, end, out);
}

void KVStore::dump_all(std::vector<std::pair<std::string, std::string>>& out) {
    std::lock_guard<std::mutex> lk(mu_);
    list_.dump_all(out);
}

size_t KVStore::size() {
    std::lock_guard<std::mutex> lk(mu_);
    return list_.size();
}

void KVStore::maybe_compact() {
    if (wal_size_ > compact_threshold_) {
        compact();
    }
}

// ----------------------------------------------------------------------------
// WAL 压缩：把"全部历史操作日志"重写成"每 key 一条最新值"
// 流程（崩溃安全的三步走）：
//   1. dump 内存全量 -> 写 data.wal.tmp（只含每 key 最新值）
//   2. fflush + fsync 临时文件（保证新文件完整落盘）
//   3. fclose 旧句柄 -> rename(tmp, wal) 原子替换 -> 重新打开
// 任意一步崩溃：rename 未执行，旧 WAL 原样保留，重启后恢复流程不受影响
// ----------------------------------------------------------------------------
void KVStore::compact() {
    std::vector<std::pair<std::string, std::string>> all;
    list_.dump_all(all);

    std::string tmp_path = wal_path_ + ".tmp";
    FILE* f = ::fopen(tmp_path.c_str(), "wb");
    if (!f) {
        LOG_ERROR("compact: open %s failed: %s", tmp_path.c_str(), strerror(errno));
        return;
    }

    for (auto& kv : all) {
        // 每条数据重写为一条 SET 记录（复用 append_record 的格式逻辑）
        std::string body;
        body.push_back(static_cast<char>(OP_SET));
        put_le32(body, static_cast<uint32_t>(kv.first.size()));
        put_le32(body, static_cast<uint32_t>(kv.second.size()));
        body.append(kv.first);
        body.append(kv.second);
        std::string rec = body;
        put_le32(rec, crc32_calc(body.data(), body.size()));
        ::fwrite(rec.data(), 1, rec.size(), f);
    }
    ::fflush(f);
    ::fsync(::fileno(f));
    ::fclose(f);

    // 原子替换：rename 同一文件系统内是原子的
    ::fclose(wal_);
    ::rename(tmp_path.c_str(), wal_path_.c_str());
    wal_ = ::fopen(wal_path_.c_str(), "r+b");
    ::fseek(wal_, 0, SEEK_END);
    wal_size_ = static_cast<uint64_t>(::ftell(wal_));
    LOG_INFO("compact done: %zu entries, wal now %llu bytes",
             all.size(), static_cast<unsigned long long>(wal_size_));
}
