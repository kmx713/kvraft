// KVStore.cpp - WAL 预写日志 + SkipList 内存引擎实现
#include "storage/KVStore.h"

#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/Log.h"

// ---------- CRC32（多项式 0xEDB88320，表驱动） ----------
static uint32_t crc32_calc(const void* data, size_t len) {
    static uint32_t table[256];
    static bool inited = false;
    if (!inited) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j) {
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
        inited = true;
    }
    uint32_t crc = 0xFFFFFFFFu;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

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

KVStore::KVStore(const std::string& dir, bool fsync_on_write)
    : wal_(nullptr), fsync_(fsync_on_write), wal_size_(0),
      compact_threshold_(64ull * 1024 * 1024) {
    // 确保数据目录存在
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        LOG_ERROR("mkdir %s failed: %s", dir.c_str(), strerror(errno));
    }
    wal_path_ = dir + "/data.wal";
    replay();

    wal_ = ::fopen(wal_path_.c_str(), "r+b");
    if (!wal_) {
        wal_ = ::fopen(wal_path_.c_str(), "w+b");
    } else {
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
        ::fflush(wal_);
        ::fclose(wal_);
    }
}

bool KVStore::append_record(uint8_t op, const std::string& key,
                            const std::string& value) {
    if (!wal_) return false;
    std::string body;
    body.push_back(static_cast<char>(op));
    put_le32(body, static_cast<uint32_t>(key.size()));
    put_le32(body, static_cast<uint32_t>(value.size()));
    body.append(key);
    body.append(value);

    std::string rec = body;
    put_le32(rec, crc32_calc(body.data(), body.size()));

    size_t n = ::fwrite(rec.data(), 1, rec.size(), wal_);
    if (n != rec.size()) {
        LOG_ERROR("wal fwrite failed: %s", strerror(errno));
        return false;
    }
    if (fsync_) {
        ::fflush(wal_);
        ::fsync(::fileno(wal_)); // 强制落盘，掉电不丢
    }
    wal_size_ += rec.size();
    return true;
}

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

    // 逐条解析，CRC 校验失败或尾部残缺则截断（崩溃恢复场景）
    size_t off = 0, last_good = 0;
    while (true) {
        const size_t kHeader = 1 + 4 + 4; // op + klen + vlen
        if (buf.size() - off < kHeader) break;
        uint8_t op = static_cast<uint8_t>(buf[off]);
        uint32_t klen = get_le32(buf.data() + off + 1);
        uint32_t vlen = get_le32(buf.data() + off + 5);
        if (op > OP_DEL || klen > (64u << 20) || vlen > (64u << 20)) break; // 非法记录
        size_t total = kHeader + klen + vlen + 4;
        if (buf.size() - off < total) break; // 尾部残缺

        const char* body = buf.data() + off;
        uint32_t expect = get_le32(buf.data() + off + kHeader + klen + vlen);
        uint32_t actual = crc32_calc(body, kHeader + klen + vlen);
        if (expect != actual) break; // CRC 不通过，停止重放

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

    if (last_good < buf.size()) {
        LOG_WARN("wal has %zu corrupt/truncated bytes, truncate to %zu",
                 buf.size() - last_good, last_good);
        ::truncate(wal_path_.c_str(), static_cast<off_t>(last_good));
    }
    wal_size_ = last_good;
}

bool KVStore::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!append_record(OP_SET, key, value)) return false;
    list_.insert_or_update(key, value);
    maybe_compact();
    return true;
}

bool KVStore::get(const std::string& key, std::string* value) {
    std::lock_guard<std::mutex> lk(mu_);
    std::string* v = list_.find(key);
    if (!v) return false;
    if (value) *value = *v;
    return true;
}

bool KVStore::del(const std::string& key) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!list_.erase(key)) return false;
    append_record(OP_DEL, key, "");
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

// 压缩：全量 dump -> 写临时文件 -> fsync -> 原子 rename 替换旧 WAL
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

    ::fclose(wal_);
    ::rename(tmp_path.c_str(), wal_path_.c_str());
    wal_ = ::fopen(wal_path_.c_str(), "r+b");
    ::fseek(wal_, 0, SEEK_END);
    wal_size_ = static_cast<uint64_t>(::ftell(wal_));
    LOG_INFO("compact done: %zu entries, wal now %llu bytes",
             all.size(), static_cast<unsigned long long>(wal_size_));
}
