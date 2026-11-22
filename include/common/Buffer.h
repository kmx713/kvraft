#pragma once
// ============================================================================
// Buffer.h - 应用层读写缓冲区（muduo 风格设计，解决 TCP 粘包/拆包的载体）
// ============================================================================
//
// 【为什么必须要有应用层 Buffer？】
//   TCP 是"字节流"协议，没有消息边界：
//     - 粘包：客户端发了 3 条命令，一次 recv() 可能收到 2.5 条
//     - 拆包：1 条命令可能被拆成多次 recv() 才凑齐
//   所以 recv() 到的数据必须先囤在 Buffer 里，攒够一条完整帧再解析。
//
// 【内存布局】（read_idx_ 到 write_idx_ 之间是"可读区"）
//
//   +-------------------+------------------+------------------+
//   |  已消费(可回收)    |   可读数据        |   可写空间        |
//   +-------------------+------------------+------------------+
//   0               read_idx_          write_idx_        buf_.size()
//
//   前部空间回收：可写空间不足时，先把可读区整体前移（memmove）腾出尾部，
//   还不够就扩容。这样长期运行内存不会无限增长。
// ============================================================================

#include <errno.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <vector>

class Buffer {
public:
    explicit Buffer(size_t init_size = 4096)
        : buf_(kCheapPrepend + init_size),
          read_idx_(kCheapPrepend),
          write_idx_(kCheapPrepend) {}

    // ---- 可读区操作 ----
    size_t readable() const { return write_idx_ - read_idx_; }

    // 可读区起始地址（解析帧时从这里开始读）
    const char* peek() const { return buf_.data() + read_idx_; }

    // 消费 n 字节（解析完一条帧后调用，前移 read_idx_）
    void retrieve(size_t n) {
        if (n >= readable()) {
            retrieve_all();
        } else {
            read_idx_ += n;
        }
    }

    // 清空缓冲区（索引重置到起点，内存保留复用）
    void retrieve_all() {
        read_idx_ = kCheapPrepend;
        write_idx_ = kCheapPrepend;
    }

    // ---- 可写区操作 ----
    char* begin_write() { return buf_.data() + write_idx_; }
    size_t writable() const { return buf_.size() - write_idx_; }

    // 确保至少 len 字节可写；不足时：先回收前部空间，再不够就扩容
    void ensure_writable(size_t len) {
        if (writable() < len) {
            make_space(len);
        }
    }

    // 把数据追加到可写区（发送方组装帧时使用）
    void append(const char* data, size_t len) {
        ensure_writable(len);
        memcpy(begin_write(), data, len);
        write_idx_ += len;
    }

    void append(const std::string& s) { append(s.data(), s.size()); }

    // 取出全部可读数据为 string（调试用）
    std::string retrieve_as_string() {
        std::string s(peek(), readable());
        retrieve_all();
        return s;
    }

    // ------------------------------------------------------------------
    // 从非阻塞 fd 读取数据到可写区（网络层核心读入口）
    // 返回：>0 读取字节数；=0 对端正常关闭（FIN）；<0 出错（errno 出参）
    // LT（水平触发）模式下一次 recv 就够，没读完的数据会再次触发 EPOLLIN
    // ------------------------------------------------------------------
    ssize_t read_fd(int fd, int* saved_errno) {
        ensure_writable(4096); // 至少留 4K 读空间
        ssize_t n = ::recv(fd, begin_write(), writable(), 0);
        if (n < 0) {
            *saved_errno = errno;
        } else if (n > 0) {
            write_idx_ += static_cast<size_t>(n);
        }
        return n;
    }

    // 把可读数据发送到 fd（发送路径）；>0 已发送字节；<0 出错
    ssize_t write_fd(int fd, int* saved_errno) {
        if (readable() == 0) return 0;
        ssize_t n = ::send(fd, peek(), readable(), 0);
        if (n < 0) {
            *saved_errno = errno;
        } else if (n > 0) {
            retrieve(static_cast<size_t>(n)); // 发成功的部分标记已消费
        }
        return n;
    }

private:
    // 腾出至少 len 字节可写空间
    void make_space(size_t len) {
        // 前部 + 尾部空间够用 -> 把可读数据 memmove 到头部复用内存
        if (read_idx_ + writable() < kCheapPrepend + len) {
            buf_.resize(write_idx_ + len); // 还不够 -> 直接扩容
        } else {
            size_t readable_bytes = readable();
            memmove(buf_.data() + kCheapPrepend, buf_.data() + read_idx_,
                    readable_bytes);
            read_idx_ = kCheapPrepend;
            write_idx_ = read_idx_ + readable_bytes;
        }
    }

    static const size_t kCheapPrepend = 8; // 前部预留字节（对齐友好）
    std::vector<char> buf_;
    size_t read_idx_;   // 可读区起点（已消费边界）
    size_t write_idx_;  // 可写区起点（已写入边界）
};
