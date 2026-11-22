// ============================================================================
// kvclient.cpp - 命令行客户端（P2/P4）
// ============================================================================
//
// 用法：
//   ./kvclient <ip:port>                      进入交互模式（REPL）
//   ./kvclient <ip:port> set k v              单条命令模式（压测脚本用）
//   ./kvclient <ip:port> get k
//
// 【Leader 重定向】
//   连上 Follower 时服务器会回 REDIRECT 帧（Leader 的 ip:port），
//   客户端自动重连重发 —— 用户无感知，随便连哪个节点都能用。
//
// 【实现选择】
//   用阻塞 socket 而不是复用 EventLoop：客户端一次只有一条连接、
//   一问一答，阻塞式最简单也最不容易出 bug。网络库留给服务端。
// ============================================================================

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/Protocol.h"

// 连接服务器（阻塞，超时 3 秒），失败返回 -1
static int dial(const std::string& addr) {
    size_t pos = addr.rfind(':');
    if (pos == std::string::npos) return -1;
    std::string host = addr.substr(0, pos);
    uint16_t port = static_cast<uint16_t>(std::atoi(addr.substr(pos + 1).c_str()));

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons(port);
    a.sin_addr.s_addr = ::inet_addr(host.c_str());

    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&a), sizeof(a)) < 0) {
        ::close(fd);
        return -1;
    }
    int on = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    return fd;
}

// 收满 n 字节（阻塞；一次 recv 可能只来半条，必须循环）
static bool recv_full(int fd, void* buf, size_t n) {
    char* p = static_cast<char*>(buf);
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::recv(fd, p + got, n - got, 0);
        if (r <= 0) return false; // 对端关闭 / 出错
        got += static_cast<size_t>(r);
    }
    return true;
}

// 发一帧并收回复。返回 0 成功；-1 网络错误
// resp_type / resp_body : 出参
static int rpc_once(int fd, uint8_t type, const std::string& body,
                    uint8_t* resp_type, std::string* resp_body) {
    std::string frame = encode_frame(type, body);
    if (::send(fd, frame.data(), frame.size(), 0) < 0) return -1;

    // 先收 9 字节帧头，校验 magic，再按 body_len 收主体
    char head[kFrameHeaderSize];
    if (!recv_full(fd, head, kFrameHeaderSize)) return -1;
    uint16_t magic = static_cast<uint8_t>(head[0]) |
                     (static_cast<uint16_t>(static_cast<uint8_t>(head[1])) << 8);
    if (magic != kFrameMagic) return -1;
    uint8_t rtype = static_cast<uint8_t>(head[2]);
    uint32_t blen = static_cast<uint8_t>(head[3]) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(head[4])) << 8) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(head[5])) << 16) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(head[6])) << 24);

    resp_body->resize(blen);
    if (blen && !recv_full(fd, &(*resp_body)[0], blen)) return -1;
    *resp_type = rtype;
    return 0;
}

// 发命令并处理重定向（最多跟 3 次，防止配置错乱成环）
// 返回最终回复文本；网络故障返回 "ERR connection failed"
static std::string rpc(const std::string& addr, const std::string& cmd) {
    std::string cur = addr;
    for (int redirect = 0; redirect < 3; ++redirect) {
        int fd = dial(cur);
        if (fd < 0) return "ERR connection failed";

        uint8_t rtype = 0;
        std::string rbody;
        int r = rpc_once(fd, MSG_CLIENT_REQ, cmd, &rtype, &rbody);
        ::close(fd); // 一问一答模式：每条命令新建连接（简单可靠；长连接留给压测工具）
        if (r < 0) return "ERR connection failed";

        if (rtype == MSG_REDIRECT) {
            cur = rbody; // 服务器说"Leader 在那边"，换地址重发
            continue;
        }
        return rbody; // MSG_CLIENT_RESP
    }
    return "ERR redirect loop";
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <ip:port> [command words...]\n", argv[0]);
        return 1;
    }
    std::string addr = argv[1];

    // ---------- 单条命令模式 ----------
    if (argc >= 3) {
        std::string cmd;
        for (int i = 2; i < argc; ++i) {
            if (i > 2) cmd += ' ';
            cmd += argv[i];
        }
        printf("%s\n", rpc(addr, cmd).c_str());
        return 0;
    }

    // ---------- 交互模式 ----------
    printf("kvclient -> %s\n", addr.c_str());
    printf("commands: set k v | get k | del k | ping | info | quit\n");
    std::string line;
    while (true) {
        printf("kv> ");
        fflush(stdout);
        if (!std::getline(std::cin, line)) break;
        if (line == "quit" || line == "exit") break;
        if (line.empty()) continue;
        printf("%s\n", rpc(addr, line).c_str());
    }
    return 0;
}
