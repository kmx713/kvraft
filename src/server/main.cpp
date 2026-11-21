// ============================================================================
// main.cpp - P1 单机 KV 工具（交互式 REPL，验证存储引擎）
// ============================================================================
//
// 用法：
//   ./kvtool <data_dir>            进入交互模式
//   ./kvtool <data_dir> set k v    单条命令模式（适合脚本/测试）
//   ./kvtool <data_dir> get k
//   ./kvtool <data_dir> del k
//   ./kvtool <data_dir> scan b e
//   ./kvtool <data_dir> size
//   ./kvtool <data_dir> compact
//
// 【崩溃恢复演示】（面试可现场演示 WAL 效果）
//   1. ./kvtool /tmp/kvdata 后 set 几个 key
//   2. 另开终端 kill -9 <pid> 强杀进程（模拟崩溃）
//   3. 重新启动 ./kvtool /tmp/kvdata，get 之前的 key —— 数据都在
//
// 注意 P1 是单机工具；P2 会把同样的 KVStore 挂到 epoll 网络服务上，
// P3 会把它变成 Raft 集群的"状态机"—— 分阶段复用同一套存储层。
// ============================================================================

#include <stdio.h>
#include <string.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "storage/KVStore.h"

// 命令帮助信息
static void print_help() {
    printf("commands:\n"
           "  set <key> <value>\n"
           "  get <key>\n"
           "  del <key>\n"
           "  scan <begin> <end>   按范围扫描 [begin, end]\n"
           "  all                  全量导出\n"
           "  size                 键数量\n"
           "  compact              压缩 WAL\n"
           "  quit                 退出\n");
}

// 执行一条解析好的命令
// tok  : 按空白切分后的命令词
// quit : 出参，用户是否请求退出
static bool exec_cmd(KVStore* store, const std::vector<std::string>& tok,
                     bool* quit) {
    const std::string& cmd = tok[0];
    *quit = false;

    if (cmd == "quit" || cmd == "exit") {
        *quit = true;
    } else if (cmd == "set" && tok.size() >= 3) {
        // 返回 true 表示 key 已存在（更新），false 表示新插入
        printf("%s\n", store->set(tok[1], tok[2]) ? "OK" : "FAIL");
    } else if (cmd == "get" && tok.size() >= 2) {
        std::string v;
        if (store->get(tok[1], &v)) {
            printf("%s\n", v.c_str());
        } else {
            printf("NOT_FOUND\n");
        }
    } else if (cmd == "del" && tok.size() >= 2) {
        printf("%s\n", store->del(tok[1]) ? "OK" : "NOT_FOUND");
    } else if (cmd == "scan" && tok.size() >= 3) {
        std::vector<std::pair<std::string, std::string>> out;
        store->scan(tok[1], tok[2], out);
        for (auto& kv : out) printf("%s = %s\n", kv.first.c_str(), kv.second.c_str());
        printf("(%zu rows)\n", out.size());
    } else if (cmd == "all") {
        std::vector<std::pair<std::string, std::string>> out;
        store->dump_all(out);
        for (auto& kv : out) printf("%s = %s\n", kv.first.c_str(), kv.second.c_str());
        printf("(%zu rows)\n", out.size());
    } else if (cmd == "size") {
        printf("%zu\n", store->size());
    } else if (cmd == "compact") {
        store->compact();
        printf("OK\n");
    } else if (cmd == "help") {
        print_help();
    } else {
        printf("ERR bad command\n");
        print_help();
    }
    return true;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <data_dir> [cmd args...]\n", argv[0]);
        return 1;
    }

    // 构造 KVStore 会自动完成：建目录 -> 重放 WAL -> 打开追加句柄
    KVStore store(argv[1]);

    if (argc == 2) {
        // ---------- 交互模式：逐行读入命令 ----------
        printf("kvtool ready. data dir: %s\n", argv[1]);
        print_help();
        std::string line;
        while (true) {
            printf("kv> ");
            fflush(stdout); // 提示符立即显示（stdout 默认行缓冲，REPL 无换行需手动刷）
            if (!std::getline(std::cin, line)) break; // EOF（Ctrl+D）退出

            // 按空白切分：stringstream 自动跳过连续空格/制表符
            std::vector<std::string> tok;
            std::istringstream iss(line);
            std::string w;
            while (iss >> w) tok.push_back(w);
            if (tok.empty()) continue; // 空行

            bool quit = false;
            exec_cmd(&store, tok, &quit);
            if (quit) break;
        }
        printf("bye\n");
    } else {
        // ---------- 单条命令模式：argv[2..] 即命令词 ----------
        std::vector<std::string> tok;
        for (int i = 2; i < argc; ++i) tok.push_back(argv[i]);
        bool quit = false;
        exec_cmd(&store, tok, &quit);
    }
    return 0;
}
