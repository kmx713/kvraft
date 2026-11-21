// main.cpp - P1 单机 KV 工具（交互式 REPL）
// 用法：
//   ./kvtool <data_dir>            进入交互模式
//   ./kvtool <data_dir> set k v    单条命令模式
//   ./kvtool <data_dir> get k
//   ./kvtool <data_dir> del k
//   ./kvtool <data_dir> scan b e
//   ./kvtool <data_dir> size
//   ./kvtool <data_dir> compact
#include <stdio.h>
#include <string.h>

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "storage/KVStore.h"

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

static bool exec_cmd(KVStore* store, const std::vector<std::string>& tok,
                     bool* quit) {
    const std::string& cmd = tok[0];
    *quit = false;

    if (cmd == "quit" || cmd == "exit") {
        *quit = true;
    } else if (cmd == "set" && tok.size() >= 3) {
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

    KVStore store(argv[1]);

    if (argc == 2) {
        // 交互模式
        printf("kvtool ready. data dir: %s\n", argv[1]);
        print_help();
        std::string line;
        while (true) {
            printf("kv> ");
            fflush(stdout);
            if (!std::getline(std::cin, line)) break;
            std::vector<std::string> tok;
            std::istringstream iss(line);
            std::string w;
            while (iss >> w) tok.push_back(w);
            if (tok.empty()) continue;
            bool quit = false;
            exec_cmd(&store, tok, &quit);
            if (quit) break;
        }
        printf("bye\n");
    } else {
        // 单条命令模式
        std::vector<std::string> tok;
        for (int i = 2; i < argc; ++i) tok.push_back(argv[i]);
        bool quit = false;
        exec_cmd(&store, tok, &quit);
    }
    return 0;
}
