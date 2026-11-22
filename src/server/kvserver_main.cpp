// ============================================================================
// kvserver_main.cpp - Raft 集群节点入口（P3）
// ============================================================================
//
// 用法：
//   ./kvserverd <my_id> <data_dir> <addr1> <addr2> <addr3>
//
// 示例（三节点集群，本机演示用三个端口）：
//   ./kvserverd 0 /tmp/kvraft/n0 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103
//   ./kvserverd 1 /tmp/kvraft/n1 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103
//   ./kvserverd 2 /tmp/kvraft/n2 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103
//
// 三条命令可以在同一台机器跑（端口不同），也可以分布在三台机器
//（addr 写各自真实 IP）。my_id 是自己在集群中的编号（0 起）。
// ============================================================================

#include <stdio.h>
#include <stdlib.h>

#include <string>
#include <vector>

#include "raft/RaftNode.h"

int main(int argc, char* argv[]) {
    if (argc < 5) {
        fprintf(stderr,
                "usage: %s <my_id> <data_dir> <addr1> <addr2> <addr3>\n"
                "  my_id   : 本节点编号（0 起，必须 < 节点总数）\n"
                "  data_dir: 数据目录（raft.wal / role / pid）\n"
                "  addrN   : 全部节点的 ip:port（顺序即节点编号）\n",
                argv[0]);
        return 1;
    }

    int my_id = std::atoi(argv[1]);
    std::string data_dir = argv[2];
    std::vector<std::string> addrs;
    for (int i = 3; i < argc; ++i) addrs.push_back(argv[i]);

    if (my_id < 0 || my_id >= static_cast<int>(addrs.size())) {
        fprintf(stderr, "ERR my_id %d out of range [0, %zu)\n", my_id,
                addrs.size());
        return 1;
    }

    RaftNode node(my_id, addrs, data_dir);
    if (!node.start()) { // start() 内部进入事件循环，正常情况不返回
        fprintf(stderr, "node %d failed to start\n", my_id);
        return 1;
    }
    return 0;
}
