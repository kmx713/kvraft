# KVRaft - 分布式 KV 存储引擎

基于 **Raft 一致性算法** 的三节点分布式 KV 存储引擎（C++14 / Linux / epoll）。

手写全部核心组件，零第三方依赖：epoll Reactor 网络层、自定义二进制协议、
跳表存储引擎、WAL 预写日志、Raft 选举与日志复制、故障自动转移。

## 架构

```
                    ┌────────────── 客户端 kvclient ──────────────┐
                    │  文本命令 → 二进制帧（重定向自动跟 Leader）   │
                    └──────────────┬─────────────────────────────┘
                                   ▼
   ┌─────────────── 节点 0 (Leader) ───────────────┐
   │  TcpServer/TcpClient (epoll + 非阻塞 IO)      │
   │  ──► RaftNode（选举/日志复制/提交推进）        │
   │        ──► SkipList 状态机 + raft.wal 持久化  │
   └───────┬───────────────────────────┬───────────┘
           │ AppendEntries RPC         │
   ┌───────▼─────────┐        ┌────────▼────────┐
   │ 节点 1 (Follower)│        │ 节点 2 (Follower)│
   └─────────────────┘        └─────────────────┘
```

### 目录结构

```
kvraft/
├── include/
│   ├── common/    Protocol.h（帧协议）Buffer.h（应用层缓冲）Log.h
│   ├── net/       EventLoop.h TcpServer.h TcpClient.h   <- P2 网络层
│   ├── raft/      RaftMessages.h（RPC 序列化）RaftNode.h <- P3 共识层
│   └── storage/   SkipList.h（跳表）KVStore.h（WAL 单机引擎）<- P1 存储层
├── src/
│   ├── raft/      RaftNode.cpp（选举 + 日志复制 + 状态机应用）
│   ├── net/       EventLoop.cpp TcpServer.cpp TcpClient.cpp
│   ├── storage/   KVStore.cpp
│   ├── server/    kvserver_main.cpp（集群节点入口）
│   ├── client/    kvclient.cpp（命令行客户端）
│   └── common/    Protocol.cpp（CRC16/编帧/拆帧）
├── test/          test_skiplist.cpp test_protocol.cpp
└── scripts/       demo_cluster.sh（故障演练）bench.sh（压测）
```

## 快速开始

```bash
cd build
cmake ..          # 老版 CMake 也兼容（不用 -S/-B）
make -j4

# 单元测试
./test_skiplist     # 跳表：10 万次随机操作与 std::map 差分对比
./test_protocol     # 协议：粘包/拆包/CRC/Raft 消息序列化

# 一键故障演练（启动集群 -> 写入 -> 强杀 Leader -> 验证恢复）
bash scripts/demo_cluster.sh

# 手动启动三节点集群
./kvserverd 0 /tmp/kvraft/n0 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103 &
./kvserverd 1 /tmp/kvraft/n1 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103 &
./kvserverd 2 /tmp/kvraft/n2 127.0.0.1:9101 127.0.0.1:9102 127.0.0.1:9103 &

# 连任意节点使用（Follower 自动重定向 Leader）
./kvclient 127.0.0.1:9102 set name kmx
./kvclient 127.0.0.1:9102 get name

# 压测
bash scripts/bench.sh 10000 8 127.0.0.1:9101
```

## 关键设计与面试要点

### 1. 网络：epoll Reactor（P2）
- **ET/LT**：监听 fd 与连接 fd 用 LT 模式，`accept` 用 while 循环收完（EAGAIN 退出）
- **粘包/拆包**：帧格式 `magic(2B)+type(1B)+body_len(4B)+crc16(2B)+body`，
  收到字节流先进 Buffer，循环 `try_decode_frame` 直到解不出完整帧
- **背压**：内核发送缓冲满（EAGAIN）时数据排队 out_buf、挂 EPOLLOUT，
  写完自动摘除，避免 busy loop；TCP_NODELAY 关闭 Nagle（心跳延迟敏感）
- **单线程无锁**：所有回调在 EventLoop 循环线程内串行执行（muduo 思想），
  无竞态；定时器用 epoll_wait 超时实现，Raft 选举/心跳全靠它驱动

### 2. 存储：跳表 + WAL（P1）
- 跳表 12 层、1/4 晋升概率，平均 O(log n)；10 万次随机操作与 std::map 差分测试
- WAL 追加写 + fsync + CRC 校验，`kill -9` 后重放恢复，尾部残缺记录自动截断
- WAL 超 64MB 自动 Compact：dump 全量 -> 原子 rename 替换（崩溃安全）

### 3. 共识：Raft（P3）
- **选举**：随机超时 800~1500ms 防活锁；任期号单调递增；投票前持久化
- **日志复制**：AppendEntries 一致性检查（prev_log_index/term），
  失败回退 next_index 重传修复；commit 推进只直接提交本任期条目（论文 5.4.2）
- **NOOP 技巧**：Leader 上任立刻追加空条目，间接提交前任积压日志，加速恢复
- **客户端语义**：写请求必须经 Leader 且多数派提交后才回 OK（线性一致写）；
  Follower 返回 REDIRECT 帧让客户端自动跟 Leader
- **崩溃恢复**：raft.wal 记录 META（term/voted_for/commit）与 ENTRY，
  重启重放；fsync 保证已回复 OK 的数据崩溃不丢

### 4. 已知限制与演进方向（面试加分项）
- 单线程 Reactor：可扩展多 Reactor（one loop per thread）+ 主从 fd 分发
- kvclient 每命令建连：压测含建连开销；可做长连接 + pipeline 批量发
- 读请求非严格线性一致（Follower 状态机可能落后心跳间隔）：
  演进方向 ReadIndex / Lease Read（etcd 的方案）
- 快照（InstallSnapshot RPC）未实现：长日志节点重启重放全量 WAL，可加快照截断
- 组提交（group commit）批量 fsync 可提升写吞吐（吞吐/安全取舍）

## 压测结果

```
bash scripts/bench.sh 10000 8
# 输出示例（虚拟机实测会偏低，含每条命令建连开销）：
# 总耗时   : XX.XXs
# 吞吐     : XXXX ops/s
```

## 五、开发里程碑（4 个阶段）

| 阶段 | 内容 | 产出 |
|------|------|------|
| P1 | SkipList + WAL + 单机 KVServer | 单机版 KV，跑通单元测试 |
| P2 | epoll 网络层 + 协议 + 命令行客户端 | 网络版单机 KV |
| P3 | Raft 选举 + 日志复制 | 3 节点集群，kill Leader 自动恢复 |
| P4 | 压测 + 故障演练脚本 + README | 完整项目 + 数据报告 |



