# KVRaft — 分布式 KV 存储引擎设计方案

## 一、项目定位

一个用 C++ 从零实现的分布式键值存储，核心是 **Raft 一致性算法 + SkipList 存储引擎**。
不依赖任何第三方框架（网络、序列化、日志全部手写）， demonstrating 完整的分布式系统能力。

**技术栈**：C++11/17、Linux epoll、TCP 自定义二进制协议、Raft、SkipList、WAL、CMake

**集群形态**：3 节点 Raft 集群（1 Leader + 2 Follower），自动选主、故障自动恢复、写请求自动重定向。

## 二、核心特性（简历亮点）

1. **Raft 一致性算法**：Leader 选举（随机选举超时）、日志复制、提交规则、任期机制、节点崩溃恢复
2. **SkipList 存储引擎**：手写跳表（多级索引、随机层高），O(log n) 插入/查询
3. **WAL 预写日志**：写操作先落盘再应用内存，掉电不丢数据，fsync 刷盘策略
4. **epoll 高并发网络层**：非阻塞 IO + 事件驱动，Reactor 模型（复用 tmms 的架构经验）
5. **自定义二进制协议**：帧头 + 长度 + 校验，解决 TCP 粘包/拆包
6. **客户端智能路由**：自动发现 Leader，非 Leader 节点请求重定向
7. **故障演练脚本**：kill 掉 Leader 后 3 秒内自动选出新 Leader，数据不丢

## 三、文件夹结构

```
kvraft/
├── CMakeLists.txt              # 兼容老版本 CMake（不用 -S/-B）
├── README.md                   # 架构图 + 运行方式 + 压测结果
├── include/                    # 所有头文件
│   ├── common/
│   │   ├── Log.h               # 日志模块（FileLog/LogStream 风格，复用 tmms 经验）
│   │   ├── Buffer.h            # 应用层读写缓冲区
│   │   ├── Protocol.h          # 自定义协议：帧头/消息类型/编解码
│   │   └── TcpConn.h           # TCP 连接封装（非阻塞 fd + Buffer）
│   ├── net/
│   │   ├── EventLoop.h         # epoll 事件循环（单 Reactor）
│   │   ├── Acceptor.h          # 监听 fd，accept 新连接
│   │   └── TcpServer.h         # 连接管理 + 消息分发回调
│   ├── storage/
│   │   ├── SkipList.h          # 跳表核心（模板实现）
│   │   └── KVStore.h           # 存储引擎：SkipList + WAL + 快照恢复
│   ├── raft/
│   │   ├── RaftNode.h          # Raft 状态机（Follower/Candidate/Leader）
│   │   ├── RaftLog.h           # Raft 日志条目管理 + 持久化
│   │   └── RaftPeer.h          # 节点间通信（RPC：RequestVote/AppendEntries）
│   └── server/
│       ├── KVServer.h          # 整合 Raft + 存储 + 网络的服务节点
│       └── ClientSession.h     # 客户端会话：解析 SET/GET/DEL 命令
├── src/
│   ├── common/  （Log.cpp、Protocol.cpp、TcpConn.cpp）
│   ├── net/     （EventLoop.cpp、Acceptor.cpp、TcpServer.cpp）
│   ├── storage/ （KVStore.cpp）
│   ├── raft/    （RaftNode.cpp、RaftLog.cpp、RaftPeer.cpp）
│   └── server/  （main.cpp、KVServer.cpp）
├── client/
│   └── kv_client.cpp           # 命令行客户端（自动找 Leader + 重定向跟随）
├── scripts/
│   ├── start_cluster.sh        # 一键启动 3 节点集群
│   └── chaos_test.sh           # 故障演练：随机 kill 节点验证恢复
└── test/
    ├── test_skiplist.cpp       # 跳表单元测试（10万级随机插入/查询比对）
    └── bench.cpp               # 压测：多线程并发读写 QPS
```

## 四、模块设计要点

### 1. 存储层（storage）
- **SkipList**：最大 12 层，每层随机晋升概率 1/4；支持 GET/SET/DEL/SCAN
- **WAL**：追加写二进制日志（op + key + value + crc32），启动时重放恢复
- 写路径：`SET → WAL append + fsync → SkipList 插入 → 响应客户端`

### 2. Raft 层（raft）
- 三状态：Follower / Candidate / Leader，随机选举超时 150~300ms
- **RequestVote RPC**：选举投票，比较 term 与 log 新旧（最后一条日志 term/index）
- **AppendEntries RPC**：Leader 心跳 + 日志复制，多数派确认后 commit
- **持久化状态**：currentTerm / votedFor / log[] 每次变更立即落盘
- 状态机应用：committed 的日志按序应用到 KVStore

### 3. 网络层（net）
- 单 Reactor：epoll_wait + 就绪事件分发，非阻塞 fd + EAGAIN 处理
- 两类端口：**客户端端口**（命令协议）+ **节点间端口**（Raft RPC）

### 4. 协议（common/Protocol）
```
| magic(2B) | type(1B) | body_len(4B) | crc16(2B) | body |
type: CLIENT_REQ / CLIENT_RESP / RAFT_VOTE / RAFT_APPEND / REDIRECT
```
客户端命令文本协议（调试友好）：`SET name kmx` → `OK` / `GET name` → `kmx` / `ERR redirect 127.0.0.1:9001`

## 五、开发里程碑（4 个阶段）

| 阶段 | 内容 | 产出 |
|------|------|------|
| P1 | SkipList + WAL + 单机 KVServer | 单机版 KV，跑通单元测试 |
| P2 | epoll 网络层 + 协议 + 命令行客户端 | 网络版单机 KV |
| P3 | Raft 选举 + 日志复制 | 3 节点集群，kill Leader 自动恢复 |
| P4 | 压测 + 故障演练脚本 + README | 完整项目 + 数据报告 |



