# ============================================================================
# KVRaft - 分布式 KV 存储引擎 构建脚本
# 兼容老版本 CMake（不用 -S/-B 参数），构建方式：
#   mkdir build && cd build
#   cmake ..
#   make
# ============================================================================

cmake_minimum_required(VERSION 3.0)
project(kvraft CXX)

# C++14：GCC 7.5 完整支持，且够用（智能指针、lambda、自动推导）
# 不上 C++17 是为了兼容老编译器环境
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 默认 Release（-O2 优化）；调试时可 cmake -DCMAKE_BUILD_TYPE=Debug ..
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -Wall")  # -O2 优化 + -g 保留调试符号（GDB 可用）

# 所有头文件统一放 include/，全局包含
include_directories(${PROJECT_SOURCE_DIR}/include)

# std::mutex 在老 GCC 上需要链接 pthread
find_package(Threads REQUIRED)

# ---------- P1 目标：单机 KV 工具 ----------
add_executable(kvtool
    src/server/main.cpp       # REPL 入口
    src/storage/KVStore.cpp   # WAL + SkipList 存储引擎
)
target_link_libraries(kvtool ${CMAKE_THREAD_LIBS_INIT})

# ---------- 跳表单元测试 ----------
add_executable(test_skiplist
    test/test_skiplist.cpp    # 纯头文件模板 + 测试逻辑
)
target_link_libraries(test_skiplist ${CMAKE_THREAD_LIBS_INIT})

# ---------- P2/P3 目标：Raft 集群节点 ----------
# 网络层源文件（P2）+ Raft 状态机（P3）一起编成节点可执行文件
add_executable(kvserverd
    src/server/kvserver_main.cpp  # 集群节点入口
    src/raft/RaftNode.cpp         # Raft 状态机（选举 + 日志复制）
    src/net/EventLoop.cpp         # epoll 事件循环
    src/net/TcpServer.cpp         # TCP 服务端（连接管理/拆帧/背压）
    src/net/TcpClient.cpp         # TCP 客户端（节点间主动连接/自动重连）
    src/common/Protocol.cpp       # 帧协议（magic/type/len/crc16）
)
target_link_libraries(kvserverd ${CMAKE_THREAD_LIBS_INIT})

# ---------- P2 目标：命令行客户端 ----------
# 独立实现阻塞式 socket 收发（不依赖 EventLoop，客户端要的就是简单直接）
add_executable(kvclient
    src/client/kvclient.cpp
    src/common/Protocol.cpp
)
target_link_libraries(kvclient ${CMAKE_THREAD_LIBS_INIT})

# ---------- 协议单元测试 ----------
add_executable(test_protocol
    test/test_protocol.cpp
    src/common/Protocol.cpp
)
target_link_libraries(test_protocol ${CMAKE_THREAD_LIBS_INIT})
