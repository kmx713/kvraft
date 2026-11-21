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

# P2 将新增：kvserverd（epoll 网络服务）、kvclient
# P3 将新增：raft 库与集群节点目标
