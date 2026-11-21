cmake_minimum_required(VERSION 3.0)
project(kvraft CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -g -Wall")

include_directories(${PROJECT_SOURCE_DIR}/include)

find_package(Threads REQUIRED)

# P1: 单机 KV 工具
add_executable(kvtool
    src/server/main.cpp
    src/storage/KVStore.cpp
)
target_link_libraries(kvtool ${CMAKE_THREAD_LIBS_INIT})

# 跳表单元测试
add_executable(test_skiplist
    test/test_skiplist.cpp
)
target_link_libraries(test_skiplist ${CMAKE_THREAD_LIBS_INIT})
