#pragma once
// ============================================================================
// EventLoop.h - epoll 事件循环（单 Reactor 模型的核心）
// ============================================================================
//
// 【Reactor 模型一句话】
//   一个线程守着 epoll_wait，谁就绪就回调谁 —— 事件驱动、非阻塞 IO。
//   所有连接都注册在这一个循环里，回调在循环线程内串行执行
//   -> 无锁、无竞态（muduo 的 one loop per thread 思想）。
//
// 【为什么单线程够用？】
//   KV 服务的事件处理都是内存操作，单线程轻松打满万级 QPS；
//   真正的瓶颈在磁盘 fsync。多线程 Reactor（P4 压测后按需加）
//   会引入锁复杂度，先简单后扩展。
//
// 【定时器】
//   epoll_wait 的 timeout 参数被用来"顺便"实现定时器：
//   timeout = 最近的定时器 deadline - 当前时间，醒来后检查到期定时器。
//   P3 的 Raft 选举超时/心跳间隔全靠这个机制驱动。
// ============================================================================

#include <sys/epoll.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <vector>

class EventLoop {
public:
    using FdCallback = std::function<void(uint32_t events)>;    // fd 就绪回调
    using TimerCallback = std::function<void()>;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // ---- fd 事件管理 ----
    void add_fd(int fd, uint32_t events, FdCallback cb); // 注册 fd 及就绪回调
    void mod_fd(int fd, uint32_t events);                // 修改监听事件（如加 EPOLLOUT）
    void remove_fd(int fd);                              // 注销 fd（内部 close fd）

    // ---- 定时器 ----
    uint64_t run_after(uint64_t delay_ms, TimerCallback cb);    // 单次定时，返回 id
    uint64_t run_every(uint64_t interval_ms, TimerCallback cb); // 周期定时（Raft 心跳用）
    void cancel_timer(uint64_t id);

    void run();  // 进入事件循环（阻塞，直到 stop）
    void stop(); // 请求退出（atomic 标志，最多 1 秒内退出）

private:
    struct Timer {
        uint64_t id;
        uint64_t deadline;  // 到期时间戳（steady_clock 毫秒）
        uint64_t interval;  // 0 = 单次；>0 = 周期定时器
        TimerCallback cb;
    };

    static uint64_t now_ms();       // 单调时钟毫秒（不受系统改时间影响）
    void fire_expired_timers();     // 执行到期定时器；周期定时器自动续期

    int epfd_;                          // epoll 实例 fd
    static const int kMaxEvents = 64;   // 每次 epoll_wait 最多取 64 个就绪事件
    std::atomic<bool> running_;         // 循环运行标志
    std::map<int, FdCallback> fd_cbs_;  // fd -> 就绪回调
    std::map<uint64_t, Timer> timers_;  // id -> Timer
    uint64_t next_timer_id_;            // 定时器 id 分配器
};
