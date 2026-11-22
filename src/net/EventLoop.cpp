// ============================================================================
// EventLoop.cpp - epoll 事件循环实现
// ============================================================================

#include "net/EventLoop.h"

#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>

#include "common/Log.h"

// 单调时钟毫秒值：steady_clock 不受系统时间跳变影响
uint64_t EventLoop::now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               steady_clock::now().time_since_epoch())
        .count();
}

EventLoop::EventLoop() : running_(false), next_timer_id_(1) {
    epfd_ = ::epoll_create1(0);
    if (epfd_ < 0) {
        LOG_ERROR("epoll_create1 failed: %s", strerror(errno));
    }
}

EventLoop::~EventLoop() {
    if (epfd_ >= 0) ::close(epfd_);
}

// 注册 fd：epoll_ctl ADD + 记录回调
// events 一般为 EPOLLIN | EPOLLRDHUP（LT 模式，读不完会再次触发）
void EventLoop::add_fd(int fd, uint32_t events, FdCallback cb) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd; // 只存 fd，回调从自己的 map 里查
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        LOG_ERROR("epoll ADD fd=%d failed: %s", fd, strerror(errno));
        return;
    }
    fd_cbs_[fd] = std::move(cb);
}

// 修改监听事件（发送缓冲堆积时加 EPOLLOUT，清空后去掉）
void EventLoop::mod_fd(int fd, uint32_t events) {
    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) < 0) {
        LOG_ERROR("epoll MOD fd=%d failed: %s", fd, strerror(errno));
    }
}

// 注销：DEL + close（close 后 fd 自动从 epoll 失效）
void EventLoop::remove_fd(int fd) {
    ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    ::close(fd);
    fd_cbs_.erase(fd);
}

// deadline = now + delay；interval > 0 表示周期任务
uint64_t EventLoop::run_after(uint64_t delay_ms, TimerCallback cb) {
    Timer t;
    t.id = next_timer_id_++;
    t.deadline = now_ms() + delay_ms;
    t.interval = 0;
    t.cb = std::move(cb);
    timers_[t.id] = std::move(t);
    return t.id;
}

uint64_t EventLoop::run_every(uint64_t interval_ms, TimerCallback cb) {
    Timer t;
    t.id = next_timer_id_++;
    t.deadline = now_ms() + interval_ms;
    t.interval = interval_ms;
    t.cb = std::move(cb);
    timers_[t.id] = std::move(t);
    return t.id;
}

void EventLoop::cancel_timer(uint64_t id) {
    // 直接从 map 里摘掉即可：run() 里执行前会重新收集到期定时器快照，
    // 所以"定时器回调里取消另一个定时器"是安全的
    timers_.erase(id);
}

// ==================================================================
// fire_expired_timers - 执行所有到期定时器
//
// 关键点：先收集快照、再逐个执行。
//   定时器回调（比如 Raft 的选举超时）内部很可能再次修改 timers_
//   （取消自己、重设新选举超时、启动心跳定时器……），
//   如果边遍历边执行，迭代器会失效 -> 崩溃。
//   所以先把到期的 Timer 拷贝出来，再逐个调用回调。
//
// 周期定时器续期：deadline += interval，直到超过 now
//   （如果回调执行耗时超过 interval，会自动"追帧"，不会越欠越多）
// ==================================================================
void EventLoop::fire_expired_timers() {
    uint64_t now = now_ms();

    // 1) 收集到期定时器的快照（拷贝出来，回调里改 timers_ 不影响遍历）
    std::vector<Timer> due;
    for (auto& kv : timers_) {
        if (kv.second.deadline <= now) {
            due.push_back(kv.second);
        }
    }

    // 2) 逐个执行
    for (auto& t : due) {
        // 单次定时器：先从 map 移除（回调可能已经取消过它，erase 无害）
        if (t.interval == 0) {
            timers_.erase(t.id);
        } else {
            // 周期定时器：续期。用 while 是为了处理"一次欠了多帧"的情况
            auto it = timers_.find(t.id);
            if (it == timers_.end()) continue; // 回调已把它取消
            while (it->second.deadline <= now) {
                it->second.deadline += it->second.interval;
            }
        }
        t.cb(); // 执行回调（可能在里面取消其他定时器/注册新 fd，均安全）
    }
}

// ==================================================================
// run - 事件循环主函数（整个网络层的"心脏"）
//
// 每轮做两件事：
//   1. epoll_wait 等待 IO 事件（fd 可读/可写/对端关闭）
//   2. 检查并执行到期定时器
//
// timeout 的取值 = min(最近一个定时器的 deadline, 1s)：
//   - 有定时器且快到期 -> 提前醒来执行定时器（保证 Raft 选举/心跳准时）
//   - 没有定时器 -> 最多睡 1 秒（保证 stop() 请求最多 1 秒内被响应）
// ==================================================================
void EventLoop::run() {
    running_ = true;
    std::vector<struct epoll_event> events(kMaxEvents);

    while (running_) {
        // ---- 计算本轮 epoll_wait 超时 ----
        int timeout_ms = 1000; // 默认睡 1 秒
        if (!timers_.empty()) {
            uint64_t now = now_ms();
            uint64_t earliest = timers_.begin()->second.deadline; // map 按 deadline? 不，按 id 排
            // map 按 id 排序，找最近 deadline 需要遍历（定时器数量极少，O(n) 可忽略）
            for (auto& kv : timers_) {
                if (kv.second.deadline < earliest) earliest = kv.second.deadline;
            }
            if (earliest <= now) {
                timeout_ms = 0; // 已有到期定时器，不睡，立即处理
            } else {
                timeout_ms = static_cast<int>(earliest - now);
                if (timeout_ms > 1000) timeout_ms = 1000;
            }
        }

        // ---- 等待 IO 事件 ----
        int n = ::epoll_wait(epfd_, events.data(), kMaxEvents, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) continue; // 被信号打断（gdb 调试常见），重试即可
            LOG_ERROR("epoll_wait failed: %s", strerror(errno));
            break;
        }

        // ---- 分发 IO 事件 ----
        // 注意：回调内部可能 add_fd/remove_fd 修改 fd_cbs_，
        // 所以先取出回调再执行（和定时器同样的快照思路）
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            auto it = fd_cbs_.find(fd);
            if (it != fd_cbs_.end()) {
                it->second(events[i].events); // 执行该 fd 的就绪回调
            }
        }

        // ---- 处理到期定时器 ----
        fire_expired_timers();
    }
}

void EventLoop::stop() {
    // atomic store：可从信号处理器安全调用（进程收到 Ctrl+C 时优雅退出）
    running_ = false;
}
