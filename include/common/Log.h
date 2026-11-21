#pragma once
// Log.h - 轻量级日志模块（级别 + 时间戳 + 输出到 stderr）

#include <cstdarg>
#include <cstdio>
#include <ctime>

enum LogLevel {
    LOG_TRACE = 0,
    LOG_DEBUG = 1,
    LOG_INFO  = 2,
    LOG_WARN  = 3,
    LOG_ERROR = 4,
};

// 编译期可控制日志级别，默认 INFO
#ifndef KVRAFT_LOG_LEVEL
#define KVRAFT_LOG_LEVEL LOG_INFO
#endif

inline void kvraft_log_raw(LogLevel level, const char* fmt, ...) {
    static const char* level_names[] = {"TRACE", "DEBUG", "INFO", "WARN", "ERROR"};
    if (level < KVRAFT_LOG_LEVEL) return;

    char buf[64];
    time_t t = time(nullptr);
    struct tm tmv;
    localtime_r(&t, &tmv);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tmv);

    fprintf(stderr, "[%s] [%s] ", buf, level_names[level]);
    va_list args;
    va_start(args, fmt);
    vfprintf(stderr, fmt, args);
    va_end(args);
    fprintf(stderr, "\n");
    fflush(stderr);
}

#define LOG_TRACE(fmt, ...) kvraft_log_raw(LOG_TRACE, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...) kvraft_log_raw(LOG_DEBUG, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)  kvraft_log_raw(LOG_INFO,  fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)  kvraft_log_raw(LOG_WARN,  fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) kvraft_log_raw(LOG_ERROR, fmt, ##__VA_ARGS__)
