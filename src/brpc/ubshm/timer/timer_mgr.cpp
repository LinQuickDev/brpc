// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#define _GNU_SOURCE

#include <pthread.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <mutex>
#include "brpc/ubshm/timer/timer_mgr.h"

namespace brpc {
namespace ubring {

std::unordered_map<uint32_t, TimerContext> g_timer_ctx_map;
std::mutex g_timer_ctx_mutex;
std::atomic<uint32_t> g_total_timer_num;

static std::atomic<uint32_t> g_timer_id_counter(0);

static void normalize_timespec(itimerspec *spec) {
    if (spec->it_interval.tv_nsec >= 1000000000L) {
        spec->it_interval.tv_sec += spec->it_interval.tv_nsec / 1000000000L;
        spec->it_interval.tv_nsec %= 1000000000L;
    }
    if (spec->it_value.tv_nsec >= 1000000000L) {
        spec->it_value.tv_sec += spec->it_value.tv_nsec / 1000000000L;
        spec->it_value.tv_nsec %= 1000000000L;
    }
}


int TimerInit() {
    return 0;
}

void TimerModuleDestroy() {
    std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
    for (auto & pair: g_timer_ctx_map) {
        bthread_timer_del(pair.second.timer_id);
        pthread_spin_destroy(&pair.second.spin_lock);
    }
    g_timer_ctx_map.clear();
    g_total_timer_num.store(0);
}

int32_t TimerStart(const itimerspec *time, TimerCallback cb, void *args) {
    if (cb==nullptr) {
        // LOG(ERROR) << "Timer callback is nullptr";
        return -1;
    }

    TimerContext ctx{};
    ctx.cb = cb;
    ctx.args = args;
    ctx.periodical = (time->it_interval.tv_sec > 0 || time->it_interval.tv_nsec > 0) ? 1 : 0;
    ctx.interval = time->it_interval;
    pthread_spin_init(&ctx.spin_lock, PTHREAD_PROCESS_PRIVATE);

    uint64_t timer_id = g_timer_id_counter.fetch_add(1);

    itimerspec normalized_time = *time;
    normalize_timespec(&normalized_time);
    timespec abstime = normalized_time.it_value;

    int ret = bthread_timer_add(&ctx.timer_id, abstime, TimerCallbackWrapper, reinterpret_cast<void *>(timer_id));
    if (ret != 0) {
        // LOG(ERROR) << "Failed to add bthread timer, ret=" << ret;
        pthread_spin_destroy(&ctx.spin_lock);
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        g_timer_ctx_map[timer_id] = ctx;
    }

    std::atomic_fetch_add(&g_total_timer_num, 1U);
    return static_cast<int32_t>(timer_id);
}

uint32_t GetActiveTimerNum() {
    return std::atomic_load(&g_total_timer_num);
}

void DeleteTimerSafe(uint64_t timer_id) {
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        auto it = g_timer_ctx_map.find(timer_id);
        if (it == g_timer_ctx_map.end()) {
            return;
        }

        bthread_timer_del(it->second.timer_id);

        pthread_spin_destroy(&it->second.spin_lock);
        g_timer_ctx_map.erase(it);
    }
    std::atomic_fetch_sub(&g_total_timer_num, 1U);
}

void DeleteTimer(uint64_t timer_id) {
    std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
    auto it = g_timer_ctx_map.find(timer_id);
    if (it == g_timer_ctx_map.end()) {
        LOG(WARNING) << "Timer id=" << timer_id << " not found";
        return;
    }
    it->second.periodical = 0;
}

void TimerCallbackWrapper(void *arg) {
    auto timer_id = reinterpret_cast<uint64_t>(arg);

    TimerContext *ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        auto it = g_timer_ctx_map.find(timer_id);
        if (it==g_timer_ctx_map.end()) {
            return;
        }
        ctx = &it->second;
    }

    if (ctx == nullptr || ctx->cb == nullptr) {
        return;
    }

    void *cb_args = ctx->args;
    auto is_periodical = ctx->periodical;
    timespec interval = ctx->interval;

    if (!is_periodical) {
        {
            std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
            auto it = g_timer_ctx_map.find(timer_id);
            if (it!=g_timer_ctx_map.end()) {
                pthread_spin_destroy(&it->second.spin_lock);
                g_timer_ctx_map.erase(it);
            }
        }
        std::atomic_fetch_sub(&g_total_timer_num, 1U);
    }

    ctx->cb(cb_args);

    if (is_periodical) {
        auto now = std::chrono::steady_clock::now();
        auto interval_duration = std::chrono::seconds(interval.tv_sec) + std::chrono::nanoseconds(interval.tv_nsec);
        auto future_time = now + interval_duration;

        auto future_time_point = std::chrono::time_point_cast<std::chrono::nanoseconds>(future_time);
        timespec abstime{};
        abstime.tv_sec=std::chrono::duration_cast<std::chrono::seconds>(future_time_point.time_since_epoch()).count();
        abstime.tv_nsec = future_time_point.time_since_epoch().count() % 1000000000;

        bthread_timer_add(&ctx->timer_id, abstime, TimerCallbackWrapper, reinterpret_cast<void *>(timer_id));
    }
}




}  // namespace ubring
}  // namespace brpc