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
#include <chrono>
#include <mutex>
#include <vector>
#include "bthread/bthread.h"
#include "brpc/ubshm/timer/timer_mgr.h"

namespace brpc {
namespace ubring {
std::unordered_map<uint64_t, std::shared_ptr<TimerContext> > g_timer_ctx_map;
std::mutex g_timer_ctx_mutex;
std::atomic<uint64_t> g_total_timer_num;

static std::atomic<uint64_t> g_timer_id_counter(1);

static timespec get_current_realtime() {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts;
}

static timespec add_timespec(const timespec &base, const timespec &offset) {
    timespec result{};
    result.tv_sec = base.tv_sec + offset.tv_sec;
    result.tv_nsec = base.tv_nsec + offset.tv_nsec;
    if (result.tv_nsec >= NS_PER_SEC) {
        result.tv_sec += result.tv_nsec / NS_PER_SEC;
        result.tv_nsec %= NS_PER_SEC;
    }
    return result;
}

static std::shared_ptr<TimerContext> find_context(uint64_t timer_id) {
    std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
    auto it = g_timer_ctx_map.find(timer_id);
    if (it == g_timer_ctx_map.end()) {
        return nullptr;
    }
    return it->second;
}

static void remove_timer_from_map(uint64_t timer_id) {
    std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
    auto it = g_timer_ctx_map.find(timer_id);
    if (it == g_timer_ctx_map.end()) {
        return;
    }
    g_timer_ctx_map.erase(it);
    --g_total_timer_num;
}

struct TimerCallbackArgs {
    std::shared_ptr<TimerContext> ctx;
    uint64_t timer_id;
};

static void RunTimerCallback(std::shared_ptr<TimerContext> ctx, uint64_t timer_id) {
    if (ctx->cb != nullptr) {
        ctx->cb(ctx->args);
    }

    bool need_remove = true;
    {
        std::lock_guard<std::mutex> lock(ctx->mtx);
        if (ctx->periodical && !ctx->stopped && !ctx->no_reschedule) {
            timespec abstime = add_timespec(get_current_realtime(), ctx->interval);
            if (bthread_timer_add(&ctx->timer_id, abstime, TimerCallbackWrapper,
                                  reinterpret_cast<void *>(timer_id)) == 0) {
                need_remove = false;
            }
        }
    }

    if (need_remove) {
        remove_timer_from_map(timer_id);
    }

    {
        std::lock_guard<std::mutex> lock(ctx->mtx);
        --ctx->running;
    }
    ctx->cv.notify_all();
}

static void *TimerCallbackWorker(void *arg) {
    std::unique_ptr<TimerCallbackArgs> holder(static_cast<TimerCallbackArgs *>(arg));
    RunTimerCallback(holder->ctx, holder->timer_id);
    return nullptr;
}

int TimerInit() {
    return 0;
}

void TimerModuleDestroy() {
    std::vector<std::shared_ptr<TimerContext> > contexts;
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        contexts.reserve(g_timer_ctx_map.size());
        for (auto &pair: g_timer_ctx_map) {
            contexts.push_back(pair.second);
        }
        g_timer_ctx_map.clear();
        g_total_timer_num.store(0);
    }

    for (auto &ctx: contexts) {
        {
            std::lock_guard<std::mutex> lock(ctx->mtx);
            ctx->stopped = true;
            bthread_timer_del(ctx->timer_id);
        }
        std::unique_lock<std::mutex> lock(ctx->mtx);
        ctx->cv.wait(lock, [&ctx] { return ctx->running == 0; });
    }
}

int32_t TimerStart(const itimerspec *time, TimerCallback cb, void *args) {
    if (cb == nullptr) {
        LOG(ERROR) << "Timer callback is nullptr";
        return -1;
    }

    auto ctx = std::make_shared<TimerContext>();
    ctx->cb = cb;
    ctx->args = args;
    ctx->periodical = (time->it_interval.tv_sec > 0 || time->it_interval.tv_nsec > 0);
    ctx->interval = time->it_interval;

    uint64_t timer_id = g_timer_id_counter.fetch_add(1);

    timespec abstime = add_timespec(get_current_realtime(), time->it_value);

    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        g_timer_ctx_map[timer_id] = ctx;
        ++g_total_timer_num;
        int ret = bthread_timer_add(&ctx->timer_id, abstime, TimerCallbackWrapper, reinterpret_cast<void *>(timer_id));
        if (ret != 0) {
            LOG(ERROR) << "Failed to add bthread timer, ret=" << ret;
            g_timer_ctx_map.erase(timer_id);
            --g_total_timer_num;
            return -1;
        }
    }

    return static_cast<int32_t>(timer_id);
}

uint32_t GetActiveTimerNum() {
    return g_total_timer_num.load();
}

void DeleteTimerSafe(uint64_t timer_id) {
    std::shared_ptr<TimerContext> ctx;
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        auto it = g_timer_ctx_map.find(timer_id);
        if (it == g_timer_ctx_map.end()) {
            return;
        }
        ctx = it->second;
    }

    {
        std::lock_guard<std::mutex> lock(ctx->mtx);
        ctx->stopped = true;
        bthread_timer_del(ctx->timer_id);
    }
    std::unique_lock<std::mutex> lock(ctx->mtx);
    ctx->cv.wait(lock, [&ctx] { return ctx->running == 0; });

    remove_timer_from_map(timer_id);
}

void DeleteTimer(uint64_t timer_id) {
    std::shared_ptr<TimerContext> ctx;
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        auto it = g_timer_ctx_map.find(timer_id);
        if (it == g_timer_ctx_map.end()) {
            LOG(WARNING) << "Timer id=" << timer_id << " not found";
            return;
        }
        ctx = it->second;
    }
    std::lock_guard<std::mutex> lock(ctx->mtx);
    ctx->no_reschedule = true;
}

void TimerCallbackWrapper(void *arg) {
    auto timer_id = reinterpret_cast<uint64_t>(arg);
    auto ctx = find_context(timer_id);
    if (ctx == nullptr) {
        LOG(ERROR) << "timer_id is not found, timer_id=" << timer_id;
        return;
    }

    {
        std::lock_guard<std::mutex> lock(ctx->mtx);
        if (ctx->stopped) {
            return;
        }
        ++ctx->running;
    }

    auto *holder = new TimerCallbackArgs{ctx, timer_id};
    bthread_t tid;
    if (bthread_start_background(&tid, nullptr, TimerCallbackWorker, holder) != 0) {
        delete holder;
        RunTimerCallback(ctx, timer_id);
    }
}
} // namespace ubring
} // namespace brpc
