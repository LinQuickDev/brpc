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

int TimerInit() {
    return 0;
}

void TimerModuleDestroy() {
    std::vector<std::shared_ptr<TimerContext> > contexts;
    contexts.reserve(g_timer_ctx_map.size());

    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        for (auto &pair: g_timer_ctx_map) {
            pair.second->periodical = 0;
            bthread_timer_del(pair.second->timer_id);
            contexts.push_back(pair.second);
        }
        g_timer_ctx_map.clear();
        g_total_timer_num.store(0);
    }

    for (auto &ctx: contexts) {
        ctx->self_ref.reset();
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
    ctx->periodical = (time->it_interval.tv_sec > 0 || time->it_interval.tv_nsec > 0) ? 1 : 0;
    ctx->interval = time->it_interval;
    ctx->self_ref = ctx;

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
    int ret = 0;
    {
        std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
        auto it = g_timer_ctx_map.find(timer_id);
        if (it == g_timer_ctx_map.end()) {
            return;
        }
        auto ctx = it->second;
        ctx->periodical = 0;
        ret = bthread_timer_del(ctx->timer_id);
        if (ret == 0) {
            g_timer_ctx_map.erase(it);
            ctx->self_ref.reset();
        }
    }
    if (ret == 0) {
        --g_total_timer_num;
    }
}

void DeleteTimer(uint64_t timer_id) {
    std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
    auto it = g_timer_ctx_map.find(timer_id);
    if (it == g_timer_ctx_map.end()) {
        LOG(WARNING) << "Timer id=" << timer_id << " not found";
        return;
    }
    it->second->periodical = 0;
}

void TimerCallbackWrapper(void *arg) {
    auto timer_id = reinterpret_cast<uint64_t>(arg);
    auto ctx = find_context(timer_id);
    if (ctx == nullptr) {
        LOG(ERROR) << "timer_id is not found, timer_id=" << timer_id;
        return;
    }
    if (ctx->cb == nullptr) {
        LOG(ERROR) << "Timer callback is nullptr";
        return;
    }

    void *cb_args = ctx->args;
    timespec interval = ctx->interval;

    if (ctx->periodical == 0) {
        {
            std::lock_guard<std::mutex> lock(g_timer_ctx_mutex);
            g_timer_ctx_map.erase(timer_id);
            --g_total_timer_num;
        }
        ctx->self_ref.reset();
    }

    ctx->cb(cb_args);

    if (ctx->periodical == 1) {
        timespec abstime = add_timespec(get_current_realtime(), interval);
        bthread_timer_add(&ctx->timer_id, abstime, TimerCallbackWrapper, reinterpret_cast<void *>(timer_id));
    }
}
} // namespace ubring
} // namespace brpc
