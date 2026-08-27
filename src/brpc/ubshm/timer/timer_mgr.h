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

#ifndef BRPC_TIMER_MGR_H
#define BRPC_TIMER_MGR_H
#include <pthread.h>
#include <ctime>
#include <atomic>
#include <unordered_map>
#include <memory>
#include <mutex>
#include "bthread/types.h"
#include "bthread/unstable.h"
#include "brpc/ubshm/common/common.h"

#if defined(OS_MACOSX)
struct itimerspec
{
    struct timespec it_interval;
    struct timespec it_value;
};
#endif

namespace brpc {
namespace ubring {

constexpr long long NS_PER_SEC = 1000000000LL;

typedef void * (*TimerCallback)(void *);

struct TimerContext{
    TimerCallback cb;
    void *args;
    uint32_t periodical;
    timespec interval;
    bthread_timer_t timer_id;
    std::shared_ptr<TimerContext> self_ref;
};

extern std::unordered_map<uint64_t, std::shared_ptr<TimerContext>> g_timer_ctx_map;
extern std::mutex g_timer_ctx_mutex;
extern std::atomic<uint64_t> g_total_timer_num;


int TimerInit(void);
void TimerModuleDestroy(void);
int32_t TimerStart(const itimerspec *time, TimerCallback cb, void *args);
uint32_t GetActiveTimerNum(void);

void DeleteTimerSafe(uint64_t timer_id);
void DeleteTimer(uint64_t timer_id);

void TimerCallbackWrapper(void *arg);
}
}
#endif //BRPC_TIMER_MGR_H