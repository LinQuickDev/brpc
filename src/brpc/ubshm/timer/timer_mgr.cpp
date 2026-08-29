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

#include <atomic>
#include <new>
#include "bthread/bthread.h"                     // bthread_usleep
#include "bthread/unstable.h"                    // bthread_timer_add/del
#include "butil/time.h"
#include "brpc/ubshm/timer/timer_mgr.h"

namespace brpc {
namespace ubring {

namespace {

// Sentinel occupying a slot while its task is being scheduled; never
// dereferenced, deleters just clear it.
const UbrTimerId kReservedSlot = (UbrTimerId)((uintptr_t)1);

}  // namespace

// Reference rules: one "owner" ref for the handle slot plus one "schedule"
// ref per pending/running bthread schedule. The schedule ref is consumed
// by the firing callback or by the deleter whose bthread_timer_del
// returned 0 (cancelled before run); the owner ref is consumed by whoever
// takes the task out of *slot -- a deleter, or the one-shot firing
// callback itself. All atomics are seq_cst so no interleaving can release
// a ref twice or free the task while a callback still runs.
struct UbrTimerTask {
    UbrTimerId* slot;
    std::atomic<bthread_timer_t> id;
    void* (*cb)(void*);
    void* arg;
    UbrTimerBackoffFn backoff;
    uint64_t interval_us;                        // timer thread only
    bool periodic;
    std::atomic<bool> stopped;
    std::atomic<int> ref;
    std::atomic<bool> join_pending;              // a DelAndWait is waiting
    std::atomic<bool> done;                      // refs hit zero, joiner frees
};

namespace {

void ReleaseRef(UbrTimerTask* task) {
    if (task->ref.fetch_sub(1) == 1) {
        if (task->join_pending.load()) {
            task->done.store(true);              // joiner frees the task
        } else {
            delete task;
        }
    }
}

void UbrTimerOnFire(void* p) {
    UbrTimerTask* task = (UbrTimerTask*)p;
    if (!task->stopped.load()) {
        task->cb(task->arg);
    }

    if (task->periodic) {
        // Claim the next schedule's ref before re-reading `stopped' so a
        // racing delete can neither free the task nor orphan a re-arm.
        task->ref.fetch_add(1);
        if (task->stopped.load()) {
            ReleaseRef(task);
        } else {
            uint64_t interval = task->interval_us;
            if (task->backoff != nullptr) {
                interval = task->backoff(task->arg, interval);
                task->interval_us = interval;
            }
            bthread_timer_t id = 0;
            if (bthread_timer_add(
                    &id, butil::microseconds_from_now((int64_t)interval),
                    UbrTimerOnFire, task) == 0) {
                task->id.store(id);
                if (task->stopped.load() && bthread_timer_del(id) == 0) {
                    ReleaseRef(task);
                }
            } else {
                LOG(ERROR) << "Fail to re-arm ubring timer";
                ReleaseRef(task);
            }
        }
        ReleaseRef(task);
        return;
    }

    // One-shot: release the schedule ref plus the owner ref if the slot
    // still holds this task (CAS so a reused slot is left untouched).
    UbrTimerId expected = task;
    const bool owned =
        __atomic_compare_exchange_n(task->slot, &expected, (UbrTimerId) nullptr,
                                    false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    ReleaseRef(task);
    if (owned) {
        ReleaseRef(task);
    }
}

UbrTimerTask* TakeOutTask(UbrTimerId* slot) {
    UbrTimerTask* task =
        __atomic_exchange_n(slot, (UbrTimerId) nullptr, __ATOMIC_SEQ_CST);
    return task == kReservedSlot ? nullptr : task;
}

// Give a reserved slot back unless a deleter already cleared it.
void ReleaseReservation(UbrTimerId* slot) {
    UbrTimerId expected = kReservedSlot;
    __atomic_compare_exchange_n(slot, &expected, (UbrTimerId) nullptr, false,
                                __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
}

RETURN_CODE TimerStartInternal(UbrTimerId* slot, uint64_t delay_us,
                               uint64_t interval_us, void* (*cb)(void*),
                               void* arg, UbrTimerBackoffFn backoff,
                               bool once) {
    if (UNLIKELY(slot == nullptr || cb == nullptr)) {
        LOG(ERROR) << "Ubr timer start invalid argument, slot=" << slot;
        return UBRING_ERR;
    }

    // Reserve the slot so a concurrent start cannot schedule twice, and
    // deleters see no half-built task: they just clear the reservation and
    // this starter gives up.
    UbrTimerId expected = nullptr;
    if (!__atomic_compare_exchange_n(slot, &expected, kReservedSlot, false,
                                     __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        return once ? UBRING_REENTRY : UBRING_ERR;
    }

    UbrTimerTask* task = new (std::nothrow) UbrTimerTask();
    if (UNLIKELY(task == nullptr)) {
        LOG(ERROR) << "Fail to malloc ubring timer task.";
        ReleaseReservation(slot);
        return UBRING_ERR;
    }
    task->slot = slot;
    task->id.store(0);
    task->cb = cb;
    task->arg = arg;
    task->backoff = backoff;
    task->interval_us = interval_us;
    task->periodic = (interval_us > 0);
    task->stopped.store(false);
    task->ref.store(2);
    task->join_pending.store(false);
    task->done.store(false);

    bthread_timer_t id = 0;
    if (UNLIKELY(bthread_timer_add(
            &id, butil::microseconds_from_now((int64_t)delay_us),
            UbrTimerOnFire, task) != 0)) {
        LOG(ERROR) << "Fail to add ubring timer";
        ReleaseReservation(slot);
        ReleaseRef(task);
        ReleaseRef(task);
        return UBRING_ERR;
    }
    task->id.store(id);

    // A deleter may have cleared the reservation meanwhile; cancel the
    // fresh task instead of publishing it.
    expected = kReservedSlot;
    if (!__atomic_compare_exchange_n(slot, &expected, (UbrTimerId) task, false,
                                     __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        task->stopped.store(true);
        if (bthread_timer_del(id) == 0) {
            ReleaseRef(task);
        }
        ReleaseRef(task);
        return UBRING_ERR;
    }
    if (task->stopped.load() && bthread_timer_del(id) == 0) {
        ReleaseRef(task);
    }
    return UBRING_OK;
}

}  // namespace

RETURN_CODE UbrTimerStart(UbrTimerId* slot, uint64_t delay_us,
                          uint64_t interval_us, void* (*cb)(void*),
                          void* arg, UbrTimerBackoffFn backoff) {
    return TimerStartInternal(slot, delay_us, interval_us, cb, arg, backoff,
                              false);
}

RETURN_CODE UbrTimerStartOnce(UbrTimerId* slot, uint64_t delay_us,
                              uint64_t interval_us, void* (*cb)(void*),
                              void* arg, UbrTimerBackoffFn backoff) {
    return TimerStartInternal(slot, delay_us, interval_us, cb, arg, backoff,
                              true);
}

void UbrTimerDel(UbrTimerId* slot) {
    if (slot == nullptr) {
        return;
    }
    UbrTimerTask* task = TakeOutTask(slot);
    if (task == nullptr) {
        return;
    }
    task->stopped.store(true);
    bthread_timer_t id = task->id.load();
    if (id != 0 && bthread_timer_del(id) == 0) {
        ReleaseRef(task);
    }
    ReleaseRef(task);
}

void UbrTimerDelAndWait(UbrTimerId* slot) {
    if (slot == nullptr) {
        return;
    }
    UbrTimerTask* task = TakeOutTask(slot);
    if (task == nullptr) {
        return;
    }
    task->join_pending.store(true);
    task->stopped.store(true);
    bthread_timer_t id = task->id.load();
    if (id != 0 && bthread_timer_del(id) == 0) {
        ReleaseRef(task);
    }
    ReleaseRef(task);
    while (!task->done.load()) {
        bthread_usleep(1000);
    }
    task->join_pending.store(false);
    delete task;
}

}  // namespace ubring
}  // namespace brpc
