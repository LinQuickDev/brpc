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

std::atomic<uint32_t> g_total_timer_num(0);

// Sentinel occupying a slot while its task is being constructed and
// scheduled. Never dereferenced: deleters just clear it, and the starter
// gives up when it finds the reservation gone.
const UbrTimerId kReservedSlot = (UbrTimerId)((uintptr_t)1);

}  // namespace

// Ownership rules (all atomics are seq_cst so no interleaving can break
// them):
// - "schedule" reference: one per pending/running bthread schedule. It is
//   consumed exactly once -- by the firing callback, or by the deleter
//   whose bthread_timer_del returned 0 (cancelled before run; guaranteed
//   never to run by TimerThread's version CAS).
// - "owner" reference: stands for the handle slot. It is consumed exactly
//   once by whoever takes the task out of *slot: a deleter, or -- for
//   one-shot timers -- the firing callback itself (auto-consume, mirroring
//   the old UnifiedCallback behavior of deleting non-periodic timers after
//   their run). While a slot only holds the reservation sentinel, the task
//   is unreachable to deleters and the starter owns both references.
// - The handle slot keeps the task object alive; it does NOT keep `arg'
//   alive. Freeing resources reachable from `arg' therefore requires
//   UbrTimerDelAndWait, which waits until every schedule reference is gone
//   (and with it, any running callback).
//
// The re-arm of a periodic timer claims its next schedule reference BEFORE
// re-reading `stopped', and deleters store `stopped' before cancelling, so
// either the re-arm gives the claim up or somebody successfully cancels the
// freshly scheduled timer and consumes the claim: the task cannot be freed
// while a schedule or a running callback still refers to it (no UAF).
struct UbrTimerTask {
    UbrTimerId* slot;                            // where this task is published
    std::atomic<bthread_timer_t> id;
    void* (*cb)(void*);
    void* arg;
    UbrTimerBackoffFn backoff;
    uint64_t interval_us;                        // timer thread only
    bool periodic;
    std::atomic<bool> stopped;
    std::atomic<int> ref;                        // owner + schedule refs
    std::atomic<bool> join_pending;              // a DelAndWait is waiting
    std::atomic<bool> done;                      // refs hit zero, joiner frees
};

namespace {

void ReleaseRef(UbrTimerTask* task) {
    if (task->ref.fetch_sub(1) == 1) {
        g_total_timer_num.fetch_sub(1);
        if (task->join_pending.load()) {
            task->done.store(true);              // the joiner frees the task
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
        // Claim the next schedule's reference BEFORE re-reading `stopped':
        // a racing delete either sees our claim (it will not free the
        // task) or we see its stop flag (we will not re-arm).
        task->ref.fetch_add(1);
        if (task->stopped.load()) {
            ReleaseRef(task);                    // give up re-arming
        } else {
            uint64_t interval = task->interval_us;
            if (task->backoff != nullptr) {
                interval = task->backoff(task->arg, interval);
                task->interval_us = interval;
            }
            bthread_timer_t id = 0;
            int rc = bthread_timer_add(
                &id, butil::microseconds_from_now((int64_t)interval),
                UbrTimerOnFire, task);
            if (rc == 0) {
                task->id.store(id);
                if (task->stopped.load() && bthread_timer_del(id) == 0) {
                    ReleaseRef(task);            // cancelled what we scheduled
                }
            } else {
                LOG(ERROR) << "Fail to re-arm ubring timer, rc=" << rc;
                ReleaseRef(task);                // stop the periodic chain
            }
        }
        ReleaseRef(task);                        // consume current schedule
        return;
    }

    // One-shot: consume the schedule reference, and the owner reference as
    // well if this fire still owns the slot (nobody deleted the timer).
    // Exactly one of this CAS and a racing deleter's exchange succeeds, so
    // the owner reference is released exactly once. CAS (not exchange) so
    // that a slot already reused by another task is left untouched.
    UbrTimerId expected = task;
    const bool owned =
        __atomic_compare_exchange_n(task->slot, &expected, (UbrTimerId) nullptr,
                                    false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    ReleaseRef(task);                            // consume current schedule
    if (owned) {
        ReleaseRef(task);                        // consume owner reference
    }
}

// Take the task out of *slot. Returns nullptr when the slot is empty.
// A reservation sentinel is never returned: the caller treats it as
// "start still in flight, nothing to stop or wait for".
UbrTimerTask* TakeOutTask(UbrTimerId* slot) {
    UbrTimerTask* task =
        __atomic_exchange_n(slot, (UbrTimerId) nullptr, __ATOMIC_SEQ_CST);
    if (task == kReservedSlot) {
        // The starter will observe the cleared reservation, cancel the
        // fresh task and release it by itself.
        return nullptr;
    }
    return task;
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
        LOG(ERROR) << "UbrTimerStart invalid argument, slot=" << slot;
        return UBRING_ERR;
    }

    // Reserve the slot atomically (nullptr -> reserved) so that a
    // concurrent UbrTimerStartOnce on the same slot cannot schedule twice.
    // The task is only published once it is fully scheduled; until then
    // deleters just clear the reservation and the starter gives up.
    UbrTimerId expected = nullptr;
    if (!__atomic_compare_exchange_n(slot, &expected, kReservedSlot, false,
                                     __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        // Slot occupied or reserved by a concurrent start.
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
    task->ref.store(2);                          // owner + first schedule
    task->join_pending.store(false);
    task->done.store(false);
    g_total_timer_num.fetch_add(1);

    bthread_timer_t id = 0;
    int rc = bthread_timer_add(
        &id, butil::microseconds_from_now((int64_t)delay_us),
        UbrTimerOnFire, task);
    if (UNLIKELY(rc != 0)) {
        LOG(ERROR) << "Fail to add ubring timer, rc=" << rc;
        ReleaseReservation(slot);                // no-op if a deleter cleared it
        ReleaseRef(task);                        // owner (never published)
        ReleaseRef(task);                        // schedule, never ran
        return UBRING_ERR;
    }
    task->id.store(id);

    // Publish. If a deleter cleared the reservation meanwhile, cancel the
    // fresh task instead: the deleter already conceptually owns it.
    expected = kReservedSlot;
    if (!__atomic_compare_exchange_n(slot, &expected, (UbrTimerId) task, false,
                                     __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        task->stopped.store(true);
        if (bthread_timer_del(id) == 0) {
            ReleaseRef(task);                    // schedule never runs
        }
        ReleaseRef(task);                        // owner (never published)
        return UBRING_ERR;
    }
    if (task->stopped.load() && bthread_timer_del(id) == 0) {
        ReleaseRef(task);                        // deleted before it could run
    }
    return UBRING_OK;
}

}  // namespace

void UbrTimerStart(UbrTimerId* slot, uint64_t delay_us, uint64_t interval_us,
                   void* (*cb)(void*), void* arg, UbrTimerBackoffFn backoff) {
    TimerStartInternal(slot, delay_us, interval_us, cb, arg, backoff, false);
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
    // Never wait for a running callback: self-delete from inside the
    // callback lands here (bthread_timer_del reports 1/EINVAL) and the
    // schedule reference is consumed when the callback returns.
    bthread_timer_t id = task->id.load();
    if (id != 0 && bthread_timer_del(id) == 0) {
        ReleaseRef(task);                        // cancelled before run
    }
    ReleaseRef(task);                            // owner reference
}

void UbrTimerDelAndWait(UbrTimerId* slot) {
    if (slot == nullptr) {
        return;
    }
    UbrTimerTask* task = TakeOutTask(slot);
    if (task == nullptr) {
        return;
    }

    // Register as joiner while the owner reference still keeps the task
    // alive, so the last schedule release hands the deletion over to us
    // instead of freeing the task we are about to inspect.
    task->join_pending.store(true);
    task->stopped.store(true);
    bthread_timer_t id = task->id.load();
    if (id != 0 && bthread_timer_del(id) == 0) {
        ReleaseRef(task);                        // cancelled before run
    }
    ReleaseRef(task);                            // owner reference

    // The exchange above makes this the only joiner of the task, so once
    // `done' is observed no one else touches it and we free it.
    while (!task->done.load()) {
        bthread_usleep(1000);
    }
    task->join_pending.store(false);
    delete task;
}

uint32_t GetActiveTimerNum(void) {
    return g_total_timer_num.load();
}

}  // namespace ubring
}  // namespace brpc
