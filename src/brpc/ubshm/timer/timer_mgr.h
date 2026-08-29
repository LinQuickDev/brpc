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

// Timer facade over bthread timers for the ubring module.
//
// Replaces the former timerfd + epoll based timer manager (issue #3463):
// no fd is allocated per timer, deletion never blocks on the non-blocking
// path, and handles are versioned task ids, so stale handles can never hit
// a reused fd.
//
// Lifetime model (see timer_mgr.cpp): the handle slot only keeps the task
// object alive; it does NOT keep `arg' alive. Teardown code that frees
// resources reachable from `arg' must use UbrTimerDelAndWait so that a
// possibly running callback is finished first. Callbacks that delete their
// own timer must use the non-blocking UbrTimerDel instead.

#ifndef BRPC_TIMER_MGR_H
#define BRPC_TIMER_MGR_H

#include <stdint.h>
#include "brpc/ubshm/common/common.h"

namespace brpc {
namespace ubring {

// Opaque timer handle. nullptr means "not started" (or already deleted /
// fired for one-shot timers).
typedef struct UbrTimerTask* UbrTimerId;

// Optionally maps the current re-arm interval of a periodic timer to the
// next one. Runs on the timer thread only.
typedef uint64_t (*UbrTimerBackoffFn)(void* arg, uint64_t cur_interval_us);

// Schedule `cb(arg)' to run once after `delay_us' microseconds. When
// `interval_us' is greater than zero, the task re-arms itself after every
// run (through `backoff' if provided) until deleted.
//
// The handle is published into *slot which must not be touched by other
// threads until this call completes, and must outlive the timer until it
// is deleted or (one-shot) fired. One-shot timers consume themselves on
// fire: *slot is cleared and no explicit delete is needed.
void UbrTimerStart(UbrTimerId* slot, uint64_t delay_us, uint64_t interval_us,
                   void* (*cb)(void*), void* arg,
                   UbrTimerBackoffFn backoff = nullptr);

// Atomic check-and-start variant of UbrTimerStart. Returns UBRING_OK when
// this call scheduled the timer, UBRING_REENTRY when *slot already holds a
// timer (nothing is scheduled by this call), UBRING_ERR on failure.
RETURN_CODE UbrTimerStartOnce(UbrTimerId* slot, uint64_t delay_us,
                              uint64_t interval_us, void* (*cb)(void*),
                              void* arg,
                              UbrTimerBackoffFn backoff = nullptr);

// Stop the timer referenced by *slot and release it. Non-blocking and safe
// to call from inside the timer callback itself (self-delete). This does
// NOT wait for an already running callback and therefore does NOT protect
// `arg' -- use UbrTimerDelAndWait for that.
void UbrTimerDel(UbrTimerId* slot);

// Stop the timer referenced by *slot and wait until a possibly running
// callback has finished, so the caller can safely free resources reachable
// from `arg'. For external teardown only: calling this from inside the
// callback of its own task deadlocks. If a previous non-blocking
// UbrTimerDel already took the slot, this call returns immediately and
// cannot wait for that earlier deletion.
void UbrTimerDelAndWait(UbrTimerId* slot);

uint32_t GetActiveTimerNum(void);

}  // namespace ubring
}  // namespace brpc

#endif //BRPC_TIMER_MGR_H
