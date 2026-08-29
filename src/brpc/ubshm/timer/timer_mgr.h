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

// bthread based timer facade for the ubring module. Callbacks run on the
// process-wide bthread timer thread and must return quickly.

#ifndef BRPC_TIMER_MGR_H
#define BRPC_TIMER_MGR_H

#include <stdint.h>
#include "brpc/ubshm/common/common.h"

namespace brpc {
namespace ubring {

// Opaque timer handle. nullptr means "not started" (or already deleted /
// fired for one-shot timers).
typedef struct UbrTimerTask* UbrTimerId;

// Maps the current re-arm interval of a periodic timer to the next one.
// Runs on the timer thread only.
typedef uint64_t (*UbrTimerBackoffFn)(void* arg, uint64_t cur_interval_us);

// Schedule `cb(arg)' to run after `delay_us' and, when `interval_us' > 0,
// re-arm itself after every run until deleted. One-shot timers clear *slot
// and release themselves when fired. The handle slot keeps the task object
// alive, not `arg'.
RETURN_CODE UbrTimerStart(UbrTimerId* slot, uint64_t delay_us,
                          uint64_t interval_us, void* (*cb)(void*),
                          void* arg,
                          UbrTimerBackoffFn backoff = nullptr);

// Atomic check-and-start: UBRING_OK if scheduled by this call,
// UBRING_REENTRY if *slot already holds a timer, UBRING_ERR on failure.
RETURN_CODE UbrTimerStartOnce(UbrTimerId* slot, uint64_t delay_us,
                              uint64_t interval_us, void* (*cb)(void*),
                              void* arg,
                              UbrTimerBackoffFn backoff = nullptr);

// Non-blocking delete, safe from inside the timer callback itself. Does
// not wait for a running callback and does not protect `arg'.
void UbrTimerDel(UbrTimerId* slot);

// Delete and wait until a possibly running callback finished, so the
// caller can free resources reachable from `arg'. Never call this on the
// callback's own task.
void UbrTimerDelAndWait(UbrTimerId* slot);

}  // namespace ubring
}  // namespace brpc

#endif //BRPC_TIMER_MGR_H
