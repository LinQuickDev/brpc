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

#include <stdint.h>
#include "butil/atomicops.h"
#include "butil/containers/mpsc_queue.h"
#include "butil/logging.h"
#include "bthread/bthread.h"
#include "brpc/ubshm/ub_cleanup_worker.h"
#include "brpc/ubshm/ub_ring_manager.h"

namespace brpc {
namespace ubring {

namespace {

// Cleanup latency is tolerated in seconds (it only delays releasing memory of
// an already dead link), so the idle worker polls instead of spinning and the
// module needs no extra wakeup primitive.
constexpr uint64_t kCleanupIdlePollIntervalUs = 100 * 1000;

butil::atomic<bool> g_worker_started(false);
// Set when the worker bthread could not be created. Posting then fails
// forever, and every caller falls back to running its work inline -- the
// behaviour from before the worker existed.
butil::atomic<bool> g_worker_unavailable(false);
// Posted-but-not-finished jobs. A poster increments it before enqueueing and
// the worker decrements it after the job returned, so DrainAndWait can wait
// for quiescence without observing the queue itself.
butil::atomic<int> g_cleanup_inflight(0);
butil::MPSCQueue<UbrCleanupJob> g_cleanup_queue;

void RunTrxCleanup(const UbrCleanupJob& job) {
    UbrCleanupCtl* ctl = job.ctl;
    // The slot may have been released and reused while the job was queued; the
    // cleanup body must then be skipped, but the control object still has to
    // be settled (exactly as the timer callback did before the offload).
    if (UBRingManager::IsUbrTrxSlotUsed(job.trx->trx_mgr_index, job.ubr_id)) {
        job.work(job.trx, job.ubr_id);
    }
    ATOMIC_STORE(ctl->state, UBR_CLEANUP_DONE);
    ctl->ReleaseRef();                       // timer/callback reference
}

void* UbrCleanupWorkerMain(void*) {
    UbrCleanupJob job;
    for (;;) {
        if (!g_cleanup_queue.Dequeue(job)) {
            bthread_usleep(kCleanupIdlePollIntervalUs);
            continue;
        }
        if (job.type == UbrCleanupJob::SHM_DRAIN) {
            job.shm_step(job.shm_list);
        } else {
            RunTrxCleanup(job);
        }
        g_cleanup_inflight.fetch_sub(1);
    }
    return nullptr;
}

bool EnsureStarted() {
    if (g_worker_unavailable.load()) {
        return false;
    }
    if (g_worker_started.load()) {
        return true;
    }
    bool expected = false;
    if (!g_worker_started.compare_exchange_strong(expected, true)) {
        return true;                         // another poster is starting it
    }
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UBCleanup");
    bthread_t tid;
    if (BAIDU_UNLIKELY(bthread_start_background(
            &tid, &attr, UbrCleanupWorkerMain, nullptr) != 0)) {
        LOG(ERROR) << "Fail to start the ubring cleanup worker bthread.";
        g_worker_unavailable.store(true);
        g_worker_started.store(false);
        return false;
    }
    return true;
}

bool Post(const UbrCleanupJob& job) {
    if (!EnsureStarted()) {
        return false;
    }
    // Increment before enqueueing so a concurrent DrainAndWait can never
    // observe quiescence while this job is still on its way to the queue.
    g_cleanup_inflight.fetch_add(1);
    g_cleanup_queue.Enqueue(job);
    return true;
}

}  // namespace

bool UbrCleanupWorker::PostShmDrain(ShmList* shm_list,
                                    void (*shm_step)(ShmList* shm_list)) {
    if (BAIDU_UNLIKELY(shm_list == nullptr || shm_step == nullptr)) {
        return false;
    }
    UbrCleanupJob job;
    job.type = UbrCleanupJob::SHM_DRAIN;
    job.shm_list = shm_list;
    job.shm_step = shm_step;
    return Post(job);
}

bool UbrCleanupWorker::PostTrxCleanup(UbrTrx* trx, uint64_t ubr_id,
                                      void (*work)(UbrTrx*, uint64_t),
                                      UbrCleanupCtl* ctl) {
    if (BAIDU_UNLIKELY(trx == nullptr || work == nullptr || ctl == nullptr)) {
        return false;
    }
    UbrCleanupJob job;
    job.type = UbrCleanupJob::TRX_CLEANUP;
    job.trx = trx;
    job.ubr_id = ubr_id;
    job.work = work;
    job.ctl = ctl;
    return Post(job);
}

void UbrCleanupWorker::DrainAndWait() {
    while (g_cleanup_inflight.load() > 0) {
        bthread_usleep(1000);
    }
}

}  // namespace ubring
}  // namespace brpc
