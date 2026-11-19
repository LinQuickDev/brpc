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

#ifndef BRPC_URMA_RESOURCE_H
#define BRPC_URMA_RESOURCE_H

#if BRPC_WITH_URMA

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "urma_types.h"
#include "butil/macros.h"

namespace brpc {
namespace urma {

struct LocalJettyConfig {
    urma_context_t* context;
    uint16_t sq_depth;
    uint16_t rq_depth;
    uint8_t max_sge;
    uint8_t priority;
    bool use_polling;

    bool operator==(const LocalJettyConfig& other) const;
};

// A queue owner deletes its send/receive queues before this base destroys
// their completion queue and event channel.
class LocalCompletionQueue {
public:
    urma_jfce_t* jfce{nullptr};
    urma_jfc_t* jfc{nullptr};

protected:
    LocalCompletionQueue() = default;
    ~LocalCompletionQueue();
    bool CreateCompletionQueue(urma_context_t*, uint32_t depth, bool use_polling);

private:
    DISALLOW_COPY_AND_ASSIGN(LocalCompletionQueue);
};

// Owns the local kernel objects used by the cold and prepared paths.
struct LocalJettyResources : LocalCompletionQueue {
    static std::unique_ptr<LocalJettyResources> Create(
        const LocalJettyConfig& config);
    ~LocalJettyResources();

    const LocalJettyConfig config;
    urma_jfr_t* jfr{nullptr};
    urma_jetty_t* jetty{nullptr};

private:
    explicit LocalJettyResources(const LocalJettyConfig& cfg) : config(cfg) {}
    DISALLOW_COPY_AND_ASSIGN(LocalJettyResources);
};

// Stores unused exclusive resources. Checkout transfers ownership; used
// resources are destroyed by their connection.
class PreparedJettyPool {
public:
    PreparedJettyPool() = default;
    void Prepare(const LocalJettyConfig&, size_t count);
    std::unique_ptr<LocalJettyResources> Acquire(const LocalJettyConfig&);
    void Clear();

private:
    std::mutex _mutex;
    std::vector<std::unique_ptr<LocalJettyResources>> _prepared;
    DISALLOW_COPY_AND_ASSIGN(PreparedJettyPool);
};

struct SharedQueueConfig {
    urma_context_t* context;
    uint32_t send_count;
    uint32_t recv_count;
    uint32_t send_depth;
    uint32_t recv_depth;
    uint8_t send_max_sge;
    uint8_t recv_max_sge;
    uint8_t priority;
};

// Owns one standalone JFS or JFR, with a dedicated CQ and event channel.
// Callers must stop polling and end outstanding work before releasing the
// final owner, and keep the URMA context alive for the entire lifetime.
class SharedQueueResources : public LocalCompletionQueue {
public:
    ~SharedQueueResources();
    urma_jfs_t* jfs() const { return _jfs; }
    urma_jfr_t* jfr() const { return _jfr; }

private:
    SharedQueueResources() = default;
    friend class SharedQueuePool;
    urma_jfs_t* _jfs{nullptr};
    urma_jfr_t* _jfr{nullptr};
    DISALLOW_COPY_AND_ASSIGN(SharedQueueResources);
};

// Create one instance for a device context. Acquiring a pair never allocates
// provider resources and does not reserve WR slots. Posting, credits and CQ
// dispatch belong to the transport using these queues.
class SharedQueuePool {
public:
    struct Lease {
        std::shared_ptr<const SharedQueueResources> send;
        std::shared_ptr<const SharedQueueResources> recv;
    };

    static std::unique_ptr<SharedQueuePool> Create(
        const SharedQueueConfig& config, const urma_device_cap_t& caps);
    Lease Acquire();

private:
    SharedQueuePool() = default;
    std::vector<std::shared_ptr<SharedQueueResources>> _send;
    std::vector<std::shared_ptr<SharedQueueResources>> _recv;
    std::atomic<uint64_t> _next{0};
    DISALLOW_COPY_AND_ASSIGN(SharedQueuePool);
};

// Remote imports belong to a single handshake and never enter the prepared pool.
struct PeerResources {
    PeerResources() = default;
    ~PeerResources();
    void Reset();

    urma_target_jetty_t* jetty{nullptr};
    urma_target_seg_t* segment{nullptr};

private:
    DISALLOW_COPY_AND_ASSIGN(PeerResources);
};

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_RESOURCE_H
