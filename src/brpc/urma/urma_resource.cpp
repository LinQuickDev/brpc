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

#include "brpc/urma/urma_resource.h"

#if BRPC_WITH_URMA

#include <algorithm>
#include <cerrno>
#include <new>

#include "urma_api.h"
#include "butil/logging.h"

namespace brpc {
namespace urma {

bool LocalJettyConfig::operator==(const LocalJettyConfig& other) const {
    return context == other.context && sq_depth == other.sq_depth &&
           rq_depth == other.rq_depth && max_sge == other.max_sge &&
           priority == other.priority && use_polling == other.use_polling;
}

bool LocalCompletionQueue::CreateCompletionQueue(
        urma_context_t* context, uint32_t depth, bool use_polling) {
    // Polling mode still needs a JFCE for providers that require one.
    errno = 0;
    jfce = urma_create_jfce(context);
    if (!jfce) {
        errno = errno ? errno : EIO;
        PLOG(ERROR) << "urma_create_jfce";
        return false;
    }
    if (!use_polling && jfce->fd < 0) {
        errno = ENODEV;
        PLOG(ERROR) << "Invalid URMA JFCE fd";
        return false;
    }
    urma_jfc_cfg_t cfg{};
    cfg.depth = depth;
    cfg.jfce = jfce;
    errno = 0;
    jfc = urma_create_jfc(context, &cfg);
    if (!jfc) {
        errno = errno ? errno : EIO;
        PLOG(ERROR) << "urma_create_jfc";
        return false;
    }
    return true;
}

LocalCompletionQueue::~LocalCompletionQueue() {
    if (jfc) {
        urma_delete_jfc(jfc);
    }
    if (jfce) {
        urma_delete_jfce(jfce);
    }
}

std::unique_ptr<LocalJettyResources> LocalJettyResources::Create(
        const LocalJettyConfig& config) {
    std::unique_ptr<LocalJettyResources> resource(
        new (std::nothrow) LocalJettyResources(config));
    if (!resource) {
        errno = ENOMEM;
        return nullptr;
    }
    auto failed = [&resource](const char* operation)
            -> std::unique_ptr<LocalJettyResources> {
        const int error = errno ? errno : EIO;
        PLOG(ERROR) << operation;
        resource.reset();
        errno = error;
        return nullptr;
    };

    if (!resource->CreateCompletionQueue(config.context,
            config.sq_depth + config.rq_depth, config.use_polling)) {
        const int error = errno;
        resource.reset();
        errno = error;
        return nullptr;
    }

    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = static_cast<uint32_t>(config.rq_depth);
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = resource->jfc;
    errno = 0;
    resource->jfr = urma_create_jfr(config.context, &jfr_cfg);
    if (!resource->jfr) {
        return failed("urma_create_jfr");
    }

    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(config.sq_depth);
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = config.priority;
    jetty_cfg.jfs_cfg.max_sge = config.max_sge;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = resource->jfc;
    jetty_cfg.shared.jfr = resource->jfr;
    jetty_cfg.shared.jfc = resource->jfc;
    errno = 0;
    resource->jetty = urma_create_jetty(config.context, &jetty_cfg);
    if (!resource->jetty) {
        return failed("urma_create_jetty");
    }
    return resource;
}

LocalJettyResources::~LocalJettyResources() {
    if (jetty) {
        urma_delete_jetty(jetty);
    }
    if (jfr) {
        urma_delete_jfr(jfr);
    }
}

void PreparedJettyPool::Prepare(const LocalJettyConfig& config, size_t count) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!_prepared.empty()) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        auto resource = LocalJettyResources::Create(config);
        if (!resource) {
            break;
        }
        _prepared.push_back(std::move(resource));
    }
}

std::unique_ptr<LocalJettyResources> PreparedJettyPool::Acquire(
        const LocalJettyConfig& config) {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!_prepared.empty() && _prepared.back()->config == config) {
            auto resource = std::move(_prepared.back());
            _prepared.pop_back();
            return resource;
        }
    }
    return LocalJettyResources::Create(config);
}

void PreparedJettyPool::Clear() {
    std::vector<std::unique_ptr<LocalJettyResources>> prepared;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        prepared.swap(_prepared);
    }
}

SharedQueueResources::~SharedQueueResources() {
    if (_jfs) {
        urma_delete_jfs(_jfs);
    }
    if (_jfr) {
        urma_delete_jfr(_jfr);
    }
}

std::unique_ptr<SharedQueuePool> SharedQueuePool::Create(
        const SharedQueueConfig& config, const urma_device_cap_t& caps) {
    if (!config.context || !config.send_count || !config.recv_count ||
        !config.send_depth || !config.recv_depth || !config.send_max_sge ||
        !config.recv_max_sge || config.priority > URMA_MAX_PRIORITY) {
        errno = EINVAL;
        return nullptr;
    }
    if (config.send_count > caps.max_jfs || config.recv_count > caps.max_jfr ||
        static_cast<uint64_t>(config.send_count) + config.recv_count >
            caps.max_jfc ||
        config.send_depth > std::min(caps.max_jfs_depth, caps.max_jfc_depth) ||
        config.recv_depth > std::min(caps.max_jfr_depth, caps.max_jfc_depth) ||
        config.send_max_sge > caps.max_jfs_sge ||
        config.recv_max_sge > caps.max_jfr_sge) {
        errno = ENOSPC;
        return nullptr;
    }

    std::unique_ptr<SharedQueuePool> pool(new SharedQueuePool);
    for (bool send : {true, false}) {
        auto& slots = send ? pool->_send : pool->_recv;
        const uint32_t count = send ? config.send_count : config.recv_count;
        const uint32_t depth = send ? config.send_depth : config.recv_depth;
        slots.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            slots.emplace_back(new SharedQueueResources);
            auto& slot = *slots.back();
            if (!slot.CreateCompletionQueue(config.context, depth, false)) {
                const int error = errno;
                pool.reset();
                errno = error;
                return nullptr;
            }
            errno = 0;
            if (send) {
                urma_jfs_cfg_t cfg{};
                cfg.depth = depth;
                cfg.trans_mode = URMA_TM_RM;
                cfg.priority = config.priority;
                cfg.max_sge = config.send_max_sge;
                cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
                cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
                cfg.jfc = slot.jfc;
                slot._jfs = urma_create_jfs(config.context, &cfg);
            } else {
                urma_jfr_cfg_t cfg{};
                cfg.depth = depth;
                cfg.trans_mode = URMA_TM_RM;
                cfg.max_sge = config.recv_max_sge;
                cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
                cfg.jfc = slot.jfc;
                slot._jfr = urma_create_jfr(config.context, &cfg);
            }
            if (!slot._jfs && !slot._jfr) {
                const int error = errno ? errno : EIO;
                errno = error;
                PLOG(ERROR) << (send ? "urma_create_jfs" : "urma_create_jfr");
                pool.reset();
                errno = error;
                return nullptr;
            }
        }
    }
    return pool;
}

SharedQueuePool::Lease SharedQueuePool::Acquire() {
    const uint64_t ticket = _next.fetch_add(1, std::memory_order_relaxed);
    const size_t send = ticket % _send.size();
    // Rotate receive assignments on every send cycle so equal pool sizes
    // do not permanently pair each send slot with exactly one receive slot.
    const size_t recv = (ticket / _send.size() + send) % _recv.size();
    return {_send[send], _recv[recv]};
}

PeerResources::~PeerResources() {
    Reset();
}

void PeerResources::Reset() {
    if (jetty) {
        urma_unimport_jetty(jetty);
        jetty = nullptr;
    }
    if (segment) {
        urma_unimport_seg(segment);
        segment = nullptr;
    }
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
