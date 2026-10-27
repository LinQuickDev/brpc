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

#include "brpc/rdma/rdma_recovery_policy.h"
#include "butil/time.h"
#include "bvar/passive_status.h"

namespace brpc {
namespace rdma {

const char* RdmaFailureReasonName(RdmaFailureReason reason) {
    switch (reason) {
    case RdmaFailureReason::NONE: return "none";
    case RdmaFailureReason::RETRY_EXHAUSTED: return "retry_exhausted";
    case RdmaFailureReason::RNR: return "rnr";
    case RdmaFailureReason::QP_FATAL: return "qp_fatal";
    case RdmaFailureReason::CQ_ERROR: return "cq_error";
    case RdmaFailureReason::CQ_POLL: return "cq_poll";
    case RdmaFailureReason::CQ_EVENT: return "cq_event";
    case RdmaFailureReason::CQ_NOTIFY: return "cq_notify";
    case RdmaFailureReason::SEND_POST: return "send_post";
    case RdmaFailureReason::ACCESS_ERROR: return "access_error";
    default: return "unknown";
    }
}

RdmaRecoveryPolicy::RdmaRecoveryPolicy(Clock clock, size_t max_peers)
    : _clock(clock)
    , _max_peers(max_peers)
    , _mode(RdmaRecoveryMode::LEGACY)
    , _epoch(1)
    , _cooldown_us(5000000)
    , _port_active(true)
    , _port_retry_at(0) {
}

bool RdmaRecoveryPolicy::SetMode(RdmaRecoveryMode mode) {
    if (mode != RdmaRecoveryMode::LEGACY && mode != RdmaRecoveryMode::AUTO &&
        mode != RdmaRecoveryMode::TCP_ONLY) {
        return false;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    if (_mode != mode) {
        _mode = mode;
        ++_epoch;
        _peers.clear();
    }
    return true;
}

RdmaRecoveryMode RdmaRecoveryPolicy::GetMode() {
    std::lock_guard<std::mutex> lock(_mutex);
    return _mode;
}

bool RdmaRecoveryPolicy::SetCooldownMs(int64_t cooldown_ms) {
    if (cooldown_ms <= 0 || cooldown_ms > INT32_MAX) {
        return false;
    }
    std::lock_guard<std::mutex> lock(_mutex);
    _cooldown_us = cooldown_ms * 1000;
    return true;
}

bool RdmaRecoveryPolicy::AllowNewConnection(const std::string& peer,
                                            uint64_t* epoch) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (epoch != nullptr) {
        *epoch = _epoch;
    }
    if (_mode == RdmaRecoveryMode::LEGACY) {
        return true;
    }
    if (_mode == RdmaRecoveryMode::TCP_ONLY) {
        ++_status.tcp_only_admissions;
        return false;
    }
    const int64_t now = _clock();
    if (!_port_active || now < _port_retry_at) {
        ++_status.port_cooldown_admissions;
        return false;
    }
    auto it = _peers.find(peer);
    if (it == _peers.end()) {
        return true;
    }
    if (now < it->second) {
        ++_status.peer_cooldown_admissions;
        return false;
    }
    _peers.erase(it);
    return true;
}

void RdmaRecoveryPolicy::ReportPeerFailure(const std::string& peer,
                                          uint64_t epoch,
                                          RdmaFailureReason reason) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (epoch != _epoch || reason <= RdmaFailureReason::NONE ||
        reason >= RdmaFailureReason::COUNT) {
        return;
    }
    ++_status.failures[static_cast<size_t>(reason)];
    if (_mode != RdmaRecoveryMode::AUTO || peer.empty() || _max_peers == 0 ||
        reason == RdmaFailureReason::ACCESS_ERROR) {
        return;
    }
    const int64_t now = _clock();
    for (auto it = _peers.begin(); it != _peers.end();) {
        if (it->second <= now) {
            it = _peers.erase(it);
        } else {
            ++it;
        }
    }
    if (_peers.count(peer) == 0 && _peers.size() == _max_peers) {
        auto oldest = _peers.begin();
        for (auto it = _peers.begin(); it != _peers.end(); ++it) {
            if (it->second < oldest->second) {
                oldest = it;
            }
        }
        _peers.erase(oldest);
        ++_status.cache_evictions;
    }
    _peers[peer] = now + _cooldown_us;
}

void RdmaRecoveryPolicy::ReportPortState(bool active) {
    std::lock_guard<std::mutex> lock(_mutex);
    _port_active = active;
    if (!active) {
        ++_status.port_errors;
        _port_retry_at = _clock() + _cooldown_us;
    }
}

RdmaRecoveryStatus RdmaRecoveryPolicy::GetStatus() {
    std::lock_guard<std::mutex> lock(_mutex);
    RdmaRecoveryStatus result = _status;
    result.mode = _mode;
    result.epoch = _epoch;
    result.port_active = _port_active;
    const int64_t now = _clock();
    result.port_cooldown_ms = _port_retry_at > now ?
        (_port_retry_at - now + 999) / 1000 : 0;
    for (const auto& peer : _peers) {
        result.cooled_peers += peer.second > now;
    }
    return result;
}

void RdmaRecoveryPolicy::RecordNegotiatedConnection(uint64_t epoch) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (epoch == _epoch) {
        ++_status.negotiated_connections;
    }
}

static void DescribeRecovery(std::ostream& os, void* arg) {
    const auto s = static_cast<RdmaRecoveryPolicy*>(arg)->GetStatus();
    os << "{\"mode\":\""
       << (s.mode == RdmaRecoveryMode::AUTO ? "auto" :
           s.mode == RdmaRecoveryMode::TCP_ONLY ? "tcp_only" : "legacy")
       << "\",\"epoch\":" << s.epoch
       << ",\"port_active\":" << (s.port_active ? "true" : "false")
       << ",\"port_cooldown_ms\":" << s.port_cooldown_ms
       << ",\"cooled_peers\":" << s.cooled_peers
       << ",\"tcp_only_admissions\":" << s.tcp_only_admissions
       << ",\"port_cooldown_admissions\":" << s.port_cooldown_admissions
       << ",\"peer_cooldown_admissions\":" << s.peer_cooldown_admissions
       << ",\"port_errors\":" << s.port_errors
       << ",\"cache_evictions\":" << s.cache_evictions
       << ",\"negotiated_connections\":" << s.negotiated_connections
       << ",\"failures\":{";
    for (size_t i = 1; i < static_cast<size_t>(RdmaFailureReason::COUNT); ++i) {
        if (i > 1) {
            os << ',';
        }
        os << '"' << RdmaFailureReasonName(static_cast<RdmaFailureReason>(i))
           << "\":" << s.failures[i];
    }
    os << "}}";
}

struct RecoveryMetrics {
    explicit RecoveryMetrics(RdmaRecoveryPolicy* policy)
        : status("rdma_recovery_status", DescribeRecovery, policy)
        , fallbacks("rdma_recovery_tcp_fallback_admissions", [](void* arg) {
            const auto s = static_cast<RdmaRecoveryPolicy*>(arg)->GetStatus();
            return s.tcp_only_admissions + s.port_cooldown_admissions +
                   s.peer_cooldown_admissions;
          }, policy) {
    }
    bvar::PassiveStatus<std::string> status;
    bvar::PassiveStatus<uint64_t> fallbacks;
};

RdmaRecoveryPolicy& GlobalRdmaRecoveryPolicy() {
    // Async event callbacks may outlive ordinary static destructors.
    static RdmaRecoveryPolicy* policy =
        new RdmaRecoveryPolicy(butil::monotonic_time_us);
    static RecoveryMetrics* metrics = new RecoveryMetrics(policy);
    (void)metrics;
    return *policy;
}

bool SetRdmaRecoveryMode(RdmaRecoveryMode mode) {
    return GlobalRdmaRecoveryPolicy().SetMode(mode);
}

RdmaRecoveryMode GetRdmaRecoveryMode() {
    return GlobalRdmaRecoveryPolicy().GetMode();
}

bool SetRdmaRecoveryCooldownMs(int64_t cooldown_ms) {
    return GlobalRdmaRecoveryPolicy().SetCooldownMs(cooldown_ms);
}

RdmaRecoveryStatus GetRdmaRecoveryStatus() {
    return GlobalRdmaRecoveryPolicy().GetStatus();
}

}  // namespace rdma
}  // namespace brpc
