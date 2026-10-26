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

namespace brpc {
namespace rdma {

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
        return false;
    }
    const int64_t now = _clock();
    if (!_port_active || now < _port_retry_at) {
        return false;
    }
    auto it = _peers.find(peer);
    if (it == _peers.end()) {
        return true;
    }
    if (now < it->second) {
        return false;
    }
    _peers.erase(it);
    return true;
}

void RdmaRecoveryPolicy::ReportPeerFailure(const std::string& peer,
                                          uint64_t epoch) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_mode != RdmaRecoveryMode::AUTO || epoch != _epoch ||
        peer.empty() || _max_peers == 0) {
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
    }
    _peers[peer] = now + _cooldown_us;
}

void RdmaRecoveryPolicy::ReportPortState(bool active) {
    std::lock_guard<std::mutex> lock(_mutex);
    _port_active = active;
    if (!active) {
        _port_retry_at = _clock() + _cooldown_us;
    }
}

RdmaRecoveryPolicy& GlobalRdmaRecoveryPolicy() {
    // Async event callbacks may outlive ordinary static destructors.
    static RdmaRecoveryPolicy* policy =
        new RdmaRecoveryPolicy(butil::monotonic_time_us);
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

}  // namespace rdma
}  // namespace brpc
