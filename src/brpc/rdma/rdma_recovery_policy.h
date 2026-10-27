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

#ifndef BRPC_RDMA_RECOVERY_POLICY_H
#define BRPC_RDMA_RECOVERY_POLICY_H

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <mutex>
#include <string>

namespace brpc {
namespace rdma {

enum class RdmaRecoveryMode { LEGACY, AUTO, TCP_ONLY };

enum class RdmaFailureReason {
    NONE, RETRY_EXHAUSTED, RNR, QP_FATAL, CQ_ERROR, CQ_POLL,
    CQ_EVENT, CQ_NOTIFY, SEND_POST, ACCESS_ERROR, COUNT
};

const char* RdmaFailureReasonName(RdmaFailureReason reason);

struct RdmaRecoveryStatus {
    RdmaRecoveryMode mode = RdmaRecoveryMode::LEGACY;
    uint64_t epoch = 0;
    bool port_active = true;
    int64_t port_cooldown_ms = 0;
    size_t cooled_peers = 0;
    uint64_t failures[static_cast<size_t>(RdmaFailureReason::COUNT)] = {};
    uint64_t tcp_only_admissions = 0;
    uint64_t port_cooldown_admissions = 0;
    uint64_t peer_cooldown_admissions = 0;
    uint64_t port_errors = 0;
    uint64_t cache_evictions = 0;
    uint64_t negotiated_connections = 0;
};

// Admission policy for new connections on the process's selected RDMA port.
// An empty peer selects server admission, which only checks the local port.
class RdmaRecoveryPolicy {
public:
    typedef int64_t (*Clock)();
    explicit RdmaRecoveryPolicy(Clock clock, size_t max_peers = 1024);
    bool SetMode(RdmaRecoveryMode mode);
    RdmaRecoveryMode GetMode();
    // Positive milliseconds, at most INT32_MAX. Applies to future failures.
    bool SetCooldownMs(int64_t cooldown_ms);
    bool AllowNewConnection(const std::string& peer, uint64_t* epoch = nullptr);
    void ReportPeerFailure(const std::string& peer, uint64_t epoch,
                           RdmaFailureReason reason = RdmaFailureReason::RETRY_EXHAUSTED);
    void ReportPortState(bool active);
    RdmaRecoveryStatus GetStatus();
    void RecordNegotiatedConnection(uint64_t epoch);

private:
    std::mutex _mutex;
    const Clock _clock;
    const size_t _max_peers;
    RdmaRecoveryMode _mode;
    uint64_t _epoch;
    int64_t _cooldown_us;
    bool _port_active;
    int64_t _port_retry_at;
    std::map<std::string, int64_t> _peers;
    RdmaRecoveryStatus _status;
};

// Process-lifetime policy, shared by client and server handshake admission.
RdmaRecoveryPolicy& GlobalRdmaRecoveryPolicy();
bool SetRdmaRecoveryMode(RdmaRecoveryMode mode);
RdmaRecoveryMode GetRdmaRecoveryMode();
bool SetRdmaRecoveryCooldownMs(int64_t cooldown_ms);
RdmaRecoveryStatus GetRdmaRecoveryStatus();

}  // namespace rdma
}  // namespace brpc

#endif  // BRPC_RDMA_RECOVERY_POLICY_H
