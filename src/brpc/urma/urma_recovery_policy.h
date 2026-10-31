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

#ifndef BRPC_URMA_RECOVERY_POLICY_H
#define BRPC_URMA_RECOVERY_POLICY_H

#include "brpc/rdma/rdma_recovery_policy.h"

namespace brpc {
namespace urma {

// Share the admission algorithm while keeping transport state independent.
using UrmaRecoveryMode = rdma::RdmaRecoveryMode;
using UrmaFailureReason = rdma::RdmaFailureReason;
using UrmaRecoveryStatus = rdma::RdmaRecoveryStatus;
using UrmaRecoveryPolicy = rdma::RdmaRecoveryPolicy;

UrmaRecoveryPolicy& GlobalUrmaRecoveryPolicy();
bool SetUrmaRecoveryMode(UrmaRecoveryMode mode);
UrmaRecoveryMode GetUrmaRecoveryMode();
bool SetUrmaRecoveryCooldownMs(int64_t cooldown_ms);
UrmaRecoveryStatus GetUrmaRecoveryStatus();

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_URMA_RECOVERY_POLICY_H
