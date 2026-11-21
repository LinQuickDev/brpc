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

#ifndef BRPC_URMA_MOCK_URMA_H
#define BRPC_URMA_MOCK_URMA_H

#include <cstddef>

#include "urma_api.h"

namespace brpc {
namespace urma {
enum MockFault {
    MOCK_FAIL_SEND,
    MOCK_FAIL_CREDIT,
    MOCK_HOLD_CREDIT,
    MOCK_HOLD_SEND_COMPLETION,
    MOCK_FAIL_RECV,
    MOCK_ERROR_COMPLETION
};

struct MockResources {
    size_t jettys;
    size_t jfcs;
    size_t jfrs;
    size_t jfces;
    size_t targets;
    size_t segments;
    size_t deferred;
};
}  // namespace urma
}  // namespace brpc

// These entry points exist only in the link-time mock. Faults are scoped to
// a jetty; count=-1 applies until release, otherwise count matching WRs.
extern "C" {
void brpc_urma_mock_fault(urma_jetty_t*, brpc::urma::MockFault, int count);
size_t brpc_urma_mock_release(urma_jetty_t*);
brpc::urma::MockResources brpc_urma_mock_resources();
}

#endif  // BRPC_URMA_MOCK_URMA_H
