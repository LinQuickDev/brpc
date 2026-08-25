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

#include "brpc/policy/rdma_handshake_protocol.h"
#include <cstring>

#if BRPC_WITH_URMA
#include "brpc/urma/urma_handshake_server.h"
#endif
#include "butil/logging.h"
#include "brpc/destroyable.h"
#include "brpc/rdma/rdma_handshake_server.h"

namespace brpc {
namespace policy {

ParseResult ParseRdmaHandshake(butil::IOBuf* source, Socket* socket,
                               bool /*read_eof*/, const void* /*arg*/) {
#if BRPC_WITH_URMA
    if (source->size() >= 4) {
        char magic[4];
        source->copy_to(magic, sizeof(magic));

        if (std::memcmp(magic, "URMA", sizeof(magic)) == 0 ||
            std::memcmp(magic, "URM3", sizeof(magic)) == 0) {
            return urma::ExecuteFallbackServerHandshake(source, socket);
        }
    }
#endif

    return rdma::ExecuteServerHandshake(source, socket);
}

void ProcessRdmaHandshake(InputMessageBase* msg) {
    // ParseRdmaHandshake replies inline and only ever returns
    // NOT_ENOUGH_DATA / TRY_OTHERS / hard errors, never a real message, so this
    // must never run. Keep a placeholder (required for server registration).
    DestroyingPtr<InputMessageBase> destroying_msg(msg);
    CHECK(false) << "ProcessRdmaHandshake should never be called";
}

}  // namespace policy
}  // namespace brpc
