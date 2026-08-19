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

#include "brpc/policy/transport_handshake_protocol.h"

#include "butil/logging.h"
#include "brpc/destroyable.h"
#include "brpc/handshake/rdma_handshake.h"
#include "brpc/handshake/ubshm_handshake.h"
#include "brpc/transport_handshake.h"

namespace brpc {
namespace policy {

ParseResult ParseTransportHandshake(butil::IOBuf* source, Socket* socket,
                                     bool /*read_eof*/, const void* /*arg*/) {
    if (socket->parsing_context() != NULL) {
        handshake::ServerHandshakeContext* context =
            static_cast<handshake::ServerHandshakeContext*>(
                socket->parsing_context());
        CHECK(context->adapter() != NULL);
        return context->adapter()->ExecuteServerHandshake(source, socket);
    }

    const char* first = static_cast<const char*>(source->fetch1());
    handshake::HandshakeAdapter* adapter =
        first != NULL && *first == 'U'
        ? handshake::GetUBShmServerHandshakeAdapter()
        : handshake::GetRdmaServerHandshakeAdapter();
    return adapter->ExecuteServerHandshake(source, socket);
}

void ProcessTransportHandshake(InputMessageBase* msg) {
    DestroyingPtr<InputMessageBase> destroying_msg(msg);
    CHECK(false) << "ProcessTransportHandshake should never be called";
}

}  // namespace policy
}  // namespace brpc
