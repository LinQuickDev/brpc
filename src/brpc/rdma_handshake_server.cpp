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

#include "brpc/rdma_handshake_server.h"

#include <errno.h>
#include <limits>
#include <string>

#include "butil/raw_pack.h"
#include "butil/sys_byteorder.h"
#include "brpc/adapter_transport.h"
#include "brpc/rdma_handshake.pb.h"
#include "brpc/rdma_handshake_constants.h"
#include "brpc/socket.h"
#if BRPC_WITH_RDMA
#include "brpc/rdma_transport.h"
#endif

namespace brpc {
namespace rdma {

// Fallback-only field codecs for connections that do not own an
// RdmaEndpoint. Framing, incremental input, ACK handling, and state are still
// driven by HandshakeSession, exactly like the real RDMA server handshake.
static constexpr uint16_t V2_HELLO_VERSION_INVALID =
    std::numeric_limits<uint16_t>::max();
static constexpr size_t V3_GID_LEN = 16;

static handshake::HandshakeCodec MakeFallbackCodec(int version) {
    handshake::HandshakeCodec codec{};
    codec.protocol_version = version;
    codec.hello_frame = RdmaHelloFrameSpec(version);
    codec.ack_frame = RdmaAckFrameSpec();
    codec.parse_hello = [](const std::string&) {
        return handshake::STEP_FALLBACK;
    };
    codec.build_hello = [version](bool enabled, std::string* payload) {
        if (enabled) {
            errno = EPROTO;
            return handshake::STEP_ERROR;
        }
        if (version == 2) {
            // FrameCodec emits magic and msg_len. The protocol payload starts
            // at hello_ver and deliberately advertises an invalid version.
            payload->assign(
                HELLO_V2_MSG_LEN_MIN - HELLO_MAGIC_LEN - sizeof(uint16_t),
                '\0');
            butil::RawPacker(&(*payload)[0])
                .pack16(V2_HELLO_VERSION_INVALID);
            return handshake::STEP_OK;
        }

        RdmaHello reply;
        reply.set_block_size(0);
        reply.set_sq_size(0);
        reply.set_rq_size(0);
        reply.set_lid(0);
        reply.set_gid(std::string(V3_GID_LEN, '\0'));
        reply.set_qp_num(0);
        if (!reply.SerializeToString(payload)) {
            errno = EPROTO;
            return handshake::STEP_ERROR;
        }
        return handshake::STEP_OK;
    };
    codec.build_ack = [](bool enabled, std::string* payload) {
        const uint32_t flags_be = butil::HostToNet32(
            enabled ? HELLO_ACK_RDMA_OK : 0);
        payload->assign(reinterpret_cast<const char*>(&flags_be),
                        sizeof(flags_be));
        return handshake::STEP_OK;
    };
    codec.parse_ack = [](const std::string& payload, bool* enabled) {
        if (payload.size() != HELLO_ACK_LEN) {
            errno = EPROTO;
            return handshake::STEP_ERROR;
        }
        // This server cannot upgrade, so any well-framed ACK completes the
        // downgrade. A conforming peer sends zero after the disabled hello.
        *enabled = false;
        return handshake::STEP_OK;
    };
    return codec;
}

static ParseResult FallbackServerHandshake(
    butil::IOBuf* source, Socket* socket) {
    AdapterTransport* adapter = AdapterTransport::Get(socket);
    handshake::IOBufHandshakeInput input(source);
    handshake::ServerHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{1, 2, 3, 4, 5, 6};
    callbacks.fallback_on_not_mine = false;
    callbacks.codecs.push_back(MakeFallbackCodec(2));
    callbacks.codecs.push_back(MakeFallbackCodec(3));
    callbacks.input = &input;
    callbacks.prepare_resources = []() { return handshake::STEP_OK; };
    callbacks.negotiate_resources = []() { return handshake::STEP_OK; };
    callbacks.set_high_speed_active = []() {};
    callbacks.set_tcp_active = []() {};
    callbacks.on_failed = []() {};

    const handshake::StepResult result =
        adapter->handshake_session()->RunServer(callbacks);
    if (result == handshake::STEP_NEED_MORE) {
        if (adapter->handshake_phase() == callbacks.phases.ack_wait &&
            socket->parsing_context() == NULL) {
            socket->reset_parsing_context(ServerHandshakeContext::Create());
        }
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    socket->reset_parsing_context(NULL);
    if (result == handshake::STEP_ERROR) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }
    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}

ParseResult ExecuteServerHandshake(butil::IOBuf* source, Socket* socket) {
#if BRPC_WITH_RDMA
    if (socket->socket_mode() == SOCKET_MODE_RDMA) {
        return RdmaTransport::ExecuteServerHandshake(source, socket);
    }
#endif
    return FallbackServerHandshake(source, socket);
}

}  // namespace rdma
}  // namespace brpc
