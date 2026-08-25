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

#include "brpc/urma/urma_handshake_server.h"

#if BRPC_WITH_URMA

#include <cstring>
#include <string>

#include "butil/iobuf.h"
#include "butil/sys_byteorder.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"

namespace brpc {
namespace urma {

namespace {

constexpr size_t URMA_MAGIC_LEN = 4;
constexpr size_t URMA_V3_SIZE_LEN = 4;
constexpr uint32_t URMA_V3_MAX_PB_SIZE = 4096;

ParseResult SendFallbackHelloV2(butil::IOBuf* source, Socket* socket) {
    constexpr size_t HEADER_LEN = URMA_MAGIC_LEN + sizeof(uint16_t);

    if (source->size() < HEADER_LEN) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    uint8_t header[HEADER_LEN];
    source->copy_to(header, sizeof(header));

    uint16_t msg_len_be = 0;
    std::memcpy(&msg_len_be, header + URMA_MAGIC_LEN,
                sizeof(msg_len_be));
    const uint16_t msg_len = butil::NetToHost16(msg_len_be);

    if (msg_len < v2_wire::HELLO_MSG_LEN_MIN ||
        msg_len > v2_wire::HELLO_MSG_LEN_MAX) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    if (source->size() < msg_len) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    source->pop_front(msg_len);

    v2_wire::HelloMessage reply{};
    reply.msg_len = v2_wire::HELLO_PACKET_LEN;
    reply.hello_ver = 0;
    reply.impl_ver = 0;

    uint8_t packet[v2_wire::HELLO_PACKET_LEN];
    std::memcpy(packet, "URMA", URMA_MAGIC_LEN);
    reply.Serialize(packet + URMA_MAGIC_LEN);

    butil::IOBuf output;
    output.append(packet, sizeof(packet));
    if (socket->Write(&output) != 0) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}
ParseResult SendFallbackHelloV3(butil::IOBuf* source, Socket* socket) {
    constexpr size_t HEADER_LEN = URMA_MAGIC_LEN + URMA_V3_SIZE_LEN;

    if (source->size() < HEADER_LEN) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    uint8_t header[HEADER_LEN];
    source->copy_to(header, sizeof(header));

    uint32_t pb_size_be = 0;
    std::memcpy(&pb_size_be, header + URMA_MAGIC_LEN,
                sizeof(pb_size_be));
    const uint32_t pb_size = butil::NetToHost32(pb_size_be);

    if (pb_size == 0 || pb_size > URMA_V3_MAX_PB_SIZE) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    const size_t total_size = HEADER_LEN + pb_size;
    if (source->size() < total_size) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    source->pop_front(total_size);

    UrmaHello reply;
    reply.set_buffer_size(0);
    reply.set_recv_buffer_cnt(0);
    reply.set_jetty_id(0);
    reply.set_eid(std::string(16, '\0'));
    reply.set_uasid(0);
    reply.set_tp_type(0);
    reply.set_seg_eid(std::string(16, '\0'));
    reply.set_seg_uasid(0);
    reply.set_seg_va(0);
    reply.set_seg_len(0);
    reply.set_seg_token_id(0);

    std::string body;
    if (!reply.SerializeToString(&body)) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    butil::IOBuf output;
    output.append("URM3", URMA_MAGIC_LEN);

    const uint32_t reply_size_be =
        butil::HostToNet32(static_cast<uint32_t>(body.size()));
    output.append(&reply_size_be, sizeof(reply_size_be));
    output.append(body);

    if (socket->Write(&output) != 0) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }

    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}

}  // namespace
ParseResult ExecuteFallbackServerHandshake(
    butil::IOBuf* source, Socket* socket) {
    if (source->size() < URMA_MAGIC_LEN) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    char magic[URMA_MAGIC_LEN];
    source->copy_to(magic, sizeof(magic));

    if (std::memcmp(magic, "URMA", URMA_MAGIC_LEN) == 0) {
        return SendFallbackHelloV2(source, socket);
    }

    if (std::memcmp(magic, "URM3", URMA_MAGIC_LEN) == 0) {
        return SendFallbackHelloV3(source, socket);
    }

    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}
}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA