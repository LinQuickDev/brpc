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

#ifndef BRPC_HANDSHAKE_HELLO_DESCRIPTOR_H
#define BRPC_HANDSHAKE_HELLO_DESCRIPTOR_H

#include <cstdint>
#include <string>

#include "brpc/socket_mode.h"

namespace brpc {
namespace handshake {

// In-process superset of every transport's hello message. This descriptor is
// NEVER serialized directly: each protocol codec translates between its
// frozen wire format and this struct, so the struct may evolve (new slots,
// new capability flags) without affecting wire compatibility. See
// docs/cn/transport_handshake_design.md for the full rationale.
//
// Dependency discipline: POD members + std::string only. Transport SDK types
// (ibv_*, urma_* ...) are forbidden here; opaque fields carry such data and
// the owning protocol's codec/endpoint interprets them.
//
// Evolution discipline: append-only. Never change the meaning of an existing
// field, never reference one slot from another. Private experiments go
// through `opaque_payload', not the common area.
struct HelloDescriptor {
    // ---- Common area (required for all protocols) ----
    SocketMode mode = SOCKET_MODE_TCP;
    int wire_version = 0;        // wire version of the peer's hello
    bool peer_enabled = false;   // peer requests upgrade (false = probe only)

    // ---- Capability area: presence-as-capability ----
    bool has_extension = false;    // peer sends an extension frame after ACK
    bool has_data_format = false;  // peer supports format negotiation (UBSHM)
    bool has_ece = false;          // peer hello carries ECE (RDMA v3)

    // ---- Rejection area ----
    // Filled when a codec/transport rejects; frozen wire formats map only
    // `reject_errno' onto their existing NAK encoding, while v3 (protobuf)
    // formats may also carry `reject_msg'.
    int reject_errno = 0;          // 0 = accept
    std::string reject_msg;

    // ---- Per-protocol typed slots (selected by `mode') ----
    struct RdmaInfo {              // mirrors RdmaConnectionInfo
        uint32_t block_size = 0;
        uint16_t sq_size = 0;
        uint16_t rq_size = 0;
        uint16_t lid = 0;
        uint8_t gid[16] = {0};
        uint32_t qp_num = 0;
        uint64_t ece_raw = 0;      // valid when has_ece; opaque, owned by RDMA
    } rdma;

    struct UbshmInfo {             // mirrors UBSHM HelloMessage + format ext
        uint16_t hello_ver = 0;
        uint16_t impl_ver = 0;
        uint64_t len = 0;
        char shm_name[48] = {0};   // SHM_MAX_NAME_BUFF_LEN on the wire
        int32_t data_format = 0;   // valid when has_data_format
    } ubshm;

    struct UrmaInfo {              // mirrors UrmaConnectionInfo
        uint32_t buffer_size = 0;
        uint32_t recv_buffer_cnt = 0;
        uint32_t jetty_id = 0;
        uint8_t eid[16] = {0};
        uint32_t uasid = 0;
        uint8_t tp_type = 0;
        uint8_t seg_eid[16] = {0};
        uint32_t seg_uasid = 0;
        uint64_t seg_va = 0;
        uint64_t seg_len = 0;
        uint32_t seg_token_id = 0;
    } urma;

    // ---- Opaque extension ----
    // Not interpreted by common code; private to the two endpoints.
    std::string opaque_payload;
};

// Shared empty instance for protocols that have not adopted the descriptor.
inline const HelloDescriptor& EmptyHelloDescriptor() {
    static const HelloDescriptor empty;
    return empty;
}

}  // namespace handshake
}  // namespace brpc

#endif  // BRPC_HANDSHAKE_HELLO_DESCRIPTOR_H
