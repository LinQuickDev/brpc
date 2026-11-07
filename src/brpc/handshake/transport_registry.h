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

#ifndef BRPC_HANDSHAKE_TRANSPORT_REGISTRY_H
#define BRPC_HANDSHAKE_TRANSPORT_REGISTRY_H

#include <cstddef>
#include <vector>

#include "brpc/handshake/handshake_frame.h"
#include "brpc/socket_mode.h"

namespace brpc {

class Socket;

namespace handshake {

class HandshakeAdapter;
class HandshakeProtocol;

// One transport's registration entry. A transport self-registers a single
// entry from its own translation unit (static initialization); common code
// walks the registry and never contains a per-transport switch.
//
// Two channels, generalizing the urma_fallback_handshake pattern:
//   - recognition-only: always linked. Lets a node without the transport
//     library recognize the peer's hello and NAK so the peer falls back to
//     TCP cleanly.
//   - full: linked only when the transport is available. Provides the
//     server-side adapters that drive real upgrades.
struct TransportHandshakeEntry {
    SocketMode mode = SOCKET_MODE_TCP;  // unique within the registry
    const char* name = nullptr;         // "rdma"/"ubshm"/...; logs and bvar

    // ---- recognition-only channel (always compiled) ----
    // Hello frame specs for server-side magic dispatch, in preference order
    // (e.g. v3 before v2).
    const FrameSpec* recognize_specs = nullptr;
    size_t recognize_spec_count = 0;
    // Creates a NAK-only protocol (typically a FallbackHandshakeProtocol
    // subclass) used when the full transport is unavailable. May be null
    // when the full channel is always present.
    HandshakeProtocol* (*create_recognizer)() = nullptr;

    // ---- full channel (compiled only with the transport) ----
    // Creates the per-version server adapters for one accepted connection.
    // Null when this process has no transport support.
    std::vector<HandshakeAdapter*> (*create_server_adapters)(Socket*) = nullptr;
};

class TransportHandshakeRegistry {
public:
    // Self-registration at static-init time. Duplicate modes are a
    // programming error and CHECK-fail. Returns 0 for static dummies.
    static int Register(const TransportHandshakeEntry& entry);

    static const TransportHandshakeEntry* Find(SocketMode mode);

    // Finds the entry whose hello magic fully matches the leading bytes of
    // `data'. Returns nullptr until a complete magic is visible or when no
    // entry matches (a TCP-only peer).
    static const TransportHandshakeEntry* FindByMagic(
        const char* data, size_t len);

    // Snapshot of all entries (registration order).
    static std::vector<TransportHandshakeEntry> All();

    // Test-only: drop every entry for suite isolation.
    static void ResetForTest();
};

}  // namespace handshake
}  // namespace brpc

#endif  // BRPC_HANDSHAKE_TRANSPORT_REGISTRY_H
