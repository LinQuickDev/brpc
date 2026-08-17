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

#ifndef BRPC_TRANSPORT_HANDSHAKE_H
#define BRPC_TRANSPORT_HANDSHAKE_H

#include <cstddef>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/macros.h"
#include "brpc/destroyable.h"

namespace brpc {

class Socket;

namespace handshake {

// Marker context retained by InputMessenger between the hello and ACK parse
// calls. It is wire-format agnostic and reusable by every upgrade protocol.
struct ServerHandshakeContext : public Destroyable {
    static ServerHandshakeContext* Create();
    void Destroy() override;
};

// Protocol adapters may use transport-specific intermediate values, but the
// terminal values are shared so that the TCP event callback can make the same
// acquire-side decision for RDMA, URMA and UBSHM.
enum Phase {
    UNINITIALIZED = 0,
    NEGOTIATING = 1,
    ESTABLISHED = 0x100,
    FALLBACK_TCP = 0x200,
    FAILED = 0x300,
};

// Owns all TCP-fd I/O and synchronization used while upgrading a connection.
// High-speed endpoints deliberately do not own this object: they only prepare
// and activate protocol-specific data-plane resources.
class SocketHandshakeIO {
public:
    explicit SocketHandshakeIO(Socket* socket = NULL);
    ~SocketHandshakeIO();

    void Reset(Socket* socket);

    int phase(butil::memory_order order = butil::memory_order_acquire) const {
        return _phase.load(order);
    }

    void SetPhase(int phase) {
        _phase.store(phase, butil::memory_order_relaxed);
    }

    void MarkEstablished() {
        _phase.store(ESTABLISHED, butil::memory_order_release);
    }

    void MarkFailed() {
        _phase.store(FAILED, butil::memory_order_release);
    }

    // The callback MUST publish the transport's TCP-active state. The release
    // store then makes that state and any pushed-back bytes visible to the
    // event thread that observes FALLBACK_TCP with an acquire load. This is
    // the common form of the ordering fixes from #3347 and #3406.
    template <typename PublishTcpActive>
    void PublishFallback(PublishTcpActive publish_tcp_active) {
        publish_tcp_active();
        _phase.store(FALLBACK_TCP, butil::memory_order_release);
    }

    void NotifyReadable();

    // Read/write exactly len bytes. EAGAIN is handled by waiting for the
    // socket event callback; EOF is reported as EEOF.
    int ReadExact(void* data, size_t len);
    int ReadExact(butil::IOPortal* data, size_t len);
    int WriteAll(const void* data, size_t len);
    int WriteAll(butil::IOBuf* data);

private:
    Socket* _socket;
    butil::atomic<int> _phase;
    butil::atomic<int>* _read_butex;

    DISALLOW_COPY_AND_ASSIGN(SocketHandshakeIO);
};

}  // namespace handshake
}  // namespace brpc

#endif  // BRPC_TRANSPORT_HANDSHAKE_H
