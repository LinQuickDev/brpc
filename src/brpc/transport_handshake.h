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
#include <functional>

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
// terminal values are shared so that UpgradeTransport can make the same
// acquire-side decision for RDMA, URMA and UBSHM.
enum Phase {
    UNINITIALIZED = 0,
    NEGOTIATING = 1,
    ESTABLISHED = 0x100,
    FALLBACK_TCP = 0x200,
    FAILED = 0x300,
};

enum StepResult {
    STEP_OK = 0,
    STEP_FALLBACK,
    STEP_NEED_MORE,
    STEP_NOT_MINE,
    STEP_ERROR,
};

struct HandshakePhases {
    int prepare_local;
    int hello_send;
    int hello_wait;
    int negotiate;
    int ack_send;
    int ack_wait;
};

// Protocol-specific operations invoked by the common client driver. A
// callback reports what happened but does not advance the session phase.
struct ClientHandshakeCallbacks {
    HandshakePhases phases;
    std::function<StepResult()> prepare_local;
    std::function<StepResult()> send_local_hello;
    std::function<StepResult()> receive_remote_hello;
    std::function<StepResult()> negotiate_resources;
    std::function<StepResult(bool)> send_ack;
    std::function<void()> set_high_speed_active;
    std::function<void()> set_tcp_active;
    std::function<void()> on_failed;
};

// The server driver can operate incrementally (InputMessenger parser) or run
// through ACK_WAIT in one blocking handshake bthread.
struct ServerHandshakeCallbacks {
    HandshakePhases phases;
    bool blocking;
    bool fallback_on_not_mine;
    std::function<StepResult()> receive_remote_hello;
    std::function<StepResult()> prepare_local;
    std::function<StepResult()> negotiate_resources;
    std::function<StepResult(bool)> send_local_hello;
    std::function<StepResult()> receive_ack;
    std::function<void()> set_high_speed_active;
    std::function<void()> set_tcp_active;
    std::function<void()> on_failed;
};

// Owns TCP-fd I/O and its readable notification while upgrading a connection.
// The handshake lifecycle is owned by HandshakeSession below.
class SocketHandshakeIO {
public:
    explicit SocketHandshakeIO(Socket* socket = NULL);
    ~SocketHandshakeIO();

    void Reset(Socket* socket);

    void NotifyReadable();

    // Read/write exactly len bytes. EAGAIN is handled by waiting for the
    // socket event callback; EOF is reported as EEOF.
    int ReadExact(void* data, size_t len);
    int ReadExact(butil::IOPortal* data, size_t len);
    int WriteAll(const void* data, size_t len);
    int WriteAll(butil::IOBuf* data);

private:
    Socket* _socket;
    butil::atomic<int>* _read_butex;

    DISALLOW_COPY_AND_ASSIGN(SocketHandshakeIO);
};

// Owns one connection-upgrade attempt. Wire codecs and endpoint resource
// operations remain protocol adapters, while this class provides the common
// lifecycle, publication ordering and TCP control-plane I/O.
class HandshakeSession {
public:
    explicit HandshakeSession(Socket* socket = NULL)
        : _io(socket), _phase(UNINITIALIZED), _protocol_version(0) {}

    void Reset(Socket* socket) {
        _io.Reset(socket);
        _protocol_version = 0;
        _phase.store(UNINITIALIZED, butil::memory_order_relaxed);
    }

    int phase(butil::memory_order order = butil::memory_order_acquire) const {
        return _phase.load(order);
    }

    void SetPhase(int phase) {
        _phase.store(phase, butil::memory_order_relaxed);
    }

    int protocol_version() const { return _protocol_version; }
    void set_protocol_version(int version) { _protocol_version = version; }

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

    SocketHandshakeIO* io() { return &_io; }
    const SocketHandshakeIO* io() const { return &_io; }
    void NotifyReadable() { _io.NotifyReadable(); }

    StepResult RunClient(const ClientHandshakeCallbacks& callbacks);
    StepResult RunServer(const ServerHandshakeCallbacks& callbacks);

private:
    SocketHandshakeIO _io;
    butil::atomic<int> _phase;
    int _protocol_version;

    DISALLOW_COPY_AND_ASSIGN(HandshakeSession);
};

}  // namespace handshake
}  // namespace brpc

#endif  // BRPC_TRANSPORT_HANDSHAKE_H
