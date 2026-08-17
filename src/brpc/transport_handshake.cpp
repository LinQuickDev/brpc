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

#include "brpc/transport_handshake.h"

#include <cstdint>
#include <errno.h>
#include <unistd.h>

#include "bthread/butex.h"
#include "butil/time.h"
#include "butil/logging.h"
#include "butil/object_pool.h"
#include "brpc/errno.pb.h"
#include "brpc/socket.h"

namespace brpc {
namespace handshake {

ServerHandshakeContext* ServerHandshakeContext::Create() {
    return butil::get_object<ServerHandshakeContext>();
}

void ServerHandshakeContext::Destroy() {
    butil::return_object(this);
}

static const int WAIT_TIMEOUT_MS = 50;

SocketHandshakeIO::SocketHandshakeIO(Socket* socket)
    : _socket(socket)
    , _read_butex(bthread::butex_create_checked<butil::atomic<int> >()) {
}

SocketHandshakeIO::~SocketHandshakeIO() {
    bthread::butex_destroy(_read_butex);
}

void SocketHandshakeIO::Reset(Socket* socket) {
    _socket = socket;
}

void SocketHandshakeIO::NotifyReadable() {
    _read_butex->fetch_add(1, butil::memory_order_release);
    bthread::butex_wake(_read_butex);
}

template <typename ReadOnce>
static int ReadExactLoop(butil::atomic<int>* read_butex,
                         size_t len, ReadOnce read_once) {
    size_t received = 0;
    while (received < len) {
        const int expected = read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const ssize_t nr = read_once(received, len - received);
        if (nr < 0) {
            if (errno != EAGAIN) {
                return -1;
            }
            if (bthread::butex_wait(read_butex, expected, &duetime) < 0 &&
                errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                return -1;
            }
        } else if (nr == 0) {
            errno = EEOF;
            return -1;
        } else {
            received += nr;
        }
    }
    return 0;
}

int SocketHandshakeIO::ReadExact(void* data, size_t len) {
    CHECK(data != NULL);
    CHECK(_socket != NULL);
    const int fd = _socket->fd();
    return ReadExactLoop(_read_butex, len,
        [data, fd](size_t offset, size_t remaining) {
            return read(fd, static_cast<uint8_t*>(data) + offset, remaining);
        });
}

int SocketHandshakeIO::ReadExact(butil::IOPortal* data, size_t len) {
    CHECK(data != NULL);
    CHECK(_socket != NULL);
    const int fd = _socket->fd();
    return ReadExactLoop(_read_butex, len,
        [data, fd](size_t, size_t remaining) {
            return data->append_from_file_descriptor(fd, remaining);
        });
}

template <typename WriteOnce>
static int WriteAllLoop(Socket* socket, size_t len, WriteOnce write_once) {
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const ssize_t nw = write_once(written, len - written);
        if (nw > 0) {
            written += nw;
            continue;
        }
        if (nw == 0) {
            errno = EPIPE;
            return -1;
        }
        if (errno != EAGAIN) {
            return -1;
        }
        if (socket->WaitEpollOut(socket->fd(), true, &duetime) < 0 &&
            errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

int SocketHandshakeIO::WriteAll(const void* data, size_t len) {
    CHECK(data != NULL);
    CHECK(_socket != NULL);
    const int fd = _socket->fd();
    return WriteAllLoop(_socket, len,
        [data, fd](size_t offset, size_t remaining) {
            return write(fd, static_cast<const uint8_t*>(data) + offset, remaining);
        });
}

int SocketHandshakeIO::WriteAll(butil::IOBuf* data) {
    CHECK(data != NULL);
    CHECK(_socket != NULL);
    const int fd = _socket->fd();
    return WriteAllLoop(_socket, data->size(),
        [data, fd](size_t, size_t) {
            return data->cut_into_file_descriptor(fd);
        });
}

static StepResult FinishWithFailure(
    HandshakeSession* session, const std::function<void()>& on_failed) {
    if (on_failed) {
        on_failed();
    }
    session->MarkFailed();
    return STEP_ERROR;
}

static StepResult FinishWithFallback(
    HandshakeSession* session, const std::function<void()>& set_tcp_active) {
    session->PublishFallback([&set_tcp_active]() {
        if (set_tcp_active) {
            set_tcp_active();
        }
    });
    return STEP_FALLBACK;
}

StepResult HandshakeSession::RunClient(
    const ClientHandshakeCallbacks& callbacks) {
    CHECK(callbacks.prepare_local);
    CHECK(callbacks.send_local_hello);
    CHECK(callbacks.receive_remote_hello);
    CHECK(callbacks.negotiate_resources);
    CHECK(callbacks.send_ack);
    CHECK(callbacks.set_high_speed_active);
    CHECK(callbacks.set_tcp_active);

    SetPhase(callbacks.phases.prepare_local);
    StepResult result = callbacks.prepare_local();
    if (result == STEP_FALLBACK) {
        return FinishWithFallback(this, callbacks.set_tcp_active);
    }
    if (result != STEP_OK) {
        return FinishWithFailure(this, callbacks.on_failed);
    }

    SetPhase(callbacks.phases.hello_send);
    if (callbacks.send_local_hello() != STEP_OK) {
        return FinishWithFailure(this, callbacks.on_failed);
    }

    SetPhase(callbacks.phases.hello_wait);
    result = callbacks.receive_remote_hello();
    if (result == STEP_ERROR || result == STEP_NOT_MINE ||
        result == STEP_NEED_MORE) {
        return FinishWithFailure(this, callbacks.on_failed);
    }
    bool enabled = result == STEP_OK;

    if (enabled) {
        SetPhase(callbacks.phases.negotiate);
        result = callbacks.negotiate_resources();
        if (result != STEP_OK && result != STEP_FALLBACK) {
            return FinishWithFailure(this, callbacks.on_failed);
        }
        enabled = result == STEP_OK;
    }

    SetPhase(callbacks.phases.ack_send);
    if (callbacks.send_ack(enabled) != STEP_OK) {
        return FinishWithFailure(this, callbacks.on_failed);
    }

    if (enabled) {
        callbacks.set_high_speed_active();
        MarkEstablished();
        return STEP_OK;
    }
    return FinishWithFallback(this, callbacks.set_tcp_active);
}

StepResult HandshakeSession::RunServer(
    const ServerHandshakeCallbacks& callbacks) {
    CHECK(callbacks.receive_remote_hello);
    CHECK(callbacks.prepare_local);
    CHECK(callbacks.negotiate_resources);
    CHECK(callbacks.send_local_hello);
    CHECK(callbacks.receive_ack);
    CHECK(callbacks.set_high_speed_active);
    CHECK(callbacks.set_tcp_active);

    if (phase() != callbacks.phases.ack_wait) {
        SetPhase(callbacks.phases.hello_wait);
        StepResult result = callbacks.receive_remote_hello();
        if (result == STEP_NOT_MINE) {
            if (callbacks.fallback_on_not_mine) {
                return FinishWithFallback(this, callbacks.set_tcp_active);
            }
            SetPhase(UNINITIALIZED);
            return STEP_NOT_MINE;
        }
        if (result == STEP_NEED_MORE) {
            return STEP_NEED_MORE;
        }
        if (result == STEP_ERROR) {
            return FinishWithFailure(this, callbacks.on_failed);
        }
        bool enabled = result == STEP_OK;

        if (enabled) {
            SetPhase(callbacks.phases.prepare_local);
            result = callbacks.prepare_local();
            if (result != STEP_OK && result != STEP_FALLBACK) {
                return FinishWithFailure(this, callbacks.on_failed);
            }
            enabled = result == STEP_OK;
        }

        if (enabled) {
            SetPhase(callbacks.phases.negotiate);
            result = callbacks.negotiate_resources();
            if (result != STEP_OK && result != STEP_FALLBACK) {
                return FinishWithFailure(this, callbacks.on_failed);
            }
            enabled = result == STEP_OK;
        }

        SetPhase(callbacks.phases.hello_send);
        if (callbacks.send_local_hello(enabled) != STEP_OK) {
            return FinishWithFailure(this, callbacks.on_failed);
        }
        SetPhase(callbacks.phases.ack_wait);
        if (!callbacks.blocking) {
            return STEP_NEED_MORE;
        }
    }

    StepResult result = callbacks.receive_ack();
    if (result == STEP_NEED_MORE) {
        return STEP_NEED_MORE;
    }
    if (result == STEP_ERROR || result == STEP_NOT_MINE) {
        return FinishWithFailure(this, callbacks.on_failed);
    }
    if (result == STEP_FALLBACK) {
        return FinishWithFallback(this, callbacks.set_tcp_active);
    }

    callbacks.set_high_speed_active();
    MarkEstablished();
    return STEP_OK;
}

}  // namespace handshake
}  // namespace brpc
