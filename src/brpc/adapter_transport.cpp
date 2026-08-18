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

#include "brpc/adapter_transport.h"

#include <cstdint>
#include <errno.h>
#include <unistd.h>

#include "brpc/input_messenger.h"
#include "brpc/rdma_transport.h"
#include "brpc/tcp_transport.h"
#include "brpc/ubshm_transport.h"

namespace brpc {

AdapterTransport::~AdapterTransport() = default;

AdapterTransport* AdapterTransport::Get(Socket* socket) {
    CHECK(socket != NULL);
    return static_cast<AdapterTransport*>(socket->_transport.get());
}

const AdapterTransport* AdapterTransport::Get(const Socket* socket) {
    CHECK(socket != NULL);
    return static_cast<const AdapterTransport*>(socket->_transport.get());
}

void AdapterTransport::Init(Socket* socket, const SocketOptions& options) {
    CHECK_EQ(_mode, options.socket_mode);
    _socket = socket;
    _default_connect = options.app_connect;
    _on_edge_trigger = options.on_edge_triggered_events;
    if (options.need_on_edge_trigger && _on_edge_trigger == NULL) {
        if (_mode == SOCKET_MODE_TCP) {
            _on_edge_trigger = InputMessenger::OnNewMessages;
#if BRPC_WITH_RDMA
        } else if (_mode == SOCKET_MODE_RDMA &&
                   options.user != static_cast<SocketUser*>(
                       get_client_side_messenger())) {
            // RDMA server handshake is parsed by InputMessenger.
            _on_edge_trigger = InputMessenger::OnNewMessages;
#endif
        } else {
            _on_edge_trigger = OnNewDataFromTcp;
        }
    }
    _handshake.Reset(socket);
    _tcp_transport.reset(new TcpTransport);
    _tcp_transport->Init(socket, options);

    switch (_mode) {
#if BRPC_WITH_RDMA
    case SOCKET_MODE_RDMA:
        _high_speed_transport.reset(new RdmaTransport);
        break;
#endif
#if BRPC_WITH_UBRING
    case SOCKET_MODE_UBRING:
        _high_speed_transport.reset(new UBShmTransport);
        break;
#endif
    default:
        break;
    }
    if (_high_speed_transport) {
        _high_speed_transport->Init(socket, options);
    }
}

void AdapterTransport::Release() {
    if (_high_speed_transport) {
        _high_speed_transport->Release();
    }
    _tcp_transport->Release();
}

int AdapterTransport::Reset(int32_t expected_nref) {
    if (_high_speed_transport) {
        _high_speed_transport->Reset(expected_nref);
    }
    _tcp_transport->Reset(expected_nref);
    _handshake.Reset(_socket);
    return 0;
}

std::shared_ptr<AppConnect> AdapterTransport::Connect() {
    if (_high_speed_transport) {
        return _high_speed_transport->Connect();
    }
    return _tcp_transport->Connect();
}

Transport* AdapterTransport::ActiveTransport() const {
    if (_high_speed_transport &&
        _handshake.phase() == handshake::ESTABLISHED) {
        return _high_speed_transport.get();
    }
    return _tcp_transport.get();
}

int AdapterTransport::CutFromIOBuf(butil::IOBuf* buf) {
    return ActiveTransport()->CutFromIOBuf(buf);
}

ssize_t AdapterTransport::CutFromIOBufList(
    butil::IOBuf** buf, size_t ndata) {
    return ActiveTransport()->CutFromIOBufList(buf, ndata);
}

int AdapterTransport::WaitEpollOut(butil::atomic<int>* epollout_butex,
                                    bool pollin, timespec duetime) {
    return ActiveTransport()->WaitEpollOut(
        epollout_butex, pollin, duetime);
}

void AdapterTransport::ProcessEvent(bthread_attr_t attr) {
    ActiveTransport()->ProcessEvent(attr);
}

void AdapterTransport::QueueMessage(InputMessageClosure& input_msg,
                                    int* num_bthread_created,
                                    bool last_msg) {
    ActiveTransport()->QueueMessage(
        input_msg, num_bthread_created, last_msg);
}

void AdapterTransport::Debug(std::ostream& os) {
    if (_high_speed_transport) {
        _high_speed_transport->Debug(os);
    }
}

void AdapterTransport::FallbackToTcp() {
    _handshake.PublishFallback([this]() {
        SetHighSpeedAvailable(false);
    });
}

void AdapterTransport::SetHighSpeedAvailable(bool available) {
    if (!_high_speed_transport) {
        return;
    }
    switch (_mode) {
#if BRPC_WITH_RDMA
    case SOCKET_MODE_RDMA:
        static_cast<RdmaTransport*>(_high_speed_transport.get())
            ->SetHighSpeedAvailable(available);
        break;
#endif
#if BRPC_WITH_UBRING
    case SOCKET_MODE_UBRING:
        static_cast<UBShmTransport*>(_high_speed_transport.get())
            ->SetHighSpeedAvailable(available);
        break;
#endif
    default:
        break;
    }
}

void AdapterTransport::StartServerHandshake() {
    if (!_high_speed_transport) {
        return;
    }
    switch (_mode) {
#if BRPC_WITH_UBRING
    case SOCKET_MODE_UBRING:
        static_cast<UBShmTransport*>(_high_speed_transport.get())
            ->StartServerHandshake();
        break;
#endif
    default:
        // RDMA uses the standard InputMessenger protocol parser.
        break;
    }
}

void AdapterTransport::OnNewDataFromTcp(Socket* socket) {
    static_cast<AdapterTransport*>(socket->_transport.get())->ProcessTcpEvent();
}

void AdapterTransport::ProcessTcpEvent() {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const int phase = _handshake.phase();
        if (phase == handshake::UNINITIALIZED) {
            if (!_socket->CreatedByConnect() && _high_speed_transport) {
                StartServerHandshake();
                if (_handshake.phase() != handshake::UNINITIALIZED) {
                    continue;
                }
            }
        } else if (phase < handshake::ESTABLISHED) {
            _handshake.NotifyReadable();
        } else if (phase == handshake::FALLBACK_TCP) {
            InputMessenger::OnNewMessages(_socket);
            return;
        } else if (phase == handshake::ESTABLISHED) {
            CheckUnexpectedTcpData();
            return;
        }
        if (!_socket->MoreReadEvents(&progress)) {
            break;
        }
    }
}

void AdapterTransport::CheckUnexpectedTcpData() {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        uint8_t byte;
        const ssize_t nr = read(_socket->fd(), &byte, 1);
        if (nr == 0) {
            _socket->SetEOF();
            return;
        }
        if (nr > 0) {
            _socket->SetFailed(EPROTO, "Read unexpected data from %s",
                               _socket->description().c_str());
            return;
        }
        if (errno != EAGAIN) {
            const int saved_errno = errno;
            _socket->SetFailed(saved_errno, "Fail to read from %s: %s",
                               _socket->description().c_str(),
                               berror(saved_errno));
            return;
        }
        if (!_socket->MoreReadEvents(&progress)) {
            return;
        }
    }
}

void AdapterTransport::TryReadOnTcp() {
    if (_socket->_nevent.fetch_add(1, butil::memory_order_acq_rel) != 0) {
        return;
    }
    const int phase = _handshake.phase();
    if (phase == handshake::FALLBACK_TCP) {
        InputMessenger::OnNewMessages(_socket);
    } else if (phase == handshake::ESTABLISHED) {
        CheckUnexpectedTcpData();
    }
}

}  // namespace brpc
