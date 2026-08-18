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
#include "brpc/tcp_transport.h"

namespace brpc {

void AdapterTransport::InitAdapterTransport(
    Socket* socket, const SocketOptions& options,
    const OnEdgeTrigger& default_on_edge) {
    _socket = socket;
    _default_connect = options.app_connect;
    _on_edge_trigger = options.on_edge_triggered_events;
    if (options.need_on_edge_trigger && _on_edge_trigger == NULL) {
        _on_edge_trigger = default_on_edge;
    }
    _handshake.Reset(socket);
    _tcp_transport = std::make_shared<TcpTransport>();
    _tcp_transport->Init(socket, options);
}

void AdapterTransport::ResetAdapterTransport() {
    _handshake.Reset(_socket);
}

int AdapterTransport::CutFromIOBuf(butil::IOBuf* buf) {
    if (_handshake.phase() == handshake::ESTABLISHED) {
        butil::IOBuf* data[1] = {buf};
        return CutFromHighSpeedIOBufList(data, 1);
    }
    return _tcp_transport->CutFromIOBuf(buf);
}

ssize_t AdapterTransport::CutFromIOBufList(
    butil::IOBuf** buf, size_t ndata) {
    if (_handshake.phase() == handshake::ESTABLISHED) {
        return CutFromHighSpeedIOBufList(buf, ndata);
    }
    return _tcp_transport->CutFromIOBufList(buf, ndata);
}

int AdapterTransport::WaitEpollOut(butil::atomic<int>* epollout_butex,
                                    bool pollin, timespec duetime) {
    if (_handshake.phase() == handshake::ESTABLISHED) {
        return WaitHighSpeedEpollOut(epollout_butex, pollin, duetime);
    }
    return _tcp_transport->WaitEpollOut(epollout_butex, pollin, duetime);
}

void AdapterTransport::FallbackToTcp() {
    _handshake.PublishFallback([this]() {
        SetHighSpeedAvailable(false);
    });
}

void AdapterTransport::OnNewDataFromTcp(Socket* socket) {
    static_cast<AdapterTransport*>(socket->_transport.get())->ProcessTcpEvent();
}

void AdapterTransport::ProcessTcpEvent() {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const int phase = _handshake.phase();
        if (phase == handshake::UNINITIALIZED) {
            if (!_socket->CreatedByConnect()) {
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
