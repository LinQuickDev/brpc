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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <errno.h>
#include <unistd.h>

#include "brpc/input_messenger.h"
#include "brpc/destroyable.h"
#include "brpc/handshake/handshake_adapter.h"
#include "brpc/handshake/transport_registry.h"
#include "brpc/tcp_transport.h"

namespace brpc {

namespace {

class AdapterConnect : public AppConnect {
public:
    explicit AdapterConnect(const std::shared_ptr<AppConnect>& app_connect)
        : _app_connect(app_connect) {}

    static std::shared_ptr<AppConnect> Wrap(
        const std::shared_ptr<AppConnect>& app_connect) {
        if (std::dynamic_pointer_cast<AdapterConnect>(app_connect)) {
            return app_connect;
        }
        return std::make_shared<AdapterConnect>(app_connect);
    }

    static std::shared_ptr<AppConnect> Unwrap(
        const std::shared_ptr<AppConnect>& app_connect) {
        const std::shared_ptr<AdapterConnect> adapter =
            std::dynamic_pointer_cast<AdapterConnect>(app_connect);
        return adapter ? adapter->_app_connect : app_connect;
    }

    void StartConnect(const Socket* socket,
                      void (*done)(int, void*), void* data) override {
        ApplicationConnectTask* task = new ApplicationConnectTask{
            socket, _app_connect, done, data};
        if (AdapterTransport::StartClientUpgrade(
                socket, OnUpgradeComplete, task) != 0) {
            const int error = errno != 0 ? errno : EAGAIN;
            AdapterTransport::Get(const_cast<Socket*>(socket))->CompleteConnection(
                handshake::FAILED);
            delete task;
            done(error, data);
        }
    }

    void StopConnect(Socket* socket) override {
        if (_app_connect) {
            _app_connect->StopConnect(socket);
        }
    }

private:
    struct ApplicationConnectTask {
        const Socket* socket;
        std::shared_ptr<AppConnect> app_connect;
        void (*done)(int, void*);
        void* data;
    };

    static void OnApplicationComplete(int error, void* arg) {
        std::unique_ptr<ApplicationConnectTask> task(
            static_cast<ApplicationConnectTask*>(arg));
        task->done(error, task->data);
    }

    static void OnUpgradeComplete(int error, void* arg) {
        ApplicationConnectTask* task =
            static_cast<ApplicationConnectTask*>(arg);
        if (error != 0 || !task->app_connect) {
            std::unique_ptr<ApplicationConnectTask> owned(task);
            task->done(error, task->data);
            return;
        }
        task->app_connect->StartConnect(
            task->socket, OnApplicationComplete, task);
    }

    std::shared_ptr<AppConnect> _app_connect;
};

struct ClientHandshakeTask {
    AdapterTransport* adapter;
    void (*done)(int, void*);
    void* data;
    SocketUniquePtr socket;
};

}  // namespace

AdapterTransport::AdapterTransport(SocketMode mode)
    : _mode(mode), _connection_completed(0) {}
AdapterTransport::~AdapterTransport() = default;

AdapterTransport* AdapterTransport::Get(Socket* socket) {
    CHECK(socket != nullptr);
    return static_cast<AdapterTransport*>(socket->_transport.get());
}

const AdapterTransport* AdapterTransport::Get(const Socket* socket) {
    CHECK(socket != nullptr);
    return static_cast<const AdapterTransport*>(socket->_transport.get());
}

bool AdapterTransport::upgrade_capable(SocketMode mode) const {
    if (_mode != mode || _high_speed_transport == nullptr) {
        return false;
    }
    const handshake::TransportHandshakeEntry* entry =
        handshake::TransportHandshakeRegistry::Find(mode);
    return entry != nullptr && entry->upgrade_ready != nullptr &&
           entry->upgrade_ready(_high_speed_transport.get());
}

int AdapterTransport::StartClientUpgrade(const Socket* socket,
                                         void (*done)(int, void*),
                                         void* data) {
    return StartClientUpgradeImpl(socket, done, data, bthread_start_background);
}

int AdapterTransport::StartClientUpgradeImpl(
    const Socket* socket, void (*done)(int, void*), void* data,
    StartBackground start_background) {
    AdapterTransport* adapter = Get(const_cast<Socket*>(socket));
    ClientHandshakeTask* task = new ClientHandshakeTask{adapter, done, data, SocketUniquePtr()};
    if (Socket::Address(socket->id(), &task->socket) != 0) {
        delete task;
        errno = EFAILEDSOCKET;
        return -1;
    }
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "StartClientUpgrade");
    const int rc = start_background(&tid, &attr, ProcessClientHandshake, task);
    if (rc != 0) {
        delete task;
        errno = rc;
        return -1;
    }
    return 0;
}

ParseResult AdapterTransport::ProcessUpgradeReadable(butil::IOBuf* source) {
    if (_handshake.phase(butil::memory_order_acquire) ==
        handshake::ESTABLISHED) {
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }
    ParseResult result(PARSE_ERROR_NOT_ENOUGH_DATA);
    if (_socket->parsing_context() != nullptr) {
        handshake::ServerHandshakeContext* context =
            static_cast<handshake::ServerHandshakeContext*>(
                _socket->parsing_context());
        CHECK(context->adapter() != nullptr);
        result = context->adapter()->ExecuteServerHandshake(source, _socket);
    } else if (!source->empty()) {
        static const size_t MAX_MAGIC_LEN = 4;
        char prefix[MAX_MAGIC_LEN] = {};
        const size_t prefix_len = std::min(source->size(), MAX_MAGIC_LEN);
        source->copy_to(prefix, prefix_len);

        // Magic dispatch is fully driven by the transport registry: each
        // transport self-registers its hello magics (recognition-only
        // channel) and its server adapter. UBSHM only registers in
        // UBSHM-enabled builds, so elsewhere a "UB" prefix falls through to
        // the other parsers, matching master's legacy behavior; URMA/RDMA
        // always register so a TCP-only server still replies with a
        // disabled hello and the peer falls back.
        const handshake::TransportHandshakeEntry* entry =
            handshake::TransportHandshakeRegistry::FindByMagic(
                prefix, prefix_len);
        if (entry != nullptr) {
            result = entry->server_adapter()->ExecuteServerHandshake(
                source, _socket);
        } else if (handshake::TransportHandshakeRegistry::CouldMatchMagic(
                       prefix, prefix_len)) {
            // An incomplete magic: wait for more bytes before dispatching.
            result = ParseResult(PARSE_ERROR_NOT_ENOUGH_DATA);
        } else {
            result = ParseResult(PARSE_ERROR_TRY_OTHERS);
        }
    }
    const int phase = _handshake.phase(butil::memory_order_acquire);
    if (!connection_completed() &&
        (phase == handshake::ESTABLISHED ||
         phase == handshake::FALLBACK_TCP || phase == handshake::FAILED)) {
        CompleteConnection(static_cast<handshake::Phase>(phase));
    }
    return result;
}

void AdapterTransport::CompleteConnection(handshake::Phase terminal_phase) {
    CHECK(terminal_phase == handshake::ESTABLISHED ||
          terminal_phase == handshake::FALLBACK_TCP ||
          terminal_phase == handshake::FAILED);
    if (terminal_phase == handshake::FAILED &&
        _handshake.phase(butil::memory_order_acquire) != handshake::FAILED) {
        _handshake.MarkFailed();
    }
    int expected = 0;
    _connection_completed.compare_exchange_strong(
        expected, 1, butil::memory_order_release,
        butil::memory_order_relaxed);
}

void* AdapterTransport::ProcessClientHandshake(void* arg) {
    std::unique_ptr<ClientHandshakeTask> task(
        static_cast<ClientHandshakeTask*>(arg));
    AdapterTransport* adapter = task->adapter;
    Socket* socket = task->socket.get();
    int connect_error = 0;

    const handshake::TransportHandshakeEntry* entry =
        handshake::TransportHandshakeRegistry::Find(adapter->_mode);
    if (entry != nullptr && entry->run_client_upgrade != nullptr) {
        entry->run_client_upgrade(adapter, socket, &connect_error);
        task->done(connect_error, task->data);
        return nullptr;
    }

    socket->SetFailed(EPROTO, "Unsupported client transport handshake");
    adapter->CompleteConnection(handshake::FAILED);
    task->done(EPROTO, task->data);
    return nullptr;
}

void AdapterTransport::Init(Socket* socket, const SocketOptions& options) {
    CHECK_EQ(_mode, options.socket_mode);
    _socket = socket;
    _default_connect = options.app_connect;
    _on_edge_trigger = options.on_edge_triggered_events;
    if (options.need_on_edge_trigger && _on_edge_trigger == nullptr) {
        if (_mode == SOCKET_MODE_TCP) {
            _on_edge_trigger = InputMessenger::OnNewMessages;
        } else if (options.user != static_cast<SocketUser*>(
                       get_client_side_messenger())) {
            // High-speed server handshakes are parsed by InputMessenger.
            _on_edge_trigger = OnNewMessagesAfterUpgrade;
        } else {
            _on_edge_trigger = OnNewDataFromTcp;
        }
    }
    _handshake.Reset(socket);
    _connection_completed.store(0, butil::memory_order_relaxed);
    _tcp_transport.reset(new TcpTransport);
    _tcp_transport->Init(socket, options);

    const handshake::TransportHandshakeEntry* entry =
        handshake::TransportHandshakeRegistry::Find(_mode);
    if (entry != nullptr && entry->create_transport != nullptr) {
        _high_speed_transport.reset(entry->create_transport());
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
    _connection_completed.store(0, butil::memory_order_relaxed);
    return 0;
}

std::shared_ptr<AppConnect> AdapterTransport::Connect() {
    if (upgrade_capable(_mode)) {
        return AdapterConnect::Wrap(_default_connect);
    }
    SocketUser* const client_messenger =
        static_cast<SocketUser*>(get_client_side_messenger());
    if (client_messenger != nullptr &&
        _socket->user() == client_messenger) {
        // This socket keeps the unwrapped AppConnect across health-check
        // resets, so its read path must not depend on another handshake.
        _on_edge_trigger = _tcp_transport->GetOnEdgeTrigger();
        FallbackToTcp();
        return AdapterConnect::Unwrap(_tcp_transport->Connect());
    }
    return _tcp_transport->Connect();
}

Transport* AdapterTransport::ActiveTransport() const {
    if (_high_speed_transport &&
        _handshake.phase(butil::memory_order_acquire) ==
            handshake::ESTABLISHED) {
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
    const char* state = "UNKNOWN";
    switch (_handshake.phase(butil::memory_order_acquire)) {
    case handshake::UNINITIALIZED: state = "UNINITIALIZED"; break;
    case handshake::PREPARING: state = "PREPARING"; break;
    case handshake::HELLO_SEND: state = "HELLO_SEND"; break;
    case handshake::HELLO_WAIT: state = "HELLO_WAIT"; break;
    case handshake::NEGOTIATING: state = "NEGOTIATING"; break;
    case handshake::ACK_SEND: state = "ACK_SEND"; break;
    case handshake::ACK_WAIT: state = "ACK_WAIT"; break;
    case handshake::EXTENSION_SEND: state = "EXTENSION_SEND"; break;
    case handshake::EXTENSION_WAIT: state = "EXTENSION_WAIT"; break;
    case handshake::ESTABLISHED: state = "ESTABLISHED"; break;
    case handshake::FALLBACK_TCP: state = "FALLBACK_TCP"; break;
    case handshake::FAILED: state = "FAILED"; break;
    }
    os << "\nhandshake_state=" << state
       << "\nhandshake_version=" << _handshake.protocol_version();
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
    const handshake::TransportHandshakeEntry* entry =
        handshake::TransportHandshakeRegistry::Find(_mode);
    if (entry != nullptr && entry->set_high_speed_available != nullptr) {
        entry->set_high_speed_available(_high_speed_transport.get(),
                                        available);
    }
}

void AdapterTransport::OnNewMessagesAfterUpgrade(Socket* socket) {
    AdapterTransport* adapter = Get(socket);
    if (adapter->_handshake.phase(butil::memory_order_acquire) ==
        handshake::ESTABLISHED) {
        adapter->CheckUnexpectedTcpData();
        return;
    }

    InputMessenger::OnNewMessagesUntil(socket, [](Socket* s) {
        return Get(s)->handshake_phase() == handshake::ESTABLISHED;
    });

    const handshake::TransportHandshakeEntry* entry =
        handshake::TransportHandshakeRegistry::Find(adapter->_mode);
    if (entry != nullptr && entry->start_upgrade_events != nullptr &&
        adapter->_handshake.phase(butil::memory_order_acquire) ==
            handshake::ESTABLISHED) {
        int saved_errno = 0;
        if (entry->start_upgrade_events(
                adapter->_high_speed_transport.get(), &saved_errno) < 0) {
            adapter->_handshake.MarkFailed();
            adapter->CompleteConnection(handshake::FAILED);
            socket->SetFailed(saved_errno,
                              "Fail to start %s upgrade events from %s: %s",
                              entry->name, socket->description().c_str(),
                              berror(saved_errno));
        }
    }
    if (adapter->handshake_phase() == handshake::ESTABLISHED) {
        adapter->CheckUnexpectedTcpData();
    }
}

void AdapterTransport::OnNewDataFromTcp(Socket* socket) {
    static_cast<AdapterTransport*>(socket->_transport.get())->ProcessTcpEvent();
}

void AdapterTransport::ProcessTcpEvent() {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const int phase = _handshake.phase(butil::memory_order_acquire);
        if (phase != handshake::UNINITIALIZED &&
            phase < handshake::ESTABLISHED) {
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
        if (errno == EINTR) {
            continue;
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
    const int phase = _handshake.phase(butil::memory_order_acquire);
    if (phase == handshake::FALLBACK_TCP) {
        InputMessenger::OnNewMessages(_socket);
    } else if (phase == handshake::ESTABLISHED) {
        CheckUnexpectedTcpData();
    }
}

}  // namespace brpc
