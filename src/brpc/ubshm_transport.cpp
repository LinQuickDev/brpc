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

#if BRPC_WITH_UBRING

#include <cstring>

#include "brpc/ubshm_transport.h"
#include "brpc/adapter_transport.h"
#include "brpc/errno.pb.h"
#include "brpc/handshake/ubshm_handshake.h"
#include "brpc/ubshm/common/common.h"
#include "brpc/ubshm/ub_endpoint.h"
#include "brpc/ubshm/ub_helper.h"
#include "brpc/ubshm/ubr_trx.h"

namespace brpc {
DECLARE_bool(usercode_in_coroutine);
DECLARE_bool(usercode_in_pthread);

extern SocketVarsCollector *g_vars;

namespace ubring {

void UBConnect::StartConnect(const Socket* socket,
                             void (*done)(int err, void* data),
                             void* data) {
    UBShmTransport* transport = UBShmTransport::Get(socket);
    CHECK(transport->_ub_ep != NULL);
    SocketUniquePtr ptr;
    if (Socket::Address(socket->id(), &ptr) != 0) {
        return;
    }
    if (!IsUBAvailable()) {
        transport->FallbackToTcp();
        done(0, data);
        return;
    }
    _done = done;
    _data = data;
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UBProcessHandshakeAtClient");
    if (bthread_start_background(
            &tid, &attr, UBShmTransport::ProcessHandshakeAtClient,
            transport) < 0) {
        LOG(FATAL) << "Fail to start handshake bthread";
        Run();
    } else {
        ptr.release();
    }
}

void UBConnect::StopConnect(Socket*) {}

void UBConnect::Run() {
    _done(errno, _data);
}

}  // namespace ubring

UBShmTransport* UBShmTransport::Get(const Socket* socket) {
    const AdapterTransport* adapter = AdapterTransport::Get(socket);
    Transport* transport = adapter->high_speed_transport();
    CHECK(transport != NULL);
    return static_cast<UBShmTransport*>(transport);
}

AdapterTransport* UBShmTransport::adapter_transport() const {
    return AdapterTransport::Get(_socket);
}

handshake::HandshakeSession* UBShmTransport::handshake_session() const {
    return adapter_transport()->handshake_session();
}

int UBShmTransport::handshake_phase() const {
    return adapter_transport()->handshake_phase();
}

int UBShmTransport::handshake_version() const {
    return adapter_transport()->handshake_version();
}

void UBShmTransport::FallbackToTcp() {
    adapter_transport()->FallbackToTcp();
}

void* UBShmTransport::ProcessHandshakeAtClient(void* arg) {
    UBShmTransport* transport = static_cast<UBShmTransport*>(arg);
    ubring::UBShmEndpoint* ep = transport->_ub_ep;
    SocketUniquePtr socket(ep->_socket);
    ubring::UBConnect::RunGuard guard(
        static_cast<ubring::UBConnect*>(socket->_app_connect.get()));

    LOG_IF(INFO, ubring::FLAGS_ub_trace_verbose)
        << "Start handshake on " << socket->_local_side;

    const size_t local_shm_len =
        static_cast<size_t>(ubring::FLAGS_data_queue_size) * MB_TO_BYTE;
    ubring::SHM local_trx_shm = {
        NULL, local_shm_len, 0, {0},
        static_cast<uint32_t>(socket->fd())};
    const auto shm_name_str = butil::endpoint2str(socket->local_side());
    const char* const shm_name = shm_name_str.c_str();
    ubring::HelloMessage remote{};
    ubring::UBShmHandshakeAdapter wire;

    handshake::ClientHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{
        C_ALLOC_SHM, C_HELLO_SEND, C_HELLO_WAIT,
        C_MAP_REMOTE_SHM, C_ACK_SEND, 0};
    callbacks.codec = wire.MakeCodec();
    callbacks.codec.build_hello = [&](bool enabled, std::string* payload) {
        CHECK(enabled);
        const handshake::StepResult result = wire.BuildHello(
            true, local_shm_len, local_trx_shm.name, payload);
        if (result == handshake::STEP_OK) {
            ubring::HelloMessage local{};
            local.Deserialize(payload->data());
            LOG_IF(INFO, ubring::FLAGS_ub_trace_verbose)
                << "client handshake message : " << local.toString();
        }
        return result;
    };
    callbacks.codec.parse_hello = [&](const std::string& payload) {
        const handshake::StepResult result = wire.ParseHello(payload, &remote);
        if (result == handshake::STEP_FALLBACK) {
            LOG(WARNING)
                << "Fail to negotiate with server, fallback to tcp:"
                << socket->description();
        }
        return result;
    };
    callbacks.prepare_resources = [&]() {
        if (ep->AllocateClientResources(&local_trx_shm, shm_name) == 0) {
            return handshake::STEP_OK;
        }
        LOG(WARNING) << "Fallback to tcp:" << socket->description();
        errno = 0;
        return handshake::STEP_FALLBACK;
    };
    callbacks.negotiate_resources = [&]() {
        if (ep->_ub_ring->UbrMapRemoteShm(
                &local_trx_shm, shm_name) == 0) {
            return handshake::STEP_OK;
        }
        LOG(WARNING) << "Fail to map the remote shm, fallback to tcp:"
                     << socket->description();
        return handshake::STEP_FALLBACK;
    };
    callbacks.set_high_speed_active = [transport]() {
        transport->SetHighSpeedAvailable(true);
    };
    callbacks.set_tcp_active = [transport]() {
        transport->SetHighSpeedAvailable(false);
    };
    callbacks.on_failed = [&]() {
        const int saved_errno = errno != 0 ? errno : EPROTO;
        socket->SetFailed(saved_errno,
                          "Fail to complete ubring handshake from %s: %s",
                          socket->description().c_str(), berror(saved_errno));
    };

    const handshake::StepResult result =
        transport->handshake_session()->RunClient(callbacks);
    if (result == handshake::STEP_OK) {
        ep->_ub_ring->UbrUnlinkLocalShm();
        LOG_IF(INFO, ubring::FLAGS_ub_trace_verbose)
            << "Client handshake ends (use ubring) on "
            << socket->description();
    } else if (result == handshake::STEP_FALLBACK) {
        LOG_IF(INFO, ubring::FLAGS_ub_trace_verbose)
            << "Client handshake ends (use tcp) on "
            << socket->description();
    }
    errno = 0;
    return NULL;
}

void UBShmTransport::Init(Socket *socket, const SocketOptions &options) {
    CHECK(_ub_ep == NULL);
    _socket = socket;
    _default_connect = options.app_connect;
    _on_edge_trigger = NULL;
    _ub_ep = new(std::nothrow)ubring::UBShmEndpoint(socket);
    if (!_ub_ep) {
        const int saved_errno = errno;
        PLOG(ERROR) << "Fail to create UBShmEndpoint";
        socket->SetFailed(
            saved_errno, "Fail to create UBShmEndpoint: %s", berror(saved_errno));
    }
    _ub_state = UB_UNKNOWN;
}

void UBShmTransport::Release() {
    if (_ub_ep) {
        delete _ub_ep;
        _ub_ep = NULL;
        _ub_state = UB_UNKNOWN;
    }
}

int UBShmTransport::Reset(int32_t expected_nref) {
    if (_ub_ep) {
        _ub_ep->Reset();
        _ub_state = UB_UNKNOWN;
    }
    return 0;
}

std::shared_ptr<AppConnect> UBShmTransport::Connect() {
    if (_default_connect == nullptr) {
        return  std::make_shared<ubring::UBConnect>();
    }
    return _default_connect;
}

void UBShmTransport::SetHighSpeedAvailable(bool available) {
    _ub_state = available ? UB_ON : UB_OFF;
}

int UBShmTransport::CutFromIOBuf(butil::IOBuf* buf) {
    butil::IOBuf* data[1] = {buf};
    return static_cast<int>(CutFromIOBufList(data, 1));
}

ssize_t UBShmTransport::CutFromIOBufList(
    butil::IOBuf** buf, size_t ndata) {
    CHECK(_ub_ep != NULL);
    return _ub_ep->CutFromIOBufList(buf, ndata);
}

int UBShmTransport::WaitEpollOut(
    butil::atomic<int>* epollout_butex, bool pollin, timespec duetime) {
    const int expected_val = epollout_butex->load(butil::memory_order_acquire);
    CHECK(_ub_ep != NULL);
    if (!_ub_ep->IsWritable()) {
        g_vars->nwaitepollout << 1;
        _ub_ep->PollerRegisterEpollOut(pollin);
        const int rc = bthread::butex_wait(
            epollout_butex, expected_val, &duetime);
        if (rc < 0) {
            if (errno != EAGAIN && errno != ETIMEDOUT) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to wait ub window of " << _socket;
                _socket->SetFailed(saved_errno,
                                   "Fail to wait ub window of %s: %s",
                                   _socket->description().c_str(),
                                   berror(saved_errno));
            }
            if (_socket->Failed()) {
                // Unlike TCP, writing cannot discover an already failed UB
                // channel, so check the Socket failure here.
                return 1;
            }
        }
        _ub_ep->PollerUnRegisterEpollOut(pollin);
    }
    return 0;
}

void UBShmTransport::ProcessEvent(bthread_attr_t attr) {
    bthread_t tid;
    if (FLAGS_usercode_in_coroutine) {
        OnEdge(_socket);
    } else if (ubring::FLAGS_ub_edisp_unsched == false) {
        auto rc = bthread_start_background(&tid, &attr, OnEdge, _socket);
        if (rc != 0) {
            LOG(FATAL) << "Fail to start ProcessEvent";
            OnEdge(_socket);
        }
    } else if (bthread_start_urgent(&tid, &attr, OnEdge, _socket) != 0) {
        LOG(FATAL) << "Fail to start ProcessEvent";
        OnEdge(_socket);
    }
}

void UBShmTransport::QueueMessage(InputMessageClosure& input_msg,
                                 int* num_bthread_created, bool last_msg) {
    if (last_msg) {
        return;
    }
    InputMessageBase* to_run_msg = input_msg.release();
    if (!to_run_msg) {
        return;
    }

    if (ubring::FLAGS_ub_disable_bthread) {
        ProcessInputMessage(to_run_msg);
        return;
    }
    // Create bthread for last_msg. The bthread is not scheduled
    // until bthread_flush() is called (in the worse case).

    // TODO(gejun): Join threads.
    bthread_t th;
    bthread_attr_t tmp = (FLAGS_usercode_in_pthread ?
                                      BTHREAD_ATTR_PTHREAD :
                                                                    BTHREAD_ATTR_NORMAL) | BTHREAD_NOSIGNAL;
    tmp.keytable_pool = _socket->keytable_pool();
    tmp.tag = bthread_self_tag();
    bthread_attr_set_name(&tmp, "ProcessInputMessage");

    if (!FLAGS_usercode_in_coroutine && bthread_start_background(
            &th, &tmp, ProcessInputMessage, to_run_msg) == 0) {
        ++*num_bthread_created;
    } else {
        ProcessInputMessage(to_run_msg);
    }
}

void UBShmTransport::Debug(std::ostream &os) {}

int UBShmTransport::ContextInitOrDie(bool serverOrNot, const void* _options) {
    if (serverOrNot) {
        if (!OptionsAvailableOverUB(static_cast<const ServerOptions *>(_options))) {
            return -1;
        }
        ubring::GlobalUBInitializeOrDie();
        if (!ubring::InitPollingModeWithTag(static_cast<const ServerOptions *>(_options)->bthread_tag)) {
            return -1;
        }
    } else {
        if (!OptionsAvailableForUB(static_cast<const ChannelOptions *>(_options))) {
            return -1;
        }
        ubring::GlobalUBInitializeOrDie();
        if (!ubring::InitPollingModeWithTag(bthread_self_tag())) {
            return -1;
        }
        return 0;
    }

    return 0;
}

bool UBShmTransport::OptionsAvailableForUB(const ChannelOptions* opt) {
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "Cannot use SSL and UB at the same time";
        return false;
    }
    if (!ubring::SupportedByUB(opt->protocol.name())) {
        LOG(WARNING) << "Cannot use " << opt->protocol.name()
                     << " over UB";
        return false;
    }
    return true;
}

bool UBShmTransport::OptionsAvailableOverUB(const ServerOptions* opt) {
    if (opt->rtmp_service) {
        LOG(WARNING) << "RTMP is not supported by UB";
        return false;
    }
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "SSL is not supported by UB";
        return false;
    }
    if (opt->nshead_service) {
        LOG(WARNING) << "NSHEAD is not supported by UB";
        return false;
    }
    if (opt->mongo_service_adaptor) {
        LOG(WARNING) << "MONGO is not supported by UB";
        return false;
    }
    return true;
}
} // namespace brpc
#endif
