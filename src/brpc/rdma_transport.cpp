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

#if BRPC_WITH_RDMA

#include "brpc/rdma_transport.h"
#include "butil/sys_byteorder.h"
#include "brpc/event_dispatcher.h"
#include "brpc/input_messenger.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_handshake.h"
#include "brpc/rdma/rdma_handshake_constants.h"
#include "brpc/rdma/rdma_handshake_server.h"
#include "brpc/rdma/rdma_helper.h"

namespace brpc {
DECLARE_bool(usercode_in_coroutine);
DECLARE_bool(usercode_in_pthread);

extern SocketVarsCollector *g_vars;

namespace rdma {

DECLARE_bool(rdma_trace_verbose);

void RdmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int err, void* data),
                               void* data) {
    RdmaTransport* transport =
        static_cast<RdmaTransport*>(socket->_transport.get());
    CHECK(transport->_rdma_ep != NULL);
    SocketUniquePtr ptr;
    if (Socket::Address(socket->id(), &ptr) != 0) {
        return;
    }
    if (!IsRdmaAvailable()) {
        transport->FallbackToTcp();
        done(0, data);
        return;
    }
    _done = done;
    _data = data;
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "RdmaProcessHandshakeAtClient");
    if (bthread_start_background(&tid, &attr,
                                 RdmaTransport::ProcessHandshakeAtClient,
                                 transport) < 0) {
        LOG(FATAL) << "Fail to start handshake bthread";
        Run();
    } else {
        ptr.release();
    }
}

void RdmaConnect::StopConnect(Socket*) {}

void RdmaConnect::Run() {
    _done(errno, _data);
}

}  // namespace rdma

void* RdmaTransport::ProcessHandshakeAtClient(void* arg) {
    RdmaTransport* transport = static_cast<RdmaTransport*>(arg);
    rdma::RdmaEndpoint* ep = transport->_rdma_ep;
    SocketUniquePtr socket(transport->_socket);
    rdma::RdmaConnect::RunGuard guard(
        static_cast<rdma::RdmaConnect*>(socket->_app_connect.get()));

    LOG_IF(INFO, rdma::FLAGS_rdma_trace_verbose)
        << "Start handshake on " << socket->description();

    std::unique_ptr<rdma::RdmaHandshake> protocol =
        rdma::CreateClientHandshake(ep, transport->_handshake.io());
    CHECK(protocol != NULL);
    transport->_handshake.set_protocol_version(protocol->ProtocolVersion());
    rdma::ParsedHello remote{};

    handshake::ClientHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{
        C_ALLOC_QPCQ, C_HELLO_SEND, C_HELLO_WAIT,
        C_BRINGUP_QP, C_ACK_SEND, 0};
    callbacks.prepare_local = [&]() {
        if (ep->AllocateResources() == 0) {
            return handshake::STEP_OK;
        }
        PLOG(WARNING) << "Fail to allocate rdma resources, fallback to tcp:"
                      << socket->description();
        // Resource preparation is transactional (see #3424): partial RDMA
        // resources are cleaned by AllocateResources and TCP remains usable.
        errno = 0;
        return handshake::STEP_FALLBACK;
    };
    callbacks.send_local_hello = [&]() {
        if (protocol->SendLocalHello() == 0) {
            return handshake::STEP_OK;
        }
        const int saved_errno = errno;
        socket->SetFailed(saved_errno,
                          "Fail to complete rdma handshake from %s: %s",
                          socket->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.receive_remote_hello = [&]() {
        const rdma::RemoteHelloResult result =
            protocol->ReceiveAndParseRemoteHello(&remote);
        if (result == rdma::RemoteHelloResult::NEGOTIATED) {
            return handshake::STEP_OK;
        }
        if (result != rdma::RemoteHelloResult::ERROR) {
            return handshake::STEP_FALLBACK;
        }
        const int saved_errno = errno;
        socket->SetFailed(saved_errno,
                          "Fail to complete rdma handshake from %s: %s",
                          socket->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.negotiate_resources = [&]() {
        ep->ApplyRemoteHello(remote);
        if (ep->BringUpQp(remote, false) == 0) {
            return handshake::STEP_OK;
        }
        LOG(WARNING) << "Fail to bringup QP, fallback to tcp:"
                     << socket->description();
        return handshake::STEP_FALLBACK;
    };
    callbacks.send_ack = [&](bool enabled) {
        const uint32_t flags_be = butil::HostToNet32(
            enabled ? rdma::HELLO_ACK_RDMA_OK : 0);
        if (transport->_handshake.io()->WriteAll(
                &flags_be, rdma::HELLO_ACK_LEN) == 0) {
            return handshake::STEP_OK;
        }
        const int saved_errno = errno;
        socket->SetFailed(saved_errno,
                          "Fail to complete rdma handshake from %s: %s",
                          socket->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.set_high_speed_active = [transport]() {
        transport->SetHighSpeedAvailable(true);
    };
    callbacks.set_tcp_active = [transport]() {
        transport->SetHighSpeedAvailable(false);
    };
    callbacks.on_failed = []() {};

    const handshake::StepResult result =
        transport->_handshake.RunClient(callbacks);
    if (result == handshake::STEP_OK) {
        LOG_IF(INFO, rdma::FLAGS_rdma_trace_verbose)
            << "Client handshake ends (use rdma v"
            << transport->handshake_version() << ") on "
            << socket->description();
    } else if (result == handshake::STEP_FALLBACK) {
        LOG_IF(INFO, rdma::FLAGS_rdma_trace_verbose)
            << "Client handshake ends (use tcp) on " << socket->description();
    }
    errno = 0;
    return NULL;
}

ParseResult RdmaTransport::ExecuteServerHandshake(butil::IOBuf* source,
                                                   Socket* socket) {
    RdmaTransport* transport =
        static_cast<RdmaTransport*>(socket->_transport.get());
    rdma::RdmaEndpoint* ep = transport->_rdma_ep;
    CHECK(ep != NULL);

    std::unique_ptr<rdma::RdmaHandshake> protocol;
    rdma::ParsedHello remote{};
    handshake::ServerHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{
        S_ALLOC_QPCQ, S_HELLO_SEND, S_HELLO_WAIT,
        S_BRINGUP_QP, 0, S_ACK_WAIT};
    callbacks.blocking = false;
    callbacks.fallback_on_not_mine = false;
    callbacks.receive_remote_hello = [&]() {
        if (source->size() < rdma::HELLO_MAGIC_LEN) {
            return handshake::STEP_NEED_MORE;
        }
        uint8_t magic[rdma::HELLO_MAGIC_LEN];
        CHECK_EQ(source->copy_to(magic, sizeof(magic)), sizeof(magic));
        protocol = rdma::CreateServerHandshakeByMagic(
            ep, transport->_handshake.io(), source, magic);
        if (protocol == NULL) {
            return handshake::STEP_NOT_MINE;
        }
        transport->_handshake.set_protocol_version(protocol->ProtocolVersion());
        const rdma::RemoteHelloResult result =
            protocol->ReceiveAndParseRemoteHello(&remote);
        if (result == rdma::RemoteHelloResult::NEED_MORE) {
            return handshake::STEP_NEED_MORE;
        }
        if (result == rdma::RemoteHelloResult::ERROR) {
            return handshake::STEP_ERROR;
        }
        if (result == rdma::RemoteHelloResult::NEGOTIATED) {
            return handshake::STEP_OK;
        }
        transport->_rdma_state = RDMA_OFF;
        return handshake::STEP_FALLBACK;
    };
    callbacks.prepare_local = [&]() {
        ep->ApplyRemoteHello(remote);
        if (ep->AllocateResources() < 0) {
            PLOG(WARNING) << "Fail to allocate rdma resources, fallback to tcp:"
                          << socket->description();
            transport->_rdma_state = RDMA_OFF;
            return handshake::STEP_FALLBACK;
        }
        return handshake::STEP_OK;
    };
    callbacks.negotiate_resources = [&]() {
        if (ep->BringUpQp(remote, true) < 0) {
            transport->_rdma_state = RDMA_OFF;
            return handshake::STEP_FALLBACK;
        }
        return handshake::STEP_OK;
    };
    callbacks.send_local_hello = [&](bool) {
        CHECK(protocol != NULL);
        return protocol->SendLocalHello() == 0
            ? handshake::STEP_OK : handshake::STEP_ERROR;
    };
    callbacks.receive_ack = [&]() {
        if (source->size() < rdma::HELLO_ACK_LEN) {
            return handshake::STEP_NEED_MORE;
        }
        uint32_t flags_be = 0;
        CHECK_EQ(source->cutn(&flags_be, rdma::HELLO_ACK_LEN),
                 rdma::HELLO_ACK_LEN);
        const bool client_ack_ok =
            (butil::NetToHost32(flags_be) & rdma::HELLO_ACK_RDMA_OK) != 0;
        if (!client_ack_ok) {
            // Keep coalesced RPC bytes for InputMessenger after fallback.
            return handshake::STEP_FALLBACK;
        }
        if (!source->empty() || transport->_rdma_state == RDMA_OFF) {
            return handshake::STEP_ERROR;
        }
        return handshake::STEP_OK;
    };
    callbacks.set_high_speed_active = [transport]() {
        transport->SetHighSpeedAvailable(true);
    };
    callbacks.set_tcp_active = [transport]() {
        transport->SetHighSpeedAvailable(false);
    };
    callbacks.on_failed = []() {};

    const handshake::StepResult result =
        transport->_handshake.RunServer(callbacks);
    if (result == handshake::STEP_NEED_MORE) {
        if (transport->_handshake.phase() == S_ACK_WAIT &&
            socket->parsing_context() == NULL) {
            socket->reset_parsing_context(
                rdma::ServerHandshakeContext::Create());
        }
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    socket->reset_parsing_context(NULL);
    if (result == handshake::STEP_ERROR) {
        return MakeParseError(PARSE_ERROR_ABSOLUTELY_WRONG);
    }
    return MakeParseError(PARSE_ERROR_TRY_OTHERS);
}

void RdmaTransport::Init(Socket *socket, const SocketOptions &options) {
    CHECK(_rdma_ep == NULL);
    if (options.socket_mode == SOCKET_MODE_RDMA) {
        _rdma_ep = new(std::nothrow)rdma::RdmaEndpoint(socket);
        if (!_rdma_ep) {
            const int saved_errno = errno;
            PLOG(ERROR) << "Fail to create RdmaEndpoint";
            socket->SetFailed(
                saved_errno, "Fail to create RdmaEndpoint: %s", berror(saved_errno));
        }
        _rdma_state = RDMA_UNKNOWN;
    } else {
        _rdma_state = RDMA_OFF;
        socket->_socket_mode = SOCKET_MODE_TCP;
    }
    OnEdgeTrigger default_on_edge;
    if (options.need_on_edge_trigger) {
        // Server-side RDMA sockets drive the handshake through the standard
        // InputMessenger path (ParseRdmaHandshake), so they use OnNewMessages
        // just like TCP sockets. Only client-side sockets, whose handshake
        // ProcessHandshakeAtClient is an active blocking bthread relying on
        // HandshakeSession being woken by OnNewDataFromTcp.
        if (options.user == static_cast<SocketUser*>(get_client_side_messenger())) {
            default_on_edge = UpgradeTransport::OnNewDataFromTcp;
        } else {
            default_on_edge = InputMessenger::OnNewMessages;
        }
    }
    InitUpgradeTransport(socket, options, default_on_edge);
}

void RdmaTransport::Release() {
    if (_rdma_ep) {
        delete _rdma_ep;
        _rdma_ep = NULL;
        _rdma_state = RDMA_UNKNOWN;
    }
}

int RdmaTransport::Reset(int32_t expected_nref) {
    if (_rdma_ep) {
        _rdma_ep->Reset();
        _rdma_state = RDMA_UNKNOWN;
    }
    ResetUpgradeTransport();
    return 0;
}

std::shared_ptr<AppConnect> RdmaTransport::Connect() {
    if (_default_connect == nullptr) {
        return  std::make_shared<rdma::RdmaConnect>();
    }
    return _default_connect;
}

void RdmaTransport::SetHighSpeedAvailable(bool available) {
    _rdma_state = available ? RDMA_ON : RDMA_OFF;
}

ssize_t RdmaTransport::CutFromHighSpeedIOBufList(
    butil::IOBuf** buf, size_t ndata) {
    CHECK(_rdma_ep != NULL);
    return _rdma_ep->CutFromIOBufList(buf, ndata);
}

int RdmaTransport::WaitHighSpeedEpollOut(
    butil::atomic<int>* epollout_butex, bool, timespec duetime) {
    const int expected_val = epollout_butex->load(butil::memory_order_acquire);
    CHECK(_rdma_ep != NULL);
    if (!_rdma_ep->IsWritable()) {
        g_vars->nwaitepollout << 1;
        if (bthread::butex_wait(epollout_butex, expected_val, &duetime) < 0) {
            if (errno != EAGAIN && errno != ETIMEDOUT) {
                const int saved_errno = errno;
                PLOG(WARNING) << "Fail to wait rdma window of " << _socket;
                _socket->SetFailed(saved_errno,
                                   "Fail to wait rdma window of %s: %s",
                                   _socket->description().c_str(),
                                   berror(saved_errno));
            }
            if (_socket->Failed()) {
                // Unlike TCP, writing cannot discover an already failed RDMA
                // channel, so check the Socket failure here.
                return 1;
            }
        }
    }
    return 0;
}

void RdmaTransport::ProcessEvent(bthread_attr_t attr) {
    bthread_t tid;
    if (FLAGS_usercode_in_coroutine) {
        OnEdge(_socket);
    } else if (!EventDispatcherUnsched()) {
        auto rc = bthread_start_urgent(&tid, &attr, OnEdge, _socket);
        if (rc != 0) {
            LOG(FATAL) << "Fail to start ProcessEvent";
            OnEdge(_socket);
        }
    } else if (bthread_start_background(&tid, &attr, OnEdge, _socket) != 0) {
        LOG(FATAL) << "Fail to start ProcessEvent";
        OnEdge(_socket);
    }
}

void RdmaTransport::QueueMessage(InputMessageClosure& input_msg,
                                 int* num_bthread_created, bool last_msg) {
    if (last_msg && !rdma::FLAGS_rdma_use_polling) {
        return;
    }
    InputMessageBase* to_run_msg = input_msg.release();
    if (!to_run_msg) {
        return;
    }

    if (rdma::FLAGS_rdma_disable_bthread) {
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

void RdmaTransport::Debug(std::ostream &os) {
    if (_rdma_state == RDMA_ON && _rdma_ep) {
        _rdma_ep->DebugInfo(os);
    }
    const char* state = "UNKNOWN";
    switch (_handshake.phase(butil::memory_order_relaxed)) {
    case UNINIT: state = "UNINIT"; break;
    case C_ALLOC_QPCQ: state = "C_ALLOC_QPCQ"; break;
    case C_HELLO_SEND: state = "C_HELLO_SEND"; break;
    case C_HELLO_WAIT: state = "C_HELLO_WAIT"; break;
    case C_BRINGUP_QP: state = "C_BRINGUP_QP"; break;
    case C_ACK_SEND: state = "C_ACK_SEND"; break;
    case S_HELLO_WAIT: state = "S_HELLO_WAIT"; break;
    case S_ALLOC_QPCQ: state = "S_ALLOC_QPCQ"; break;
    case S_BRINGUP_QP: state = "S_BRINGUP_QP"; break;
    case S_HELLO_SEND: state = "S_HELLO_SEND"; break;
    case S_ACK_WAIT: state = "S_ACK_WAIT"; break;
    case ESTABLISHED: state = "ESTABLISHED"; break;
    case FALLBACK_TCP: state = "FALLBACK_TCP"; break;
    case FAILED: state = "FAILED"; break;
    }
    os << "\nhandshake_state=" << state
       << "\nhandshake_version=" << handshake_version();
}

int RdmaTransport::ContextInitOrDie(bool serverOrNot, const void* _options) {
    if (serverOrNot) {
        if (!OptionsAvailableOverRdma(static_cast<const ServerOptions *>(_options))) {
            return -1;
        }
        rdma::GlobalRdmaInitializeOrDie();
        if (!rdma::InitPollingModeWithTag(static_cast<const ServerOptions *>(_options)->bthread_tag)) {
            return -1;
        }
    } else {
        if (!OptionsAvailableForRdma(static_cast<const ChannelOptions *>(_options))) {
            return -1;
        }
        rdma::GlobalRdmaInitializeOrDie();
        if (!rdma::InitPollingModeWithTag(bthread_self_tag())) {
            return -1;
        }
        return 0;
    }

    return 0;
}

bool RdmaTransport::OptionsAvailableForRdma(const ChannelOptions* opt) {
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "Cannot use SSL and RDMA at the same time";
        return false;
    }
    if (!rdma::SupportedByRdma(opt->protocol.name())) {
        LOG(WARNING) << "Cannot use " << opt->protocol.name()
                     << " over RDMA";
        return false;
    }
    return true;
}

bool RdmaTransport::OptionsAvailableOverRdma(const ServerOptions* opt) {
    if (opt->rtmp_service) {
        LOG(WARNING) << "RTMP is not supported by RDMA";
        return false;
    }
    if (opt->has_ssl_options()) {
        LOG(WARNING) << "SSL is not supported by RDMA";
        return false;
    }
    if (opt->nshead_service) {
        LOG(WARNING) << "NSHEAD is not supported by RDMA";
        return false;
    }
    if (opt->mongo_service_adaptor) {
        LOG(WARNING) << "MONGO is not supported by RDMA";
        return false;
    }
    return true;
}
} // namespace brpc
#endif
