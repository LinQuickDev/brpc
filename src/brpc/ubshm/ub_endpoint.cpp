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

#include <errno.h>

#include <gflags/gflags.h>
#include <array>
#include "butil/fd_utility.h"
#include "butil/logging.h"                   // CHECK, LOG
#include "butil/sys_byteorder.h"             // HostToNet,NetToHost
#include "bthread/bthread.h"
#include "brpc/errno.pb.h"
#include "brpc/event_dispatcher.h"
#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/reloadable_flags.h"
#include "brpc/ubshm/ub_helper.h"
#include "brpc/ubshm/ub_endpoint.h"
#include "brpc/ubshm/shm/shm_def.h"
#include "brpc/ubshm/common/common.h"
#include "brpc/ubshm_transport.h"
#include "brpc/ubshm/ubr_trx.h"

DECLARE_int32(task_group_ntags);

namespace brpc {
DECLARE_bool(log_connection_close);
namespace ubring {

extern bool g_skip_ub_init;
DEFINE_int32(data_queue_size, 4, "data queue size for UB");
DEFINE_bool(ub_trace_verbose, false, "Print log message verbosely");
BRPC_VALIDATE_GFLAG(ub_trace_verbose, brpc::PassValidate);
DEFINE_int32(ub_poller_num, 1, "Poller number in ub polling mode.");
DEFINE_bool(ub_poller_yield, false, "Yield thread in RDMA polling mode.");
DEFINE_bool(ub_edisp_unsched, false, "Disable event dispatcher schedule");
DEFINE_bool(ub_disable_bthread, false, "Disable bthread in RDMA");

static const size_t MIN_ONCE_READ = 4096;
static const size_t MAX_ONCE_READ = 524288;
static const size_t IOBUF_IOV_MAX = 256;

static const char* MAGIC_STR = "UB";
static const size_t MAGIC_STR_LEN = 2;
static const size_t HELLO_MSG_LEN_MIN = 64;
static const size_t ACK_MSG_LEN = 4;
static uint16_t g_ub_hello_msg_len = 64;
static uint16_t g_ub_hello_version = 2;
static uint16_t g_ub_impl_version = 1;

static const uint32_t ACK_MSG_UB_OK = 0x1;

static butil::Mutex* g_ubring_resource_mutex = NULL;

void HelloMessage::Serialize(void* data) const {
    char* current_pos = static_cast<char*>(data);
    const uint16_t net_msg_len = butil::HostToNet16(msg_len);
    memcpy(current_pos, &net_msg_len, sizeof(net_msg_len));
    current_pos += sizeof(net_msg_len);
    const uint16_t net_hello_ver = butil::HostToNet16(hello_ver);
    memcpy(current_pos, &net_hello_ver, sizeof(net_hello_ver));
    current_pos += sizeof(net_hello_ver);
    const uint16_t net_impl_ver = butil::HostToNet16(impl_ver);
    memcpy(current_pos, &net_impl_ver, sizeof(net_impl_ver));
    current_pos += sizeof(net_impl_ver);
    const uint64_t net_len = butil::HostToNet64(len);
    memcpy(current_pos, &net_len, sizeof(net_len));
    current_pos += sizeof(net_len);
    memcpy(current_pos, shm_name, SHM_MAX_NAME_BUFF_LEN);
}

void HelloMessage::Deserialize(void* data) {
    char* current_pos = static_cast<char*>(data);
    uint16_t net_msg_len;
    memcpy(&net_msg_len, current_pos, sizeof(net_msg_len));
    msg_len = butil::NetToHost16(net_msg_len);
    current_pos += sizeof(net_msg_len);
    uint16_t net_hello_ver;
    memcpy(&net_hello_ver, current_pos, sizeof(net_hello_ver));
    hello_ver = butil::NetToHost16(net_hello_ver);
    current_pos += sizeof(net_hello_ver);
    uint16_t net_impl_ver;
    memcpy(&net_impl_ver, current_pos, sizeof(net_impl_ver));
    impl_ver = butil::NetToHost16(net_impl_ver);
    current_pos += sizeof(net_impl_ver);
    uint64_t net_len;
    memcpy(&net_len, current_pos, sizeof(net_len));
    len = butil::NetToHost64(net_len);
    current_pos += sizeof(net_len);
    memcpy(shm_name, current_pos, SHM_MAX_NAME_BUFF_LEN);
}

std::string HelloMessage::toString() const {
    constexpr size_t MAX_LEN = 16 + 6 + 16 + 6 + 16 + 6 + 20 + 6 + SHM_MAX_NAME_BUFF_LEN + 32;
    std::array<char, MAX_LEN> buf;
    int n = snprintf(buf.data(), buf.size(),
        "msg_len=%u, hello_ver=%u, impl_ver=%u, len=%lu, shm_name=%.*s",
        msg_len,
        hello_ver,
        impl_ver,
        static_cast<unsigned long>(len),  // compatible with 32/64-bit
        static_cast<int>(SHM_MAX_NAME_BUFF_LEN),  // limit max output length
        shm_name
    );
    return std::string(buf.data(), static_cast<size_t>(n));
}

UBShmEndpoint::UBShmEndpoint(Socket* s)
    : _socket(s)
    , _socket_id(s ? s->id() : INVALID_SOCKET_ID)
    , _ub_ring(nullptr)
    , _cq_sid(INVALID_SOCKET_ID)
{
}

UBShmEndpoint::~UBShmEndpoint() {
    Reset();
}

void UBShmEndpoint::Reset() {
    DeallocateResources();

    delete _ub_ring;
    _ub_ring = nullptr;
    _cq_sid = INVALID_SOCKET_ID;
}

void UBConnect::StartConnect(const Socket* socket,
                               void (*done)(int err, void* data),
                               void* data) {
    auto* ub_transport = static_cast<UBShmTransport*>(socket->_transport.get());
    CHECK(ub_transport->_ub_ep != NULL);
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    if (!IsUBAvailable()) {
        ub_transport->FallbackToTcp();
        done(0, data);
        return;
    }
    _done = done;
    _data = data;
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UBProcessHandshakeAtClient");
    if (bthread_start_background(&tid, &attr,
                UBShmTransport::ProcessHandshakeAtClient, ub_transport) < 0) {
        LOG(FATAL) << "Fail to start handshake bthread";
        Run();
    } else {
        s.release();
    }
}

void UBConnect::StopConnect(Socket* socket) { }

void UBConnect::Run() {
    _done(errno, _data);
}

}  // namespace ubring

// Keep the wire constants and endpoint resource implementation in ubring,
// while the connection-control state machine belongs to the Transport layer.
using namespace ubring;

void UBShmTransport::StartServerHandshake() {
    if (!IsUBAvailable()) {
        FallbackToTcp();
        return;
    }
    bthread_t tid;
    _handshake.SetPhase(S_HELLO_WAIT);
    SocketUniquePtr socket;
    _socket->ReAddress(&socket);
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UBProcessHandshakeAtServer");
    if (bthread_start_background(
            &tid, &attr, ProcessHandshakeAtServer, this) < 0) {
        _handshake.SetPhase(UNINIT);
        LOG(FATAL) << "Fail to start handshake bthread";
    } else {
        socket.release();
    }
}
bool HelloNegotiationValid(HelloMessage& msg) {
    if (msg.hello_ver == g_ub_hello_version &&
        msg.impl_ver == g_ub_impl_version) {
        // This can be modified for future compatibility
        return true;
    }
    return false;
}

void* UBShmTransport::ProcessHandshakeAtClient(void* arg) {
    UBShmTransport* ub_transport = static_cast<UBShmTransport*>(arg);
    UBShmEndpoint* ep = ub_transport->_ub_ep;
    SocketUniquePtr s(ep->_socket);
    UBConnect::RunGuard rg((UBConnect*)s->_app_connect.get());

    LOG_IF(INFO, FLAGS_ub_trace_verbose) 
        << "Start handshake on " << s->_local_side;

    uint8_t data[g_ub_hello_msg_len];
    size_t local_shm_len = (size_t)(FLAGS_data_queue_size) * MB_TO_BYTE;
    SHM local_trx_shm = {NULL, local_shm_len, 0, {0}, (uint32_t)s->fd()};
    auto shm_name_str = butil::endpoint2str(s->local_side());
    const char* shm_name = shm_name_str.c_str();
    HelloMessage remote_msg{};

    handshake::ClientHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{
        C_ALLOC_SHM, C_HELLO_SEND, C_HELLO_WAIT,
        C_MAP_REMOTE_SHM, C_ACK_SEND, 0};
    callbacks.prepare_local = [&]() {
        if (ep->AllocateClientResources(&local_trx_shm, shm_name) == 0) {
            return handshake::STEP_OK;
        }
        LOG(WARNING) << "Fallback to tcp:" << s->description();
        errno = 0;
        return handshake::STEP_FALLBACK;
    };
    callbacks.send_local_hello = [&]() {
        HelloMessage local_msg{};
        local_msg.msg_len = g_ub_hello_msg_len;
        local_msg.hello_ver = g_ub_hello_version;
        local_msg.impl_ver = g_ub_impl_version;
        local_msg.len = local_shm_len;
        memcpy(local_msg.shm_name, local_trx_shm.name,
               SHM_MAX_NAME_BUFF_LEN);
        memcpy(data, MAGIC_STR, MAGIC_STR_LEN);
        local_msg.Serialize((char*)data + MAGIC_STR_LEN);
        if (ub_transport->_handshake.io()->WriteAll(
                data, g_ub_hello_msg_len) == 0) {
            LOG_IF(INFO, FLAGS_ub_trace_verbose)
                << "client handshake message : " << local_msg.toString();
            return handshake::STEP_OK;
        }
        const int saved_errno = errno;
        PLOG(WARNING) << "Fail to send hello message to server:"
                      << s->description();
        s->SetFailed(saved_errno,
                     "Fail to complete ubring handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.receive_remote_hello = [&]() {
        if (ub_transport->_handshake.io()->ReadExact(
                data, MAGIC_STR_LEN) < 0) {
            const int saved_errno = errno;
            s->SetFailed(saved_errno,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(saved_errno));
            return handshake::STEP_ERROR;
        }
        if (memcmp(data, MAGIC_STR, MAGIC_STR_LEN) != 0) {
            s->SetFailed(EPROTO,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(EPROTO));
            return handshake::STEP_ERROR;
        }
        if (ub_transport->_handshake.io()->ReadExact(
                data, HELLO_MSG_LEN_MIN - MAGIC_STR_LEN) < 0) {
            const int saved_errno = errno;
            s->SetFailed(saved_errno,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(saved_errno));
            return handshake::STEP_ERROR;
        }
        remote_msg.Deserialize(data);
        if (remote_msg.msg_len < HELLO_MSG_LEN_MIN) {
            s->SetFailed(EPROTO,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(EPROTO));
            return handshake::STEP_ERROR;
        }
        if (!HelloNegotiationValid(remote_msg)) {
            LOG(WARNING) << "Fail to negotiate with server, fallback to tcp:"
                         << s->description();
            return handshake::STEP_FALLBACK;
        }
        return handshake::STEP_OK;
    };
    callbacks.negotiate_resources = [&]() {
        if (ep->_ub_ring->UbrMapRemoteShm(&local_trx_shm, shm_name) == 0) {
            return handshake::STEP_OK;
        }
        LOG(WARNING) << "Fail to map the remote shm, fallback to tcp:"
                     << s->description();
        return handshake::STEP_FALLBACK;
    };
    callbacks.send_ack = [&](bool enabled) {
        uint32_t* flags = (uint32_t*)data;
        *flags = butil::HostToNet32(enabled ? ACK_MSG_UB_OK : 0);
        if (ub_transport->_handshake.io()->WriteAll(data, ACK_MSG_LEN) == 0) {
            return handshake::STEP_OK;
        }
        const int saved_errno = errno;
        s->SetFailed(saved_errno,
                     "Fail to complete ubring handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.set_high_speed_active = [ub_transport]() {
        ub_transport->SetHighSpeedAvailable(true);
    };
    callbacks.set_tcp_active = [ub_transport]() {
        ub_transport->SetHighSpeedAvailable(false);
    };
    callbacks.on_failed = []() {};

    const handshake::StepResult result =
        ub_transport->_handshake.RunClient(callbacks);
    if (result == handshake::STEP_OK) {
        ep->_ub_ring->UbrUnlinkLocalShm();
        LOG_IF(INFO, FLAGS_ub_trace_verbose) 
            << "Client handshake ends (use ubring) on " << s->description();
    } else if (result == handshake::STEP_FALLBACK) {
        LOG_IF(INFO, FLAGS_ub_trace_verbose) 
            << "Client handshake ends (use tcp) on " << s->description();
    }

    errno = 0;

    return NULL;
}

void* UBShmTransport::ProcessHandshakeAtServer(void* arg) {
    UBShmTransport* ub_transport = static_cast<UBShmTransport*>(arg);
    UBShmEndpoint* ep = ub_transport->_ub_ep;
    SocketUniquePtr s(ep->_socket);

    LOG_IF(INFO, FLAGS_ub_trace_verbose)
        << "Start handshake on " << s->description();

    uint8_t data[g_ub_hello_msg_len];
    HelloMessage remote_msg{};
    bool local_enabled = false;

    handshake::ServerHandshakeCallbacks callbacks{};
    callbacks.phases = handshake::HandshakePhases{
        S_ALLOC_SHM, S_HELLO_SEND, S_HELLO_WAIT,
        S_ALLOC_SHM, 0, S_ACK_WAIT};
    callbacks.fallback_on_not_mine = true;
    callbacks.receive_remote_hello = [&]() {
        if (ub_transport->_handshake.io()->ReadExact(
                data, MAGIC_STR_LEN) < 0) {
            const int saved_errno = errno;
            s->SetFailed(saved_errno,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(saved_errno));
            return handshake::STEP_ERROR;
        }
        if (memcmp(data, MAGIC_STR, MAGIC_STR_LEN) != 0) {
            s->_read_buf.append(data, MAGIC_STR_LEN);
            return handshake::STEP_NOT_MINE;
        }
        if (ub_transport->_handshake.io()->ReadExact(
                data, g_ub_hello_msg_len - MAGIC_STR_LEN) < 0) {
            const int saved_errno = errno;
            s->SetFailed(saved_errno,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(saved_errno));
            return handshake::STEP_ERROR;
        }
        remote_msg.Deserialize(data);
        LOG_IF(INFO, FLAGS_ub_trace_verbose)
            << "server receive handshake message : " << remote_msg.toString();
        if (remote_msg.msg_len < HELLO_MSG_LEN_MIN) {
            s->SetFailed(EPROTO,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(EPROTO));
            return handshake::STEP_ERROR;
        }
        if (!HelloNegotiationValid(remote_msg)) {
            return handshake::STEP_FALLBACK;
        }
        return handshake::STEP_OK;
    };
    callbacks.prepare_local = [&]() {
        ubring::SHM remote_trx_shm = {
            NULL, remote_msg.len, 0, {0}, (uint32_t)ep->_socket->fd()};
        strncpy(remote_trx_shm.name, remote_msg.shm_name, SHM_MAX_NAME_BUFF_LEN);

        size_t local_shm_len = (size_t)(FLAGS_data_queue_size) * MB_TO_BYTE;
        // server-side shared memory name
        ubring::SHM local_trx_shm = {
            NULL, local_shm_len, 0, {0}, (uint32_t)ep->_socket->fd()};
        char client_name[SHM_MAX_NAME_BUFF_LEN];
        strncpy(client_name, remote_msg.shm_name, SHM_MAX_NAME_BUFF_LEN);

        char *client_ip_port = strrchr(client_name, '_');
        if (client_ip_port != NULL) {
            *client_ip_port = '\0';
        }
        int result = snprintf(local_trx_shm.name, SHM_MAX_NAME_BUFF_LEN, "%s_%s",
            client_name, SERVER_SHM_NAME_SUFFIX);
        if (UNLIKELY(result < 0)) {
            LOG(WARNING) << "Copy client shared memory name failed, ret=" << result;
            return handshake::STEP_FALLBACK;
        }
        if (ep->AllocateServerResources(
                &remote_trx_shm, &local_trx_shm) < 0) {
            LOG(WARNING) << "Fail to allocate ub resources, fallback to tcp:"
                         << s->description();
            return handshake::STEP_FALLBACK;
        }
        return handshake::STEP_OK;
    };
    callbacks.negotiate_resources = []() {
        return handshake::STEP_OK;
    };
    callbacks.send_local_hello = [&](bool enabled) {
        local_enabled = enabled;
        HelloMessage local_msg{};
        local_msg.msg_len = g_ub_hello_msg_len;
        if (enabled) {
            local_msg.hello_ver = g_ub_hello_version;
            local_msg.impl_ver = g_ub_impl_version;
            local_msg.len = (FLAGS_data_queue_size) * MB_TO_BYTE;
            memcpy(local_msg.shm_name, remote_msg.shm_name,
                   SHM_MAX_NAME_BUFF_LEN);
        }
        memcpy(data, MAGIC_STR, MAGIC_STR_LEN);
        local_msg.Serialize((char*)data + MAGIC_STR_LEN);
        if (ub_transport->_handshake.io()->WriteAll(
                data, g_ub_hello_msg_len) == 0) {
            return handshake::STEP_OK;
        }
        const int saved_errno = errno;
        s->SetFailed(saved_errno,
                     "Fail to complete ub handshake from %s: %s",
                     s->description().c_str(), berror(saved_errno));
        return handshake::STEP_ERROR;
    };
    callbacks.receive_ack = [&]() {
        if (ub_transport->_handshake.io()->ReadExact(
                data, ACK_MSG_LEN) < 0) {
            const int saved_errno = errno;
            s->SetFailed(saved_errno,
                         "Fail to complete ubring handshake from %s: %s",
                         s->description().c_str(), berror(saved_errno));
            return handshake::STEP_ERROR;
        }
        uint32_t* flags = (uint32_t*)data;
        const bool client_enabled =
            (butil::NetToHost32(*flags) & ACK_MSG_UB_OK) != 0;
        if (!client_enabled) {
            return handshake::STEP_FALLBACK;
        }
        if (!local_enabled) {
            s->SetFailed(EPROTO,
                         "Fail to complete ub handshake from %s: %s",
                         s->description().c_str(), berror(EPROTO));
            return handshake::STEP_ERROR;
        }
        return handshake::STEP_OK;
    };
    callbacks.set_high_speed_active = [ub_transport]() {
        ub_transport->SetHighSpeedAvailable(true);
    };
    callbacks.set_tcp_active = [ub_transport]() {
        ub_transport->SetHighSpeedAvailable(false);
    };
    callbacks.on_failed = []() {};

    const handshake::StepResult result =
        ub_transport->_handshake.RunServer(callbacks);
    if (result == handshake::STEP_OK) {
        ep->_ub_ring->UbrUnlinkLocalShm();
        LOG_IF(INFO, FLAGS_ub_trace_verbose)
            << "Server handshake ends (use ubring) on " << s->description();
    } else if (result == handshake::STEP_FALLBACK) {
        LOG_IF(INFO, FLAGS_ub_trace_verbose) 
            << "Server handshake ends (use tcp) on " << s->description();
    }
    if (result != handshake::STEP_ERROR) {
        ub_transport->TryReadOnTcp();
    }

    return NULL;
}

namespace ubring {

bool UBShmEndpoint::IsWritable() const {
    if (BAIDU_UNLIKELY(g_skip_ub_init)) {
        // Just for UT
        return false;
    }
    auto ret = _ub_ring->IsUbrTrxWriteable(EPOLLET);
    if (ret == 0) {
        return true;
    }
    return false;
}

ssize_t UBShmEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (BAIDU_UNLIKELY(g_skip_ub_init)) {
        // Just for UT
        errno = EAGAIN;
        return -1;
    }
    if (BAIDU_UNLIKELY(ndata == 0)) {
        return 0;
    }
    struct iovec vec[IOBUF_IOV_MAX];
    size_t nvec = 0;
    for (size_t i = 0; i < ndata; ++i) {
        const butil::IOBuf* p = from[i];
        const size_t nref = p->backing_block_num();
        for (size_t j = 0; j < nref && nvec < IOBUF_IOV_MAX; ++j, ++nvec) {
            butil::StringPiece sp = p->backing_block(j);
            vec[nvec].iov_base = const_cast<char*>(sp.data());
            vec[nvec].iov_len = sp.size();
        }
    }

    ssize_t nw = 0;
    errno = 0;
    nw = _ub_ring->UbrTrxWritev(vec, nvec);
    if (UNLIKELY(nw == -1)) {
        if (errno == EMSGSIZE) {
            LOG(ERROR) << "Non-blocking send msg failed, message is larger than ubring capacity.";
        } else {
            LOG(ERROR) << "Non-blocking send msg in failed, connection has been closed.";
            errno = EPIPE;
        }
    } else if (UNLIKELY(nw == UBRING_RETRY)) {
        errno = EAGAIN;
        nw = -1;
    }
    if (nw <= 0) {
        return nw;
    }
    size_t npop_all = nw;
    for (size_t i = 0; i < ndata; ++i) {
        npop_all -= from[i]->pop_front(npop_all);
        if (npop_all == 0) {
            break;
        }
    }
    return nw;
}

int UBShmEndpoint::AllocateClientResources(ubring::SHM* local_trx_shm, const char* shm_name) {
    if (BAIDU_UNLIKELY(g_skip_ub_init)) {
        // For UT
        return 0;
    }

    CHECK(_ub_ring == NULL);
    // TODO: Pooling management
    _ub_ring = new UBRing();

    SocketOptions options;
    options.user = this;
    options.keytable_pool = _socket->_keytable_pool;
    if (Socket::Create(options, &_cq_sid) < 0) {
        const int saved_errno = errno;
        PLOG(WARNING) << "Fail to create socket for cq";
        delete _ub_ring;
        _ub_ring = NULL;
        _cq_sid = INVALID_SOCKET_ID;
        errno = saved_errno;
        return -1;
    }
    int ret = _ub_ring->UbrAllocateLocalShm(local_trx_shm, shm_name);
    if (ret != 0) {
        const int saved_errno = errno;
        DeallocateResources();
        delete _ub_ring;
        _ub_ring = NULL;
        _cq_sid = INVALID_SOCKET_ID;
        errno = saved_errno;
        return ret;
    }
    PollerRegisterEvent(CqSidOp::ADD, EPOLLIN);
    return 0;
}

int UBShmEndpoint::AllocateServerResources(ubring::SHM* remote_trx_shm, ubring::SHM* local_trx_shm) {
    if (BAIDU_UNLIKELY(g_skip_ub_init)) {
        // For UT
        return 0;
    }

    CHECK(_ub_ring == NULL);
    // TODO: Pooling management
    _ub_ring = new UBRing();

    SocketOptions options;
    options.user = this;
    options.keytable_pool = _socket->_keytable_pool;
    if (Socket::Create(options, &_cq_sid) < 0) {
        const int saved_errno = errno;
        PLOG(WARNING) << "Fail to create socket for cq";
        delete _ub_ring;
        _ub_ring = NULL;
        _cq_sid = INVALID_SOCKET_ID;
        errno = saved_errno;
        return -1;
    }
    int ret = _ub_ring->UbrAllocateServerShm(remote_trx_shm, local_trx_shm);
    if (ret != 0) {
        const int saved_errno = errno;
        DeallocateResources();
        delete _ub_ring;
        _ub_ring = NULL;
        _cq_sid = INVALID_SOCKET_ID;
        errno = saved_errno;
        return ret;
    }
    // TODO mwj should polling start after the connection is established?
    PollerRegisterEvent(CqSidOp::ADD, EPOLLIN);
    return ret;
}

void UBShmEndpoint::DeallocateResources() {
    if (!_ub_ring) {
        return;
    }
    PollerRegisterEvent(CqSidOp::REMOVE);
    _ub_ring->UbrTrxClose();
    if (INVALID_SOCKET_ID != _cq_sid) {
        SocketUniquePtr s;
        if (Socket::Address(_cq_sid, &s) == 0) {
            s->_user = NULL;
            s->_fd = -1;
            s->SetFailed();
        }
    }
}

void UBShmEndpoint::PollIn(UBShmEndpoint* ep, uint32_t ep_event) {
    SocketUniquePtr s;
    if (Socket::Address(ep->_socket_id, &s) < 0) {
        return;
    }
    auto* ub_transport = static_cast<UBShmTransport*>(s->_transport.get());
    CHECK(ep == ub_transport->_ub_ep);

    InputMessageClosure last_msg;
    while (true) {
        int ret = ep->_ub_ring->IsUbrTrxReadable(ep_event);
        if (ret < 0) {
            return;
        }

        bool read_eof = false;
        while (!read_eof) {
            const int64_t received_us = butil::cpuwide_time_us();
            const int64_t base_realtime = butil::gettimeofday_us() - received_us;

            size_t once_read = s->_avg_msg_size * 16;
            if (once_read < MIN_ONCE_READ) {
                once_read = MIN_ONCE_READ;
            } else if (once_read > MAX_ONCE_READ) {
                once_read = MAX_ONCE_READ;
            }

            const ssize_t nr = s->_read_buf.append_from_reader(ep->_ub_ring, once_read);
            if (nr <= 0) {
                if (0 == nr) {
                    // Set `read_eof' flag and proceed to feed EOF into `Protocol'
                    // (implied by m->_read_buf.empty), which may produce a new
                    // `InputMessageBase' under some protocols such as HTTP
                    LOG_IF(WARNING, FLAGS_log_connection_close) << *s << " was closed by remote side";
                    read_eof = true;
                } else if (errno != EAGAIN) {
                    if (errno == EINTR) {
                        continue;
                    }
                    const int saved_errno = errno;
                    PLOG(WARNING) << "Fail to read from " << *s;
                    s->SetFailed(saved_errno, "Fail to read from %s: %s",
                                 s->description().c_str(), berror(saved_errno));
                    return;
                } else {
                    return;
                }
            }

            InputMessenger* messenger = static_cast<InputMessenger*>(s->user());
            if (messenger->ProcessNewMessage(s.get(), nr, read_eof, received_us,
                                             base_realtime, last_msg) < 0) {
                return;
            } 
        }

        if (read_eof) {
            s->SetEOF();
        }
    }
}

void UBShmEndpoint::PollOut(UBShmEndpoint* ep, uint32_t ep_event) {
    SocketUniquePtr s;
    if (Socket::Address(ep->_socket_id, &s) < 0) {
        return;
    }
    auto* ub_transport = static_cast<UBShmTransport*>(s->_transport.get());
    CHECK(ep == ub_transport->_ub_ep);
    if (ep->IsWritable()) {
        s->WakeAsEpollOut();
    }

}

int UBShmEndpoint::GlobalInitialize() {
    g_ubring_resource_mutex = new butil::Mutex;
    _poller_groups = std::vector<PollerGroup>(FLAGS_task_group_ntags);
    return 0;
}

void UBShmEndpoint::GlobalRelease() {
    for (int i = 0; i < FLAGS_task_group_ntags; ++i) {
        PollingModeRelease(i);
    }
}

std::vector<UBShmEndpoint::PollerGroup> UBShmEndpoint::_poller_groups;

int UBShmEndpoint::PollingModeInitialize(bthread_tag_t tag,
                                        std::function<void()> callback,
                                        std::function<void()> init_fn,
                                        std::function<void()> release_fn) {
    auto& group = _poller_groups[tag];
    auto& pollers = group.pollers;
    auto& running = group.running;
    bool expected = false;
    if (!running.compare_exchange_strong(expected, true)) {
        return 0;
    }
    struct FnArgs {
        Poller* poller;
        std::atomic<bool>* running;
    };
    auto fn = [](void* p) -> void* {
        std::unique_ptr<FnArgs> args(static_cast<FnArgs*>(p));
        auto poller = args->poller;
        auto running = args->running;
        std::unordered_set<CqSidOp, CqSidOpHash, CqSidOpEqual> cq_sids;
        CqSidOp op;

        if (poller->init_fn) {
            poller->init_fn();
        }
        while (running->load(std::memory_order_relaxed)) {
            while (poller->op_queue.Dequeue(op)) {
                if (op.type == CqSidOp::ADD) {
                    cq_sids.emplace(op);
                } else if (op.type == CqSidOp::REMOVE) {
                    cq_sids.erase(op);
                    
                } else if (op.type == CqSidOp::MOD) {
                    cq_sids.erase(op);
                    cq_sids.emplace(op);
                }
            }
            for (auto cq : cq_sids) {
                SocketUniquePtr s;
                if (Socket::Address(cq.sid, &s) < 0) {
                    continue;
                }
                UBShmEndpoint* ep = static_cast<UBShmEndpoint*>(s->user());
                if (!ep) {
                    continue;
                }

                if (cq.event & EPOLLIN) {
                    PollIn(ep, cq.event);
                }

                if (cq.event & EPOLLOUT) {
                    PollOut(ep, cq.event);
                }
            }
            if (poller->callback) {
                poller->callback();
            }
            if (FLAGS_ub_poller_yield) {
                bthread_yield();
            }
        }

        if (poller->release_fn) {
            poller->release_fn();
        }

        return nullptr;
    };
    for (int i = 0; i < FLAGS_ub_poller_num; ++i) {
        auto args = new FnArgs{&pollers[i], &running};
        auto attr = FLAGS_ub_disable_bthread ? BTHREAD_ATTR_PTHREAD
                                               : BTHREAD_ATTR_NORMAL;
        attr.tag = tag;
        bthread_attr_set_name(&attr, "UBPolling");
        pollers[i].callback = callback;
        pollers[i].init_fn = init_fn; 
        pollers[i].release_fn = release_fn;
        auto rc = bthread_start_background(&pollers[i].tid, &attr, fn, args);
        if (rc != 0) {
            LOG(ERROR) << "Fail to start ubring polling bthread";
            return -1;
        }
    }
    return 0;
}

void UBShmEndpoint::PollingModeRelease(bthread_tag_t tag) {
    auto& group = _poller_groups[tag];
    auto& pollers = group.pollers;
    auto& running = group.running;
    running.store(false, std::memory_order_relaxed);
    for (int i = 0; i < FLAGS_ub_poller_num; ++i) {
        bthread_join(pollers[i].tid, NULL);
    }
}

void UBShmEndpoint::PollerRegisterEvent(CqSidOp::OpType op, uint32_t events) {
    auto index = butil::fmix32(_cq_sid) % FLAGS_ub_poller_num;
    auto& group = _poller_groups[bthread_self_tag()];
    auto& pollers = group.pollers;
    auto& poller = pollers[index];
    if (INVALID_SOCKET_ID != _cq_sid) {
        poller.op_queue.Enqueue(CqSidOp{_cq_sid, events, op});
    }
}

}  // namespace ubring
}  // namespace brpc

#endif  // if BRPC_WITH_UBRING
