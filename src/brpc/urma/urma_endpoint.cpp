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

#include "brpc/urma/urma_endpoint.h"

#if BRPC_WITH_URMA

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/sys_byteorder.h"
#include "butil/time.h"
#include "bthread/bthread.h"
#include "bthread/butex.h"

#include "urma_api.h"

#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_bonding.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"

DECLARE_int32(task_group_ntags);

namespace brpc {
namespace urma {

// Flags used here are declared in urma_endpoint.h (urma_use_polling,
// urma_poller_num, urma_disable_bthread). Declare the rest here.
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_cq_batch_dispatch);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_bool(urma_poller_yield);

// ---- Constants shared with the handshake module ----
static const int WAIT_TIMEOUT_MS = 50;
static const size_t HELLO_ACK_LEN = 4;
static const uint32_t HELLO_ACK_URMA_OK = 0x1;
static const size_t IOBUF_BLOCK_HEADER_LEN = sizeof(butil::IOBuf::Block);
static const size_t URMA_READ_THRESHOLD = 4096;
static const uint32_t URMA_READ_IMM_TAG = 0xA5000000U;
static const uint32_t URMA_READ_IMM_MASK = 0xFFFF0000U;
static const uint8_t URMA_READ_PROTOCOL_VERSION = 1;
static const size_t URMA_READ_CONTROL_SIZE = 56;
enum UrmaReadControlType : uint8_t {
    URMA_READ_READY = 1,
    URMA_READ_OFFER = 2,
    URMA_READ_DONE = 3
};

static uint64_t HostToNet64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (static_cast<uint64_t>(butil::HostToNet32(value & 0xffffffffU)) << 32) |
           butil::HostToNet32(value >> 32);
#else
    return value;
#endif
}

static uint64_t NetToHost64(uint64_t value) {
    return HostToNet64(value);
}

static uint64_t MakeWrContext(uint16_t slot, uint8_t kind) {
    return (static_cast<uint64_t>(slot + 1) << 8) | kind;
}

static uint16_t WrContextSlot(uint64_t ctx) {
    return static_cast<uint16_t>((ctx >> 8) - 1);
}

static uint8_t WrContextKind(uint64_t ctx) {
    return static_cast<uint8_t>(ctx & 0xff);
}

// ---- Globals: prepared jetty pool + poller groups ----
struct PreparedJetty {
    UrmaResource* res;
};
static butil::Mutex g_prepared_mutex;
static UrmaResource* g_prepared_list = nullptr;  // singly-linked
static int g_prepared_cnt = 0;
// A failed connection can disappear while the peer still has a READ issued
// against our registered pool, or while local WRs still reference buffers.
// The provider API in this transport has no cross-peer DMA-drain proof, so
// retain those buffers until process teardown instead of recycling them.
struct QuarantinedUrmaResource {
    UrmaResource* resource;
    std::vector<butil::IOBuf> send;
    std::vector<butil::IOBuf> recv;
    butil::IOBuf tx;
    butil::IOBuf rx;
    butil::IOBuf control[3];
};
// Deliberately process-lifetime. Each normal entry pins a pair of READ slots,
// so admission is bounded by the configured READ arena, including controls
// and receive buffers retained with each entry.
static butil::Mutex g_urma_dma_quarantine_mutex;
static std::vector<QuarantinedUrmaResource*>* g_urma_dma_quarantine =
    new std::vector<QuarantinedUrmaResource*>;

static int PreparedJettyCount() {
    const int requested =
        std::max(0, std::min(FLAGS_urma_prepared_jetty_cnt, 1024));
    if (requested == 0) {
        return 0;
    }

    struct rlimit nofile;
    if (getrlimit(RLIMIT_NOFILE, &nofile) != 0 ||
        nofile.rlim_cur == RLIM_INFINITY) {
        return requested;
    }

    // In event mode each prepared JFCE consumes a file descriptor. Keep room
    // for one TCP fd per future URMA connection and for brpc/system internals.
    static const rlim_t kReservedFdCount = 64;
    const rlim_t max_prepared =
        nofile.rlim_cur > kReservedFdCount
            ? (nofile.rlim_cur - kReservedFdCount) / 2
            : 0;
    if (max_prepared >= static_cast<rlim_t>(requested)) {
        return requested;
    }

    LOG(WARNING) << "Cap URMA prepared jetty count from " << requested
                 << " to " << max_prepared
                 << " due to RLIMIT_NOFILE=" << nofile.rlim_cur;
    return static_cast<int>(max_prepared);
}

std::vector<UrmaEndpoint::PollerGroup> UrmaEndpoint::_poller_groups;

// ============================================================================
// UrmaResource lifecycle.
// ============================================================================

bool UrmaResource::Destroy() {
    // Stop at the first failure: later objects may still be referenced by
    // the provider. The endpoint retains buffers together with this graph.
#define RELEASE_URMA_OBJECT(member, release) \
    if (member) { \
        const urma_status_t status = release(member); \
        if (status != URMA_SUCCESS) { \
            LOG(ERROR) << #release << " failed: " << status; \
            return false; \
        } \
        member = nullptr; \
    }
    RELEASE_URMA_OBJECT(remote_jetty, urma_unimport_jetty);
    RELEASE_URMA_OBJECT(jetty, urma_delete_jetty);
    RELEASE_URMA_OBJECT(jfr, urma_delete_jfr);
    RELEASE_URMA_OBJECT(remote_seg, urma_unimport_seg);
    RELEASE_URMA_OBJECT(jfc, urma_delete_jfc);
    RELEASE_URMA_OBJECT(jfce, urma_delete_jfce);
#undef RELEASE_URMA_OBJECT
    return true;
}

UrmaResource::~UrmaResource() {
    Destroy();
}

// ============================================================================
// Constructor / destructor / Reset.
// ============================================================================

UrmaEndpoint::UrmaEndpoint(Socket* s)
    : _socket(s),
      _state(UNINIT),
      _handshake_version(0),
      _resource(nullptr) {
    _sq_size = static_cast<uint16_t>(
        std::max(16, std::min(4096, static_cast<int>(FLAGS_urma_sq_size))));
    _rq_size = static_cast<uint16_t>(
        std::max(16, std::min(4096, static_cast<int>(FLAGS_urma_rq_size))));
    _read_butex = bthread::butex_create_checked<butil::atomic<int>>();
    _read_butex->store(0, butil::memory_order_relaxed);
    _input_processor.Init(s, InputMessengerProcessor::STREAM_URMA_JETTY);
}

UrmaEndpoint::~UrmaEndpoint() {
    DeallocateResources();
    if (_read_butex) {
        bthread::butex_destroy(_read_butex);
        _read_butex = nullptr;
    }
}

void UrmaEndpoint::Reset() {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    DeallocateResources();
    _state = UNINIT;
    _handshake_version = 0;
    _remote_recv_block_size = 0;
    _local_window_capacity = 0;
    _remote_window_capacity = 0;
    _remote_rq_window_size.store(0, butil::memory_order_relaxed);
    _sq_window_size.store(0, butil::memory_order_relaxed);
    _new_rq_wrs.store(0, butil::memory_order_relaxed);
    _sq_imm_window_size = 0;
    _sq_current = 0;
    _rq_received = 0;
    _pending_received_bytes.store(0, butil::memory_order_relaxed);
    _input_processor.Reset();
    _sbuf.clear();
    _sq_slot_in_use.clear();
    _rbuf.clear();
    _rbuf_data.clear();
    _read_butex->store(0, butil::memory_order_relaxed);
    _read_path_iobufs.clear();
    _read_tx_buf.clear();
    _read_rx_buf.clear();
    _read_tx_data = nullptr;
    _read_rx_data = nullptr;
    _read_peer_epoch = _read_tx_epoch = 0;
    _read_tx_seq = _read_rx_seq = 0;
    _read_tx_offset = _read_rx_offset = 0;
    _remote_segment_va = _remote_segment_len = 0;
    _read_max_chunk = _read_peer_max_chunk = _read_tx_length = 0;
    _read_local_ready = _read_peer_ready = _read_tx_credit = false;
    _read_tx_offer_pending = false;
    _read_rx_active = false;
    _read_pending_seq = _read_pending_offset = 0;
    _read_pending_length = 0;
    _read_slot_capacity = 0;
    _read_pending_segment_offset = 0;
    _closing = _post_uncertain = false;
    _read_ready_pending = _read_post_pending = _read_done_pending = false;
    _read_ack_pending = _read_early_offer = false;
    _sq_control_type.clear();
    for (size_t i = 0; i < 3; ++i) {
        _read_control_buf[i].clear();
        _read_control_data[i] = nullptr;
        _read_control_busy[i] = false;
    }
}

// ============================================================================
// Handshake IO helpers (ReadFromFd / WriteToFd / PushBackToReadBuf).
// Modeled on RdmaEndpoint::ReadFromFdLoop / WriteToFdLoop.
// ============================================================================

int UrmaEndpoint::ReadFromFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t received = 0;
    while (received < len) {
        const int expected_val = _read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nr = read(fd, p + received, len - received);
        if (nr < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                int rc = bthread::butex_wait(_read_butex, expected_val, &duetime);
                if (rc < 0 && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        if (nr == 0) {
            errno = EEOF;
            return -1;
        }
        received += nr;
    }
    return 0;
}

void UrmaEndpoint::PushBackToReadBuf(const void* data, size_t len) {
    _socket->fd_input_processor().read_buf().append(data, len);
}

int UrmaEndpoint::WriteToFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = write(fd, p + written, len - written);
        if (nw >= 0) {
            written += nw;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

// ============================================================================
// Hello builders / parsers.
// ============================================================================

void UrmaEndpoint::MakeLocalParsedHello(ParsedHello* out) const {
    *out = ParsedHello{};  // value-initialize (avoids memset on non-trivial type)
    out->buffer_size = static_cast<uint32_t>(GetUrmaRecvBlockSize());
    out->recv_buffer_cnt = _rq_size - 1;
    if (_resource && _resource->jetty) {
        out->jetty_id = _resource->jetty->jetty_id.id;
        out->uasid = _resource->jetty->jetty_id.uasid;
        const urma_eid_t* local_eid = GetUrmaLocalEid();
        const uint8_t* advertised_eid =
            local_eid != nullptr
                ? local_eid->raw
                : _resource->jetty->jetty_id.eid.raw;
        std::memcpy(out->eid, advertised_eid, 16);
    }
    out->tp_type = static_cast<uint8_t>(URMA_CTP);
    // Pool segment: flatten g_pool_seg's seg fields.
    urma_target_seg_t* pool = GetPoolSegFor(nullptr);
    if (pool) {
        std::memcpy(out->seg_eid, pool->seg.ubva.eid.raw, 16);
        out->seg_uasid = pool->seg.ubva.uasid;
        out->seg_va = pool->seg.ubva.va;
        out->seg_len = pool->seg.len;
        out->seg_token_id = pool->seg.token_id;
    }
}

void UrmaEndpoint::FillLocalHelloV2(v2_wire::HelloMessage* out) const {
    *out = v2_wire::HelloMessage{};  // value-initialize
    out->msg_len = v2_wire::HELLO_PACKET_LEN;
    out->hello_ver = v2_wire::HELLO_V2_VERSION;
    out->impl_ver = v2_wire::IMPL_V2_VERSION;
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->buffer_size = p.buffer_size;
    out->recv_buffer_cnt = p.recv_buffer_cnt;
    out->jetty_id = p.jetty_id;
    std::memcpy(out->eid, p.eid, 16);
    out->uasid = p.uasid;
    out->tp_type = p.tp_type;
    std::memcpy(out->seg_eid, p.seg_eid, 16);
    out->seg_uasid = p.seg_uasid;
    out->seg_va = p.seg_va;
    out->seg_len = p.seg_len;
    out->seg_token_id = p.seg_token_id;
}

void UrmaEndpoint::FillLocalHelloV3(UrmaHello* out) const {
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->set_buffer_size(p.buffer_size);
    out->set_recv_buffer_cnt(p.recv_buffer_cnt);
    out->set_jetty_id(p.jetty_id);
    out->set_eid(p.eid, 16);
    out->set_uasid(p.uasid);
    out->set_tp_type(p.tp_type);
    out->set_seg_eid(p.seg_eid, 16);
    out->set_seg_uasid(p.seg_uasid);
    out->set_seg_va(p.seg_va);
    out->set_seg_len(p.seg_len);
    out->set_seg_token_id(p.seg_token_id);
}

int UrmaEndpoint::WriteHelloV3(const UrmaHello& msg) {
    butil::IOBuf packet;
    packet.append("URM3", 4);
    std::string body;
    if (!msg.SerializeToString(&body)) {
        LOG(ERROR) << "Fail to serialize UrmaHello";
        return -1;
    }
    uint32_t pb_size_be = butil::HostToNet32(static_cast<uint32_t>(body.size()));
    packet.append(&pb_size_be, sizeof(pb_size_be));
    packet.append(body);
    return WriteToFd(packet);
}

int UrmaEndpoint::WriteToFd(butil::IOBuf& data) {
    // Write out the IOBuf in a single WriteToFd-style loop.
    while (!data.empty()) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = data.cut_into_file_descriptor(fd);
        if (nw >= 0) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

int UrmaEndpoint::ReadAndParseHelloV3(ParsedHello* out, bool* negotiated) {
    *negotiated = false;
    uint32_t pb_size_be = 0;
    if (ReadFromFd(&pb_size_be, sizeof(pb_size_be)) < 0) {
        return -1;
    }
    const uint32_t pb_size = butil::NetToHost32(pb_size_be);
    if (pb_size == 0 || pb_size > 4096) {
        return 0;
    }
    std::string body(pb_size, '\0');
    if (ReadFromFd(&body[0], pb_size) < 0) {
        return -1;
    }
    UrmaHello msg;
    if (!msg.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
        return 0;
    }
    if (msg.eid().size() != 16 || msg.seg_eid().size() != 16) {
        return 0;
    }
    out->buffer_size = msg.buffer_size();
    out->recv_buffer_cnt = msg.recv_buffer_cnt();
    out->jetty_id = msg.jetty_id();
    std::memcpy(out->eid, msg.eid().data(), 16);
    out->uasid = msg.uasid();
    out->tp_type = static_cast<uint8_t>(msg.tp_type());
    std::memcpy(out->seg_eid, msg.seg_eid().data(), 16);
    out->seg_uasid = msg.seg_uasid();
    out->seg_va = msg.seg_va();
    out->seg_len = msg.seg_len();
    out->seg_token_id = msg.seg_token_id();
    if (!ValidHello(*out)) {
        return 0;
    }
    *negotiated = true;
    return 0;
}

// ============================================================================
// Allocate / deallocate per-connection resources.
// ============================================================================

int UrmaEndpoint::AllocateResources() {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || !IsUrmaAvailable()) {
        errno = ENODEV;
        return -1;
    }
    if (_resource) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    _resource = new (std::nothrow) UrmaResource();
    if (!_resource) {
        return -1;
    }

    // Try the prepared pool first (sized sq/rq match).
    if (_sq_size <= static_cast<uint16_t>(FLAGS_urma_sq_size) &&
        _rq_size <= static_cast<uint16_t>(FLAGS_urma_rq_size)) {
        BAIDU_SCOPED_LOCK(g_prepared_mutex);
        if (g_prepared_list) {
            UrmaResource* next = g_prepared_list->next;
            delete _resource;
            _resource = g_prepared_list;
            g_prepared_list = next;
            _resource->next = nullptr;
            --g_prepared_cnt;
        }
    }

    if (!_resource->jfc) {
        // The SDK requires every JFC to reference a JFCE. Polling mode does
        // not arm or consume it, but still supplies the required object.
        _resource->jfce = urma_create_jfce(ctx);
        if (!_resource->jfce ||
            (!FLAGS_urma_use_polling && _resource->jfce->fd < 0)) {
            LOG(ERROR) << "Fail to create a usable URMA JFCE";
            errno = ENODEV;
            return -1;
        }

        urma_jfc_cfg_t jfc_cfg{};
        jfc_cfg.depth = static_cast<uint32_t>(_sq_size + _rq_size);
        jfc_cfg.jfce = _resource->jfce;
        _resource->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!_resource->jfc) {
            PLOG(ERROR) << "urma_create_jfc";
            return -1;
        }

        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(_rq_size);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge = 1;
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = _resource->jfc;
        _resource->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!_resource->jfr) {
            PLOG(ERROR) << "urma_create_jfr";
            return -1;
        }

        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = _resource->jfc;
        jetty_cfg.shared.jfr = _resource->jfr;
        jetty_cfg.shared.jfc = _resource->jfc;
        _resource->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!_resource->jetty) {
            PLOG(ERROR) << "urma_create_jetty";
            return -1;
        }
    }

    _sbuf.resize(_sq_size - RESERVED_WR_NUM);
    _sq_slot_in_use.assign(_sq_size - RESERVED_WR_NUM, 0);
    _rbuf.resize(_rq_size);
    _rbuf_data.resize(_rq_size, nullptr);

    _sq_control_type.assign(_sbuf.size(), 0);
    // Dedicated slots share the already advertised pool segment. Their
    // capacity is independent of the ordinary receive/IOBuf block size.
    const uint32_t device_max_read = GetUrmaMaxReadSize();
    if (device_max_read < URMA_READ_THRESHOLD) {
        LOG(ERROR) << "URMA device cannot carry a 4 KiB READ";
        errno = ENOTSUP;
        return -1;
    }
    if (!AllocateReadSlotPair(&_read_tx_buf, &_read_tx_data,
                             &_read_rx_buf, &_read_rx_data,
                             &_read_slot_capacity)) {
        PLOG(ERROR) << "Unable to reserve URMA READ slot pair";
        return -1;
    }
    _read_max_chunk = std::min(_read_slot_capacity, device_max_read);
    for (size_t i = 0; i < 3; ++i) {
        if (!AllocateReadControlBuffer(&_read_control_buf[i],
                                       &_read_control_data[i],
                                       URMA_READ_CONTROL_SIZE)) {
            PLOG(ERROR) << "Unable to reserve URMA READ control buffer";
            return -1;
        }
    }
    _read_tx_epoch = static_cast<uint64_t>(butil::cpuwide_time_us()) ^
                     reinterpret_cast<uintptr_t>(this);
    if (_read_tx_epoch == 0) _read_tx_epoch = 1;

    // Wrap the JFCE fd in a brpc Socket so PollCq is driven by epoll.
    if (!FLAGS_urma_use_polling) {
        if (!_resource->jfce || _resource->jfce->fd < 0) {
            LOG(ERROR) << "Prepared URMA resource has no usable JFCE";
            errno = ENODEV;
            return -1;
        }
        if (ReqNotifyCq() != 0) {
            return -1;
        }
        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->keytable_pool();
        options.fd = _resource->jfce->fd;
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket";
            return -1;
        }
    } else {
        // Polling mode: synthetic carrier socket (no fd).
        SocketOptions options;
        options.user = this;
        options.keytable_pool = _socket->keytable_pool();
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket (polling)";
            return -1;
        }
        PollerAddCqSid();
    }
    return 0;
}

void UrmaEndpoint::DeallocateResources() {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    _closing = true;
    if (!_resource) {
        return;
    }
    bool retain = _post_uncertain || _read_tx_offer_pending ||
                  (_read_rx_active && !_read_post_pending) ||
                  (_local_window_capacity != 0 &&
                   _sq_imm_window_size < RESERVED_WR_NUM) || _cq_active != 0;
    for (size_t i = 0; i < _sq_slot_in_use.size(); ++i) {
        retain = retain || _sq_slot_in_use[i];
    }
    if (FLAGS_urma_use_polling) {
        PollerRemoveCqSid();
    }

    // Tear down the CQ socket so the EventDispatcher stops calling PollCq.
    if (_cq_sid != INVALID_SOCKET_ID) {
        SocketUniquePtr s;
        if (Socket::Address(_cq_sid, &s) == 0) {
            if (s->fd() >= 0) {
                s->_io_event.RemoveConsumer(s->_fd);
            }
            s->_user = nullptr;  // Do not release user (this UrmaEndpoint).
            s->_fd = -1;  // Already removed fd from epoll.
            s->SetFailed();
        }
        _cq_sid = INVALID_SOCKET_ID;
    }

    // Reusing a Jetty requires a driver-supported RESET plus a complete JFC
    // drain. Until that lifecycle is implemented, prepared resources are
    // one-shot: they accelerate connection setup but are destroyed on close.
    if (!retain) retain = !_resource->Destroy();
    if (retain) {
        auto* q = new QuarantinedUrmaResource;
        q->resource = _resource;
        q->send.swap(_sbuf);
        q->recv.swap(_rbuf);
        q->tx.swap(_read_tx_buf);
        q->rx.swap(_read_rx_buf);
        for (size_t i = 0; i < 3; ++i) {
            q->control[i].swap(_read_control_buf[i]);
        }
        // No unbounded admission for a failed setup lacking a READ pair.
        if (q->tx.empty() || q->rx.empty()) GlobalDisableUrma();
        BAIDU_SCOPED_LOCK(g_urma_dma_quarantine_mutex);
        g_urma_dma_quarantine->push_back(q);
        LOG(WARNING) << "Retain URMA resource graph and DMA buffers on close; "
                     << "quarantined endpoints=" << g_urma_dma_quarantine->size();
    } else {
        delete _resource;
    }
    _resource = nullptr;
}

// ============================================================================
// ImportPeer: the critical import_seg-before-import_jetty sequence.
// ============================================================================

int UrmaEndpoint::ImportPeer(const ParsedHello& peer) {
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    // 1. urma_import_seg FIRST so the kernel establishes TP routing for the
    //    remote EID. Without this the first SEND is rejected by hardware with
    //    URMA_CR_RNR_RETRY_CNT_EXC_ERR.
    urma_seg_t peer_seg{};
    std::memcpy(peer_seg.ubva.eid.raw, peer.seg_eid, 16);
    peer_seg.ubva.uasid = peer.seg_uasid;
    peer_seg.ubva.va = peer.seg_va;
    peer_seg.len = peer.seg_len;
    peer_seg.token_id = peer.seg_token_id;
    urma_token_t seg_token{};
    urma_import_seg_flag_t seg_flag{};
    seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
    seg_flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    seg_flag.bs.mapping = URMA_SEG_NOMAP;
    _resource->remote_seg = urma_import_seg(ctx, &peer_seg, &seg_token, 0, seg_flag);
    if (!_resource->remote_seg) {
        PLOG(ERROR) << "urma_import_seg failed";
        return -1;
    }

    // 2. urma_import_jetty.
    urma_rjetty_t remote{};
    std::memcpy(remote.jetty_id.eid.raw, peer.eid, 16);
    remote.jetty_id.uasid = peer.uasid;
    remote.jetty_id.id = peer.jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    if (peer.tp_type > static_cast<uint8_t>(URMA_UTP)) {
        errno = EPROTO;
        return -1;
    }
    remote.tp_type = static_cast<urma_tp_type_t>(peer.tp_type);

    urma_token_t token{};
    const bool use_bonding_extension =
        IsUrmaBondingDevice() && remote.trans_mode == URMA_TM_RM;
    errno = 0;
    if (use_bonding_extension) {
#if BRPC_URMA_HAS_BONDING_EXT
        // The bonding provider needs the local jetty to associate its send
        // path with the imported target. A plain import may return success
        // without setting that association, leaving traffic one-way only.
        bondp_rjetty_t bonding_remote{};
        bonding_remote.base = remote;
        bonding_remote.base.flag.bs.has_drv_ext = 1;
        bonding_remote.jetty = _resource->jetty;
        _resource->remote_jetty =
            urma_import_jetty(ctx, &bonding_remote.base, &token);
#else
        LOG(ERROR) << "Bonding remote jetty import requires provider header "
                      "urma_ubagg.h";
        errno = ENOTSUP;
#endif
    } else {
        _resource->remote_jetty = urma_import_jetty(ctx, &remote, &token);
    }
    if (!_resource->remote_jetty) {
        if (errno == 0) {
            errno = EIO;
        }
        char remote_eid[URMA_EID_STR_LEN + 1] = {};
        std::snprintf(remote_eid, sizeof(remote_eid), EID_FMT,
                      EID_RAW_ARGS(peer.eid));
        PLOG(ERROR) << "urma_import_jetty failed"
                    << " remote_eid=" << remote_eid
                    << " remote_uasid=" << peer.uasid
                    << " remote_jetty_id=" << peer.jetty_id
                    << " trans_mode=" << remote.trans_mode
                    << " tp_type=" << remote.tp_type
                    << " bonding_extension=" << use_bonding_extension;
        return -1;
    }
    return 0;
}

// ============================================================================
// Send / recv data path.
// ============================================================================

// Private IOBuf accessor mirroring RdmaIOBuf: reach into IOBuf block refs to
// build a urma_sge_t directly, without memcpy.
class UrmaIOBuf : private butil::IOBuf {
    friend class ::brpc::urma::UrmaEndpoint;
public:
    using butil::IOBuf::_ref_num;
    using butil::IOBuf::_ref_at;
    using butil::IOBuf::fetch1;
    using butil::IOBuf::get_first_data_meta;
    using butil::IOBuf::cutn;
    // Build the SGE for the current head block.
    // Returns bytes added, or -1 (errno set).
    ssize_t cut_into_sglist(urma_sge_t* sglist, size_t* sge_index,
                            butil::IOBuf* to, size_t max_sge,
                            size_t max_len) {
        size_t len = 0;
        while (*sge_index < max_sge && len < max_len && _ref_num() != 0) {
            butil::IOBuf::BlockRef const& r = _ref_at(0);
            const void* start = fetch1();
            urma_target_seg_t* tseg =
                GetPoolSegFor(const_cast<void*>(start));
            if (!tseg) {
                // User-registered memory: look up the seg handle.
                uint64_t meta = get_first_data_meta();
                if (meta != 0) {
                    tseg = reinterpret_cast<urma_target_seg_t*>(
                        static_cast<uintptr_t>(meta));
                }
            }
            if (!tseg) {
                errno = ERDMAMEM;
                return -1;
            }
            size_t this_len = r.length;
            if (len + this_len > max_len) {
                this_len = max_len - len;
            }
            sglist[*sge_index].addr = reinterpret_cast<uint64_t>(start);
            sglist[*sge_index].len = static_cast<uint32_t>(this_len);
            sglist[*sge_index].tseg = tseg;
            cutn(to, this_len);
            len += this_len;
            (*sge_index)++;
        }
        return static_cast<ssize_t>(len);
    }
};

ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || _post_uncertain || !_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        if (_read_tx_offer_pending || _read_control_busy[URMA_READ_OFFER - 1]) {
            errno = EAGAIN;
            return -1;
        }
    }
    int max_sge = GetUrmaMaxSge();
    if (max_sge < 1) {
        max_sge = 1;
    }

    urma_sge_t* sglist = static_cast<urma_sge_t*>(
        alloca(sizeof(urma_sge_t) * max_sge));
    if (!sglist) {
        errno = ENOMEM;
        return -1;
    }

    size_t current = 0;
    ssize_t total_len = 0;
    while (current < ndata) {
        butil::IOBuf* request = from[current];
        bool read_path = false;
        {
            std::unique_lock<butil::Mutex> lock(_read_mutex);
            auto it = _read_path_iobufs.find(request);
            if (it != _read_path_iobufs.end()) {
                read_path = true;
                if (request->empty()) _read_path_iobufs.erase(it);
            } else if (request->size() >= URMA_READ_THRESHOLD) {
                _read_path_iobufs.insert(request);
                read_path = true;
            }
        }
        if (read_path && !request->empty()) {
            uint32_t length = 0;
            uint64_t seq = 0, stream_offset = 0, segment_offset = 0;
            {
                std::unique_lock<butil::Mutex> lock(_read_mutex);
                if (!_read_local_ready || !_read_peer_ready ||
                    !_read_tx_credit || _read_tx_offer_pending) {
                    if (total_len > 0) break;
                    errno = EAGAIN;
                    return -1;
                }
                length = static_cast<uint32_t>(std::min<size_t>(
                    request->size(), std::min(_read_slot_capacity,
                                               _read_peer_max_chunk)));
                seq = _read_tx_seq;
                stream_offset = _read_tx_offset;
                if (seq == UINT64_MAX || stream_offset > UINT64_MAX - length) {
                    errno = EOVERFLOW;
                    return -1;
                }
                if (!GetPoolSegmentOffset(_read_tx_data, length,
                                          &segment_offset)) {
                    errno = ERDMAMEM;
                    return -1;
                }
                if (length == 0 || request->copy_to(_read_tx_data, length) != length) {
                    errno = EIO;
                    return -1;
                }
                _read_tx_length = length;
                _read_tx_credit = false;
                _read_tx_offer_pending = true;
            }
            if (SendReadControl(URMA_READ_OFFER, seq, stream_offset,
                                segment_offset, length, 0) < 0) {
                std::unique_lock<butil::Mutex> lock(_read_mutex);
                if (!_post_uncertain) {
                    _read_tx_credit = true;
                    _read_tx_offer_pending = false;
                }
                if (errno == EAGAIN && total_len > 0) break;
                return -1;
            }
            request->pop_front(length);
            total_len += length;
            if (request->empty()) {
                std::unique_lock<butil::Mutex> lock(_read_mutex);
                _read_path_iobufs.erase(request);
            }
            break;  // Preserve byte-stream ordering until READ_DONE.
        }
        uint16_t remote_wnd = _remote_rq_window_size.load(butil::memory_order_relaxed);
        uint16_t sq_wnd = _sq_window_size.load(butil::memory_order_relaxed);
        if (remote_wnd <= READ_CONTROL_RESERVE ||
            sq_wnd <= READ_CONTROL_RESERVE) {
            if (total_len > 0) {
                break;
            }
            errno = EAGAIN;
            return -1;
        }
        uint16_t sq_slot = 0;
        if (!ReserveSqSlot(&sq_slot)) {
            if (total_len > 0) break;
            errno = EAGAIN;
            return -1;
        }
        butil::IOBuf* to = &_sbuf[sq_slot];
        size_t sge_index = 0;
        size_t this_len = 0;
        size_t max_len = _remote_recv_block_size > 0
                             ? _remote_recv_block_size
                             : GetUrmaRecvBlockSize();
        // The legacy path is intentionally kept within the 4 KiB SEND WR
        // limit even when several small WriteRequests are coalesced here.
        max_len = std::min(max_len, URMA_READ_THRESHOLD);
        while (sge_index < static_cast<size_t>(max_sge) &&
               this_len < max_len && current < ndata) {
            bool pinned_read_path = false;
            {
                std::unique_lock<butil::Mutex> lock(_read_mutex);
                pinned_read_path =
                    _read_path_iobufs.find(from[current]) !=
                    _read_path_iobufs.end();
            }
            if (from[current]->size() >= URMA_READ_THRESHOLD ||
                pinned_read_path) {
                break;
            }
            auto* data = reinterpret_cast<UrmaIOBuf*>(from[current]);
            if (data->empty()) {
                ++current;
                continue;
            }
            ssize_t n = data->cut_into_sglist(sglist, &sge_index, to,
                                              max_sge, max_len - this_len);
            if (n < 0) {
                to->clear();
                ReleaseSqSlot(sq_slot);
                return -1;
            }
            this_len += n;
        }
        if (sge_index == 0) {
            ReleaseSqSlot(sq_slot);
            break;
        }
        if (_read_tx_offset > UINT64_MAX - this_len) {
            to->clear();
            ReleaseSqSlot(sq_slot);
            errno = EOVERFLOW;
            return -1;
        }

        urma_sg_t sg{sglist, static_cast<uint32_t>(sge_index)};
        urma_jfs_wr_t wr{};
        std::memset(&wr, 0, sizeof(wr));
        // Send payload with URMA_OPC_SEND. Receive credits are flushed
        // separately by SendImm() after SendAck() reaches its threshold.
        // Piggybacking credits turns every payload into SEND_IMM and can
        // produce asymmetric completions with the bonding provider.
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.flag.bs.comp_order = 1;
        wr.flag.bs.place_order = 2;
        wr.tjetty = _resource->remote_jetty;
        wr.send.src = sg;
        wr.user_ctx = MakeWrContext(sq_slot, 1);
        urma_jfs_wr_t* bad_wr = nullptr;
        const uint32_t local_jetty_id = _resource->jetty->jetty_id.id;
        const uint32_t remote_jetty_id = _resource->remote_jetty->id.id;

        // Reserve both credits before making the WR visible to the provider.
        // In polling mode a completion (and even the peer's receive-credit
        // ACK) can be processed by another thread before post_send returns.
        // Decrementing after post therefore creates a transient capacity + 1
        // window and makes the strict credit check tear down a healthy
        // connection.
        _remote_rq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        _sq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        int rc = urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr);
        if (rc != URMA_SUCCESS) {
            const int provider_errno = errno;
            if (bad_wr == &wr) {
                _remote_rq_window_size.fetch_add(1, butil::memory_order_relaxed);
                _sq_window_size.fetch_add(1, butil::memory_order_relaxed);
                ReleaseSqSlot(sq_slot);
            } else {
                _post_uncertain = true;
            }
            LOG(WARNING) << "urma_post_jetty_send_wr failed: " << rc
                         << ", provider_errno=" << provider_errno
                         << " (" << berror(provider_errno) << ')'
                         << ", bad_wr=" << static_cast<const void*>(bad_wr)
                         << ", bad_is_current=" << (bad_wr == &wr)
                         << ", sq_slot=" << sq_slot
                         << ", local_jetty_id=" << local_jetty_id
                         << ", remote_jetty_id=" << remote_jetty_id
                         << ", state=" << GetStateStr()
                         << ", sq_window=" << sq_wnd
                         << ", remote_rq_window=" << remote_wnd
                         << ", num_sge=" << sge_index
                         << ", configured_max_sge=" << GetUrmaMaxSge()
                         << ", payload_size=" << this_len
                         << " on " << _socket->description();
            errno = EIO;  // Input was consumed: never report retryable EAGAIN.
            return -1;
        }
        {
            std::unique_lock<butil::Mutex> lock(_read_mutex);
            _read_tx_offset += this_len;
        }
        total_len += static_cast<ssize_t>(this_len);
    }
    return total_len;
}

bool UrmaEndpoint::ReserveSqSlot(uint16_t* slot) {
    std::unique_lock<butil::Mutex> lock(_sq_slot_mutex);
    const uint16_t capacity = static_cast<uint16_t>(_sq_slot_in_use.size());
    if (capacity == 0) return false;
    for (uint16_t n = 0; n < capacity; ++n) {
        const uint16_t i = (_sq_current + n) % capacity;
        if (!_sq_slot_in_use[i]) {
            _sq_slot_in_use[i] = 1;
            _sq_current = static_cast<uint16_t>((i + 1) % capacity);
            *slot = i;
            return true;
        }
    }
    return false;
}

void UrmaEndpoint::ReleaseSqSlot(uint16_t slot) {
    std::unique_lock<butil::Mutex> lock(_sq_slot_mutex);
    if (slot < _sq_slot_in_use.size()) {
        _sq_slot_in_use[slot] = 0;
    }
}

bool UrmaEndpoint::IsWritable() const {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || _post_uncertain ||
        _read_control_busy[URMA_READ_OFFER - 1]) return false;
    {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        if (!_read_local_ready || !_read_peer_ready ||
            _read_tx_offer_pending) {
            return false;
        }
    }
    return _remote_rq_window_size.load(butil::memory_order_relaxed) > READ_CONTROL_RESERVE &&
           _sq_window_size.load(butil::memory_order_relaxed) > READ_CONTROL_RESERVE;
}

int UrmaEndpoint::SendReadControl(uint8_t type, uint64_t seq,
                                  uint64_t stream_offset,
                                  uint64_t segment_offset, uint32_t length,
                                  uint32_t max_chunk) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    const uint16_t reserve = type == URMA_READ_OFFER ? READ_CONTROL_RESERVE : 0;
    if (_closing || _post_uncertain || !_resource || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    if (type < URMA_READ_READY || type > URMA_READ_DONE) {
        errno = EINVAL;
        return -1;
    }
    if (_read_control_busy[type - 1] ||
        _remote_rq_window_size.load(butil::memory_order_relaxed) <= reserve ||
        _sq_window_size.load(butil::memory_order_relaxed) <= reserve) {
        errno = EAGAIN;
        return -1;
    }
    uint16_t slot = 0;
    if (!ReserveSqSlot(&slot)) {
        errno = EAGAIN;
        return -1;
    }
    uint8_t frame[URMA_READ_CONTROL_SIZE] = {};
    std::memcpy(frame, "URDC", 4);
    frame[4] = URMA_READ_PROTOCOL_VERSION;
    frame[5] = type;
    const uint16_t frame_size = butil::HostToNet16(URMA_READ_CONTROL_SIZE);
    std::memcpy(frame + 6, &frame_size, sizeof(frame_size));
    uint64_t local_epoch;
    uint64_t peer_epoch;
    {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        local_epoch = _read_tx_epoch;
        peer_epoch = type == URMA_READ_READY ? 0 : _read_peer_epoch;
    }
    const uint64_t local_be = HostToNet64(local_epoch);
    const uint64_t peer_be = HostToNet64(peer_epoch);
    const uint64_t seq_be = HostToNet64(seq);
    const uint64_t offset_be = HostToNet64(stream_offset);
    const uint64_t segment_be = HostToNet64(segment_offset);
    const uint32_t length_be = butil::HostToNet32(length);
    const uint32_t max_chunk_be = butil::HostToNet32(max_chunk);
    std::memcpy(frame + 8, &local_be, sizeof(local_be));
    std::memcpy(frame + 16, &peer_be, sizeof(peer_be));
    std::memcpy(frame + 24, &seq_be, sizeof(seq_be));
    std::memcpy(frame + 32, &offset_be, sizeof(offset_be));
    std::memcpy(frame + 40, &segment_be, sizeof(segment_be));
    std::memcpy(frame + 48, &length_be, sizeof(length_be));
    std::memcpy(frame + 52, &max_chunk_be, sizeof(max_chunk_be));

    butil::IOBuf* send_buf = &_sbuf[slot];
    void* data = _read_control_data[type - 1];
    uint64_t ignored = 0;
    if (!GetPoolSegmentOffset(data, sizeof(frame), &ignored)) {
        ReleaseSqSlot(slot);
        errno = ERDMAMEM;
        return -1;
    }
    std::memcpy(data, frame, sizeof(frame));
    *send_buf = _read_control_buf[type - 1];
    urma_target_seg_t* tseg = GetPoolSegFor(data);
    _sq_control_type[slot] = type;
    _read_control_busy[type - 1] = true;
    urma_sge_t sge{reinterpret_cast<uint64_t>(data),
                   static_cast<uint32_t>(sizeof(frame)), tseg, nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.flag.bs.comp_order = 1;
    wr.flag.bs.place_order = 2;
    wr.flag.bs.solicited_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.send.src = sg;
    wr.send.imm_data = URMA_READ_IMM_TAG | type;
    wr.user_ctx = MakeWrContext(slot, 2);
    _remote_rq_window_size.fetch_sub(1, butil::memory_order_relaxed);
    _sq_window_size.fetch_sub(1, butil::memory_order_relaxed);
    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        if (bad == &wr) {
            _remote_rq_window_size.fetch_add(1, butil::memory_order_relaxed);
            _sq_window_size.fetch_add(1, butil::memory_order_relaxed);
            send_buf->clear();
            _sq_control_type[slot] = 0;
            _read_control_busy[type - 1] = false;
            ReleaseSqSlot(slot);
        } else {
            _post_uncertain = true;
        }
        errno = EIO;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::StartReadProtocol() {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    _read_ready_pending = true;
    return ProgressRead();
}

int UrmaEndpoint::ProgressRead() {
    if (_closing || _post_uncertain) {
        errno = EIO;
        return -1;
    }
    if (_read_ack_pending && _sq_imm_window_size > 0) {
        const uint32_t credits = _new_rq_wrs.exchange(0, butil::memory_order_relaxed);
        if (SendImm(credits) < 0) return -1;
        _read_ack_pending = false;
    }
    if (_read_ready_pending) {
        if (SendReadControl(URMA_READ_READY, 0, 0, 0, 0, _read_max_chunk) < 0) {
            return errno == EAGAIN ? 0 : -1;
        }
        _read_ready_pending = false;
        _read_local_ready = true;
        _socket->WakeAsEpollOut();
    }
    uint64_t early_offset_be = 0;
    if (_read_early_offer) {
        std::memcpy(&early_offset_be, _read_early_frame + 32, sizeof(early_offset_be));
    }
    if (_read_early_offer && _read_peer_ready &&
        NetToHost64(early_offset_be) <= _read_rx_offset) {
        uint8_t frame[URMA_READ_CONTROL_SIZE];
        std::memcpy(frame, _read_early_frame, sizeof(frame));
        _read_early_offer = false;
        if (HandleReadControl(frame, sizeof(frame)) < 0) return -1;
    }
    if (_read_post_pending) {
        if (PostRead(_read_pending_segment_offset, _read_pending_length) < 0) {
            return errno == EAGAIN ? 0 : -1;
        }
        _read_post_pending = false;
    }
    if (_read_done_pending) {
        if (SendReadControl(URMA_READ_DONE, _read_pending_seq,
                            _read_pending_offset, 0, _read_pending_length, 1) < 0) {
            if (errno != EAGAIN) return -1;
        } else {
            _read_done_pending = false;
            _read_pending_length = 0;
        }
    }
    return 0;
}

int UrmaEndpoint::PostRead(uint64_t segment_offset, uint32_t length) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || _post_uncertain || !_resource) {
        errno = ENOTCONN;
        return -1;
    }
    if (length == 0 || length > _read_max_chunk ||
        segment_offset > _remote_segment_len ||
        length > _remote_segment_len - segment_offset ||
        _remote_segment_va > UINT64_MAX - segment_offset) {
        errno = EPROTO;
        return -1;
    }
    const uint64_t remote_addr = _remote_segment_va + segment_offset;
    if (remote_addr > UINT64_MAX - length) {
        errno = EPROTO;
        return -1;
    }
    urma_target_seg_t* local_seg = GetPoolSegFor(_read_rx_data);
    uint64_t ignored = 0;
    if (!local_seg || !_resource->remote_seg ||
        !GetPoolSegmentOffset(_read_rx_data, length, &ignored)) {
        errno = ERDMAMEM;
        return -1;
    }
    uint16_t slot = 0;
    if (_sq_window_size.load(butil::memory_order_relaxed) == 0 ||
        !ReserveSqSlot(&slot)) {
        errno = EAGAIN;
        return -1;
    }
    urma_sge_t remote_sge{remote_addr, length, _resource->remote_seg, nullptr};
    urma_sge_t local_sge{reinterpret_cast<uint64_t>(_read_rx_data), length,
                         local_seg, nullptr};
    urma_sg_t src{&remote_sge, 1};
    urma_sg_t dst{&local_sge, 1};
    urma_jfs_wr_t wr{};
    wr.opcode = URMA_OPC_READ;
    wr.flag.bs.complete_enable = 1;
    wr.flag.bs.comp_order = 1;
    wr.flag.bs.place_order = 2;
    wr.tjetty = _resource->remote_jetty;
    wr.rw.src = src;
    wr.rw.dst = dst;
    wr.user_ctx = MakeWrContext(slot, 3);
    _sq_window_size.fetch_sub(1, butil::memory_order_relaxed);
    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        if (bad == &wr) {
            _sq_window_size.fetch_add(1, butil::memory_order_relaxed);
            ReleaseSqSlot(slot);
        } else {
            _post_uncertain = true;
        }
        errno = EIO;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::HandleReadControl(const void* data, size_t length) {
    if (!data || length != URMA_READ_CONTROL_SIZE) {
        errno = EPROTO;
        return -1;
    }
    const uint8_t* frame = static_cast<const uint8_t*>(data);
    uint16_t frame_size_be = 0;
    std::memcpy(&frame_size_be, frame + 6, sizeof(frame_size_be));
    if (std::memcmp(frame, "URDC", 4) != 0 ||
        frame[4] != URMA_READ_PROTOCOL_VERSION ||
        butil::NetToHost16(frame_size_be) != URMA_READ_CONTROL_SIZE) {
        errno = EPROTO;
        return -1;
    }
    uint64_t fields64[5];
    std::memcpy(&fields64[0], frame + 8, 8);
    std::memcpy(&fields64[1], frame + 16, 8);
    std::memcpy(&fields64[2], frame + 24, 8);
    std::memcpy(&fields64[3], frame + 32, 8);
    std::memcpy(&fields64[4], frame + 40, 8);
    const uint64_t sender_epoch = NetToHost64(fields64[0]);
    const uint64_t target_epoch = NetToHost64(fields64[1]);
    const uint64_t seq = NetToHost64(fields64[2]);
    const uint64_t stream_offset = NetToHost64(fields64[3]);
    const uint64_t segment_offset = NetToHost64(fields64[4]);
    uint32_t length_be = 0, max_chunk_be = 0;
    std::memcpy(&length_be, frame + 48, 4);
    std::memcpy(&max_chunk_be, frame + 52, 4);
    const uint32_t payload_length = butil::NetToHost32(length_be);
    const uint32_t max_chunk = butil::NetToHost32(max_chunk_be);

    if (frame[5] == URMA_READ_READY) {
        if (sender_epoch == 0 || target_epoch != 0 || seq != 0 ||
            stream_offset != 0 || segment_offset != 0 || payload_length != 0 ||
            max_chunk < URMA_READ_THRESHOLD) {
            errno = EPROTO;
            return -1;
        }
        {
            std::unique_lock<butil::Mutex> lock(_read_mutex);
            if (_read_peer_ready) {
                errno = EPROTO;
                return -1;
            }
            _read_peer_epoch = sender_epoch;
            _read_peer_max_chunk = max_chunk;
            _read_peer_ready = true;
            _read_tx_credit = true;
        }
        _socket->WakeAsEpollOut();
        return 0;
    }
    if (frame[5] == URMA_READ_OFFER &&
        (!_read_peer_ready || stream_offset > _read_rx_offset)) {
        if (_read_early_offer || _read_rx_active || _read_done_pending ||
            target_epoch != _read_tx_epoch || sender_epoch == 0 ||
            (_read_peer_ready && sender_epoch != _read_peer_epoch) ||
            seq != _read_rx_seq || payload_length == 0 ||
            payload_length > _read_max_chunk || max_chunk != 0 ||
            stream_offset > UINT64_MAX - payload_length ||
            segment_offset > _remote_segment_len ||
            payload_length > _remote_segment_len - segment_offset) {
            errno = EPROTO;
            return -1;
        }
        std::memcpy(_read_early_frame, frame, URMA_READ_CONTROL_SIZE);
        _read_early_offer = true;
        return 0;
    }
    {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        if (!_read_peer_ready || target_epoch != _read_tx_epoch ||
            sender_epoch != _read_peer_epoch) {
            errno = EPROTO;
            return -1;
        }
    }
    if (frame[5] == URMA_READ_OFFER) {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        if (max_chunk != 0 || payload_length == 0 ||
            payload_length > _read_max_chunk ||
            stream_offset != _read_rx_offset || seq != _read_rx_seq ||
            _read_rx_active || _read_done_pending || _read_early_offer ||
            _remote_segment_len == 0 ||
            stream_offset > UINT64_MAX - payload_length ||
            segment_offset > _remote_segment_len ||
            payload_length > _remote_segment_len - segment_offset) {
            errno = EPROTO;
            return -1;
        }
        _read_rx_active = true;
        _read_pending_seq = seq;
        _read_pending_offset = stream_offset;
        _read_pending_length = payload_length;
        _read_pending_segment_offset = segment_offset;
        _read_post_pending = true;
        return 0;
    }
    if (frame[5] == URMA_READ_DONE) {
        std::unique_lock<butil::Mutex> lock(_read_mutex);
        if (max_chunk != 1 || segment_offset != 0 ||
            !_read_tx_offer_pending || seq != _read_tx_seq ||
            stream_offset != _read_tx_offset || payload_length != _read_tx_length ||
            stream_offset > UINT64_MAX - payload_length || _read_tx_seq == UINT64_MAX) {
            errno = EPROTO;
            return -1;
        }
        _read_tx_offer_pending = false;
        _read_tx_credit = true;
        _read_tx_offset += payload_length;
        _read_tx_seq++;
        _socket->WakeAsEpollOut();
        return 0;
    }
    errno = EPROTO;
    return -1;
}

// ============================================================================
// Recv path.
// ============================================================================

int UrmaEndpoint::DoPostRecv(void* block, size_t block_size) {
    urma_target_seg_t* tseg = GetPoolSegFor(block);
    if (!tseg) {
        errno = ERDMAMEM;
        return -1;
    }
    urma_sge_t sge{reinterpret_cast<uint64_t>(block),
                   static_cast<uint32_t>(block_size), tseg, nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfr_wr_t wr{sg, 0, nullptr};
    urma_jfr_wr_t* bad = nullptr;
    // Use the shared-JFR path on every device, including bonding. The bonding
    // provider owns physical receive scheduling for the JFR; a local
    // jetty-to-target association is not part of the RM receive API.
    const urma_status_t status =
        urma_post_jfr_wr(_resource->jfr, &wr, &bad);
    if (status != URMA_SUCCESS) {
        LOG(WARNING) << "Failed to post URMA receive WR: status=" << status
                     << " bonding=" << IsUrmaBondingDevice()
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr)
                     << " local_jetty_id=" << _resource->jetty->jetty_id.id
                     << " provider_associated_remote="
                     << static_cast<const void*>(
                            _resource->jetty->remote_jetty)
                     << " state=" << GetStateStr()
                     << " on " << _socket->description();
        errno = status;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::PostRecv(uint32_t num, bool zerocopy) {
    for (uint32_t i = 0; i < num; ++i) {
        size_t block_size = GetUrmaRecvBlockSize();
        if (zerocopy) {
            _rbuf[_rq_received].clear();
            butil::IOBufAsZeroCopyOutputStream zcis(
                &_rbuf[_rq_received], block_size + IOBUF_BLOCK_HEADER_LEN);
            void* data = nullptr;
            int size = 0;
            if (!zcis.Next(&data, &size) || !data ||
                size < static_cast<int>(block_size)) {
                errno = ENOMEM;
                return -1;
            }
            _rbuf_data[_rq_received] = data;
            if (DoPostRecv(data, block_size) < 0) {
                return -1;
            }
        } else {
            if (_rbuf_data[_rq_received] == nullptr) {
                _rbuf[_rq_received].clear();
                butil::IOBufAsZeroCopyOutputStream zcos(
                    &_rbuf[_rq_received],
                    block_size + IOBUF_BLOCK_HEADER_LEN);
                void* data = nullptr;
                int size = 0;
                if (!zcos.Next(&data, &size) || !data ||
                    size < static_cast<int>(block_size)) {
                    errno = ENOMEM;
                    return -1;
                }
                _rbuf_data[_rq_received] = data;
            }
            if (DoPostRecv(_rbuf_data[_rq_received], block_size) < 0) {
                return -1;
            }
        }
        _rq_received = (_rq_received + 1) % _rq_size;
    }
    return 0;
}

int UrmaEndpoint::SendImm(uint32_t imm) {
    if (imm == 0) {
        return 0;
    }
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    if (_sq_imm_window_size == 0) {
        errno = EAGAIN;
        return -1;
    }
    // Empty-payload SEND_IMM flushes peer-side receive credit. Connection
    // lifetime is owned by the TCP fd, so this is not an EOF marker.
    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.flag.bs.comp_order = 1;
    wr.flag.bs.place_order = 2;
    wr.flag.bs.solicited_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.send.imm_data = imm;
    wr.user_ctx = 0;  // 0 == pure ack (HandleCompletion reuses budget).
    urma_jfs_wr_t* bad = nullptr;
    // Reserve the ACK-only SQ slot before posting for the same reason as the
    // data windows in CutFromIOBufList: polling may observe its completion as
    // soon as the provider accepts the WR.
    --_sq_imm_window_size;
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        const int provider_errno = errno;
        if (bad == &wr) {
            ++_sq_imm_window_size;
            _new_rq_wrs.fetch_add(imm, butil::memory_order_relaxed);
        }
        _post_uncertain = true;  // Fail the connection; do not retry an ACK.
        LOG(WARNING) << "Failed to post URMA credit ACK: status=" << status
                     << " provider_errno=" << provider_errno
                     << " (" << berror(provider_errno) << ')'
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr)
                     << " imm=" << imm
                     << " local_jetty_id="
                     << _resource->jetty->jetty_id.id
                     << " remote_jetty_id="
                     << _resource->remote_jetty->id.id
                     << " state=" << GetStateStr()
                     << " on " << _socket->description();
        errno = status;
        return -1;
    }
    return 0;
}

int UrmaEndpoint::SendAck(int num) {
    const uint16_t old =
        _new_rq_wrs.fetch_add(num, butil::memory_order_relaxed);
    if (old + num > _remote_window_capacity / 2 &&
        _sq_imm_window_size > 0) {
        return SendImm(_new_rq_wrs.exchange(0, butil::memory_order_relaxed));
    }
    return 0;
}

ssize_t UrmaEndpoint::HandleCompletion(const urma_cr_t& cr) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || _post_uncertain) {
        errno = ENOTCONN;
        return -1;
    }
    const ssize_t bytes = HandleCompletionLocked(cr);
    if (bytes < 0 || ProgressRead() < 0) return -1;
    return bytes;
}

ssize_t UrmaEndpoint::HandleCompletionLocked(const urma_cr_t& cr) {
    bool zerocopy = FLAGS_urma_recv_zerocopy;
    if (cr.status != URMA_CR_SUCCESS) {
        LOG(WARNING) << "URMA completion failed, status=" << cr.status;
        errno = EIO;
        return -1;
    }
    if (cr.flag.bs.s_r == 0) {
        // Send completion: reclaim SQ window and wake the writer.
        if (cr.user_ctx == 0) {
            // Pure-ack WR: just replenish the imm budget.
            if (_sq_imm_window_size >= RESERVED_WR_NUM) {
                LOG(WARNING)
                    << "URMA credit-ACK completion exceeds reserved SQ "
                       "window: current="
                    << _sq_imm_window_size
                    << " capacity=" << RESERVED_WR_NUM
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            _sq_imm_window_size += 1;
            if (SendAck(0) < 0) return -1;
            return 0;
        }
        const uint8_t context_kind = WrContextKind(cr.user_ctx);
        const uint16_t context_slot = WrContextSlot(cr.user_ctx);
        if (cr.user_ctx < 0x100 || context_slot >= _sbuf.size() ||
            !_sq_slot_in_use[context_slot] ||
            (context_kind != 1 && context_kind != 2 && context_kind != 3)) {
            LOG(WARNING) << "Invalid URMA send completion context="
                         << cr.user_ctx;
            errno = EPROTO;
            return -1;
        }
        uint16_t wnd = 1;  // We signal every WR (complete_enable=1).
        uint16_t old =
            _sq_window_size.load(butil::memory_order_relaxed);
        while (true) {
            if (old >= _local_window_capacity) {
                LOG(WARNING)
                    << "URMA send completion exceeds SQ window: old=" << old
                    << " increment=" << wnd
                    << " capacity=" << _local_window_capacity
                    << " user_ctx=" << cr.user_ctx
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            if (_sq_window_size.compare_exchange_weak(
                    old, static_cast<uint16_t>(old + wnd),
                    butil::memory_order_relaxed)) {
                break;
            }
        }
        if (context_kind == 2) {
            const uint8_t type = _sq_control_type[context_slot];
            if (type < URMA_READ_READY || type > URMA_READ_DONE) {
                errno = EPROTO;
                return -1;
            }
            _read_control_busy[type - 1] = false;
            _sq_control_type[context_slot] = 0;
        }
        _sbuf[context_slot].clear();
        ReleaseSqSlot(context_slot);
        if (context_kind == 3) {
            // READ completion: the remote source has been consumed into our
            // registered RX slot. Copy into the byte-stream input buffer
            // before returning its one-slot credit.
            std::unique_lock<butil::Mutex> lock(_read_mutex);
            if (!_read_rx_active || _read_pending_length == 0) {
                errno = EPROTO;
                return -1;
            }
            const uint32_t read_len = _read_pending_length;
            if (_read_pending_offset != _read_rx_offset) {
                errno = EPROTO;
                return -1;
            }
            if (_read_rx_seq == UINT64_MAX) {
                errno = EPROTO;
                return -1;
            }
            butil::IOBuf received;
            if (received.append(_read_rx_data, read_len) != 0) {
                errno = ENOMEM;
                return -1;
            }
            _input_processor.read_buf().append(received);
            _read_rx_offset += read_len;
            ++_read_rx_seq;
            _read_rx_active = false;
            _read_done_pending = true;
            lock.unlock();
            butil::subtle::MemoryBarrier();
            if (_remote_rq_window_size.load(butil::memory_order_relaxed) >=
                _local_window_capacity / 8) {
                _socket->WakeAsEpollOut();
            }
            return read_len;
        }
        butil::subtle::MemoryBarrier();
        if (_remote_rq_window_size.load(butil::memory_order_relaxed) >=
            _local_window_capacity / 8) {
            _socket->WakeAsEpollOut();
        }
        return 0;
    }
    // Recv completion.
    const bool is_read_control =
        cr.opcode == URMA_CR_OPC_SEND_WITH_IMM &&
        (cr.imm_data & URMA_READ_IMM_MASK) == URMA_READ_IMM_TAG;
    uint8_t read_control_frame[URMA_READ_CONTROL_SIZE];
    if (is_read_control) {
        if (cr.completion_len != URMA_READ_CONTROL_SIZE) {
            errno = EPROTO;
            return -1;
        }
        std::memcpy(read_control_frame, _rbuf_data[_rq_received],
                    sizeof(read_control_frame));
        if ((cr.imm_data & 0xffffU) != read_control_frame[5]) {
            errno = EPROTO;
            return -1;
        }
        zerocopy = false;
    } else if (cr.opcode == URMA_CR_OPC_SEND_WITH_IMM && cr.imm_data > 0) {
        if (cr.completion_len != 0 || cr.imm_data > _local_window_capacity) {
            LOG(WARNING) << "Invalid URMA receive credit: " << cr.imm_data;
            errno = EPROTO;
            return -1;
        }
        const uint16_t acks = static_cast<uint16_t>(cr.imm_data);
        uint16_t old =
            _remote_rq_window_size.load(butil::memory_order_relaxed);
        while (true) {
            if (old > _local_window_capacity - acks) {
                LOG(WARNING)
                    << "URMA receive credit exceeds window: old=" << old
                    << " credit=" << acks
                    << " capacity=" << _local_window_capacity
                    << " imm=" << cr.imm_data
                    << " remote_window_capacity="
                    << _remote_window_capacity
                    << " on " << _socket->description();
                errno = EPROTO;
                return -1;
            }
            if (_remote_rq_window_size.compare_exchange_weak(
                    old, static_cast<uint16_t>(old + acks),
                    butil::memory_order_relaxed)) {
                break;
            }
        }
        if (_sq_window_size.load(butil::memory_order_relaxed) > 0) {
            _socket->WakeAsEpollOut();
        }
    } else if (cr.completion_len == 0) {
        LOG(WARNING) << "Zero-length URMA receive without immediate credit";
        errno = EPROTO;
        return -1;
    }
    if (!is_read_control && cr.completion_len > GetUrmaRecvBlockSize()) {
        LOG(WARNING) << "URMA completion exceeds receive buffer: "
                     << cr.completion_len;
        errno = EPROTO;
        return -1;
    }
    if (!is_read_control &&
        cr.completion_len < static_cast<uint32_t>(FLAGS_urma_zerocopy_min_size)) {
        zerocopy = false;
    }
    if (!is_read_control && cr.completion_len > 0 &&
        (_read_rx_active || _read_rx_offset > UINT64_MAX - cr.completion_len)) {
        errno = EPROTO;
        return -1;
    }
    butil::IOPortal& read_buf = _input_processor.read_buf();
    if (is_read_control) {
        // The control frame was copied into endpoint state synchronously;
        // do not expose it as RPC payload.
    } else if (zerocopy) {
        _rbuf[_rq_received].cutn(&read_buf, cr.completion_len);
    } else {
        butil::IOBuf received;
        if (received.append(_rbuf_data[_rq_received], cr.completion_len) != 0) {
            errno = ENOMEM;
            return -1;
        }
        read_buf.append(received);
    }
    if (PostRecv(1, zerocopy) < 0) {
        return -1;
    }
    if (cr.completion_len > 0) {
        if (SendAck(1) < 0) return -1;
        if (!is_read_control) {
            std::unique_lock<butil::Mutex> lock(_read_mutex);
            _read_rx_offset += cr.completion_len;
        } else {
            _read_ack_pending = true;
        }
    }
    if (is_read_control &&
        HandleReadControl(read_control_frame, sizeof(read_control_frame)) < 0) {
        return -1;
    }
    return is_read_control ? 0 : static_cast<ssize_t>(cr.completion_len);
}

void UrmaEndpoint::DispatchReceivedBytes(SocketUniquePtr& s, ssize_t bytes) {
    int64_t pending = _pending_received_bytes.load(butil::memory_order_relaxed);
    if (bytes > 0) {
        pending = _pending_received_bytes.fetch_add(
                      bytes, butil::memory_order_acq_rel) + bytes;
    }

    const State state = _state.load(butil::memory_order_acquire);
    if (state != ESTABLISHED) {
        return;
    }

    // PollCq and the handshake bthread can both reach this method when the
    // state changes to ESTABLISHED. Serialize them so each byte added to the
    // URMA input processor is reported to InputMessenger exactly once.
    std::unique_lock<butil::Mutex> dispatch_lock(_dispatch_mutex);
    if (_state.load(butil::memory_order_acquire) != ESTABLISHED) {
        return;
    }
    pending = _pending_received_bytes.exchange(
        0, butil::memory_order_acq_rel);
    if (pending <= 0 || s->Failed()) {
        return;
    }

    if (!s->user()) {
        LOG(ERROR) << "URMA socket has no InputMessenger: "
                   << s->description();
        return;
    }

    const int64_t received_us = butil::cpuwide_time_us();
    const int64_t base_realtime = butil::gettimeofday_us() - received_us;
    InputMessageClosure last_msg;
    _input_processor.ProcessNewMessage(static_cast<ssize_t>(pending), false,
                                       received_us, base_realtime, last_msg);
}

void UrmaEndpoint::PollCq(Socket* m) {
    auto* ep = static_cast<UrmaEndpoint*>(m->user());
    if (!ep) return;
    SocketUniquePtr s;
    if (Socket::Address(ep->_socket->id(), &s) != 0) {
        return;
    }
    if (s->Failed()) {
        return;
    }

    struct CqGuard {
        explicit CqGuard(UrmaEndpoint* endpoint) : ep(endpoint) {
            std::lock_guard<std::recursive_mutex> lock(ep->_io_mutex);
            ++ep->_cq_active;
        }
        ~CqGuard() {
            std::lock_guard<std::recursive_mutex> lock(ep->_io_mutex);
            --ep->_cq_active;
        }
        UrmaEndpoint* ep;
    } guard(ep);
    const bool event_mode = !FLAGS_urma_use_polling;
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        urma_jfc_t* event_jfc = nullptr;
        if (event_mode) {
            const int event_count = ep->WaitCqEvent(s, &event_jfc);
            if (event_count < 0) {
                return;
            }
            if (event_count == 0) {
                if (!m->MoreReadEvents(&progress)) {
                    return;
                }
                continue;
            }
        }

        ssize_t bytes = 0;
        auto drain_cq = [&]() -> int {
            while (true) {
                const int n =
                    std::max(1, std::min<int>(FLAGS_urma_cqe_poll_once, 32));
                urma_cr_t crs[32];
                int cnt;
                {
                    std::lock_guard<std::recursive_mutex> lock(ep->_io_mutex);
                    if (ep->_closing || !ep->_resource) return ECANCELED;
                    cnt = urma_poll_jfc(ep->_resource->jfc, n, crs);
                }
                if (cnt < 0) {
                    return EIO;
                }
                if (cnt == 0) {
                    return 0;
                }
                for (int i = 0; i < cnt; ++i) {
                    if (s->Failed()) {
                        return ECANCELED;
                    }
                    const ssize_t nr = ep->HandleCompletion(crs[i]);
                    if (nr < 0) {
                        return errno ? errno : EIO;
                    }
                    bytes += nr;
                }
                if (FLAGS_urma_cq_batch_dispatch && bytes > 0) {
                    // Publish received stream bytes before polling another CQ
                    // batch. Event mode still drains to empty below, but RPC
                    // parsing no longer waits for the entire drain to finish.
                    ep->DispatchReceivedBytes(s, bytes);
                    bytes = 0;
                    if (s->Failed()) {
                        return ECANCELED;
                    }
                }
            }
        };

        int completion_error = drain_cq();
        if (event_mode) {
            // The bonding provider records which physical JFCs produced CRs
            // while bondp_poll_jfc drains the virtual JFC.
            // bondp_rearm_jfc consumes that mask, so rearming before the drain
            // leaves those physical JFCs unarmed.
            uint32_t nevents = 1;
            {
                std::lock_guard<std::recursive_mutex> lock(ep->_io_mutex);
                if (ep->_closing || !ep->_resource) return;
                urma_ack_jfc(&event_jfc, &nevents, 1);
            }
            if (completion_error == 0) {
                if (ep->ReqNotifyCq() != 0) {
                    return;
                }

                // Close the drain/rearm race. A completion that arrived while
                // the JFC was unarmed may not produce an edge on every
                // provider. The JFC is armed now, so a final nonblocking drain
                // is safe.
                completion_error = drain_cq();
            }
        }

        if (completion_error != 0) {
            if (!s->Failed()) {
                s->SetFailed(completion_error, "URMA completion error");
            }
            return;
        }
        ep->DispatchReceivedBytes(s, bytes);

        if (!event_mode) {
            return;
        }
        // The bonding JFCE fd is itself an epoll fd aggregating physical
        // JFCEs, while brpc watches it with EPOLLET. urma_wait_jfc(..., 1, ...)
        // consumes only one aggregated event. Keep draining the inner JFCE
        // until it reports no event; otherwise another physical event can
        // leave the fd continuously readable and never create a new outer
        // edge. The event_count == 0 branch above resets _nevent only after
        // the inner queue is empty.
    }
}

// ============================================================================
// ApplyRemoteHello: size the send/recv windows from the peer's hello.
// ============================================================================

void UrmaEndpoint::ApplyRemoteHello(const ParsedHello& remote) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    _remote_recv_block_size = remote.buffer_size;
    _remote_segment_va = remote.seg_va;
    _remote_segment_len = remote.seg_len;
    const uint32_t peer_rq_size = remote.recv_buffer_cnt + 1;
    const uint32_t local_capacity =
        std::min<uint32_t>(_sq_size, peer_rq_size);
    _local_window_capacity = static_cast<uint16_t>(
        local_capacity > RESERVED_WR_NUM
            ? local_capacity - RESERVED_WR_NUM
            : 0);
    _remote_window_capacity =
        _rq_size > RESERVED_WR_NUM ? _rq_size - RESERVED_WR_NUM : 0;
    _sq_imm_window_size = RESERVED_WR_NUM;
    _remote_rq_window_size.store(_local_window_capacity,
                                 butil::memory_order_relaxed);
    _sq_window_size.store(_local_window_capacity, butil::memory_order_relaxed);
}

// ============================================================================
// OnNewDataFromTcp: edge-triggered dispatcher.
// ============================================================================

static void TryReadOnTcpDuringUrmaEst(Socket* socket);

void UrmaEndpoint::OnNewDataFromTcp(Socket* m) {
    auto* tp = static_cast<UrmaTransport*>(m->_transport.get());
    if (!tp) {
        return;
    }
    // Access _urma_ep directly (OnNewDataFromTcp is a friend of UrmaTransport);
    // GetUrmaEp() CHECKs non-null which would crash on TCP-fallback sockets.
    UrmaEndpoint* ep = tp->_urma_ep;
    if (!ep) {
        // No URMA endpoint: pure TCP path.
        InputMessenger::OnNewMessages(m);
        return;
    }
    int progress = 0;
    while (true) {
        const State state =
            ep->_state.load(butil::memory_order_acquire);
        if (state == UNINIT) {
            if (!m->CreatedByConnect()) {
                // Server side: kick off the handshake bthread.
                if (!IsUrmaAvailable()) {
                    ep->_state = FALLBACK_TCP;
                    tp->_urma_state = UrmaTransport::URMA_OFF;
                    InputMessenger::OnNewMessages(m);
                    return;
                }
                SocketUniquePtr s;
                m->ReAddress(&s);
                ep->_state = S_HELLO_WAIT;
                bthread_t tid;
                bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
                bthread_attr_set_name(&attr, "UrmaServerHandshake");
                if (bthread_start_background(&tid, &attr,
                        ProcessHandshakeAtServer, ep) != 0) {
                    ep->_state = UNINIT;
                    LOG(FATAL) << "Fail to start UrmaServerHandshake bthread";
                } else {
                    s.release();
                }
                return;
            }
            // Client side: handled by ProcessHandshakeAtClient.
            return;
        } else if (state < ESTABLISHED) {
            // During handshake: wake the handshake bthread parked in ReadFromFd.
            ep->_read_butex->fetch_add(1, butil::memory_order_release);
            bthread::butex_wake(ep->_read_butex);
            return;
        } else if (state == FALLBACK_TCP) {
            InputMessenger::OnNewMessages(m);
            return;
        } else if (state == ESTABLISHED) {
            TryReadOnTcpDuringUrmaEst(m);
            return;
        }
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    }
}

inline void UrmaEndpoint::TryReadOnTcp() {
    if (_state.load(butil::memory_order_acquire) == FALLBACK_TCP) {
        InputMessenger::OnNewMessages(_socket);
    }
}

static void TryReadOnTcpDuringUrmaEst(Socket* socket) {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        uint8_t byte = 0;
        const ssize_t nr = read(socket->fd(), &byte, 1);
        if (nr < 0) {
            if (errno != EAGAIN) {
                const int saved_errno = errno;
                socket->SetFailed(saved_errno, "Fail to read URMA TCP fd: %s",
                                  berror(saved_errno));
                return;
            }
            if (!socket->MoreReadEvents(&progress)) {
                return;
            }
        } else if (nr == 0) {
            socket->SetEOF();
            return;
        } else {
            socket->SetFailed(
                EPROTO, "Unexpected TCP data after URMA was established");
            return;
        }
    }
}

void UrmaEndpoint::FallbackToTcp(UrmaTransport* transport, bool process_tcp) {
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FALLBACK_TCP, butil::memory_order_release);
    DeallocateResources();
    if (process_tcp) {
        TryReadOnTcp();
    }
}

void UrmaEndpoint::FailHandshake(UrmaTransport* transport, int error,
                                 const char* reason) {
    LOG(ERROR) << "URMA handshake failed in state=" << GetStateStr()
               << " on " << _socket->description()
               << ": " << reason << ", error=" << error
               << " (" << berror(error) << ')';
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FAILED, butil::memory_order_release);
    DeallocateResources();
    auto* connect =
        static_cast<UrmaConnect*>(_socket->_app_connect.get());
    if (connect) {
        connect->_error = error;
    }
    _socket->SetFailed(error, "URMA handshake failed: %s", reason);
}

// ============================================================================
// Handshake state machines (client / server). Run in a background bthread.
// ============================================================================

void* UrmaEndpoint::ProcessHandshakeAtClient(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    if (!IsUrmaAvailable()) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    // Prepost the shared JFR before sending the client hello so the peer sees
    // a ready receive queue as soon as its import completes.
    if (ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_HELLO_SEND;
    std::unique_ptr<UrmaHandshake> hs(CreateClientHandshake(ep));
    ep->_handshake_version = hs->ProtocolVersion();
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client hello");
        return nullptr;
    }
    ep->_state = C_HELLO_WAIT;
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read server hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = C_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import server resources");
        return nullptr;
    }
    ep->_state = C_ACK_SEND;
    uint32_t flags = HELLO_ACK_URMA_OK;
    uint32_t flags_be = butil::HostToNet32(flags);
    if (ep->WriteToFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client ack");
        return nullptr;
    }
    tp->_urma_state = UrmaTransport::URMA_ON;
    ep->_state = ESTABLISHED;
    if (ep->StartReadProtocol() < 0) {
        const int saved_errno = errno ? errno : EIO;
        s->SetFailed(saved_errno, "Fail to send URMA READ_READY");
        return nullptr;
    }
    ep->DispatchReceivedBytes(s, 0);
    return nullptr;
}

void* UrmaEndpoint::ProcessHandshakeAtServer(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    ep->_state = S_HELLO_WAIT;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client magic");
        return nullptr;
    }
    std::unique_ptr<UrmaHandshake> hs(CreateServerHandshakeByMagic(ep, magic));
    if (!hs) {
        // Not an URMA peer: push the magic back and fall back to TCP.
        ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_handshake_version = hs->ProtocolVersion();
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FailHandshake(tp, EPROTO, "invalid client hello");
        return nullptr;
    }
    ep->_state = S_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "allocate server resources");
        return nullptr;
    }
    const bool bonding = IsUrmaBondingDevice();
    if (!bonding &&
        ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "post server receives");
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = S_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import client resources");
        return nullptr;
    }
    if (bonding &&
        ep->PostRecv(ep->_rq_size, FLAGS_urma_recv_zerocopy) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "post server receives");
        return nullptr;
    }
    ep->_state = S_HELLO_SEND;
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send server hello");
        return nullptr;
    }
    ep->_state = S_ACK_WAIT;
    uint32_t flags_be = 0;
    if (ep->ReadFromFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client ack");
        return nullptr;
    }
    uint32_t flags = butil::NetToHost32(flags_be);
    bool client_ack_ok = (flags & HELLO_ACK_URMA_OK) != 0;
    if (client_ack_ok) {
        if (tp->_urma_state.load(butil::memory_order_acquire) ==
            UrmaTransport::URMA_OFF) {
            // Protocol breakdown: client wants URMA but we already fell back.
            ep->FailHandshake(tp, EPROTO, "client ack mismatch");
            return nullptr;
        }
        tp->_urma_state = UrmaTransport::URMA_ON;
        ep->_state = ESTABLISHED;
        if (ep->StartReadProtocol() < 0) {
            const int saved_errno = errno ? errno : EIO;
            s->SetFailed(saved_errno, "Fail to send URMA READ_READY");
            return nullptr;
        }
        ep->DispatchReceivedBytes(s, 0);
    } else {
        ep->FallbackToTcp(tp, true);
    }
    return nullptr;
}

// ============================================================================
// UrmaConnect: drives the client handshake bthread.
// ============================================================================

void UrmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int, void*), void* data) {
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    _done = done;
    _data = data;
    _error = 0;
    auto* tp = static_cast<UrmaTransport*>(socket->_transport.get());
    if (!tp) {
        Run();
        return;
    }
    if (!tp->_urma_ep || !IsUrmaAvailable()) {
        // Fall back to TCP immediately.
        if (tp->_urma_ep) {
            tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        }
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
        return;
    }
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UrmaClientHandshake");
    if (bthread_start_background(&tid, &attr,
            UrmaEndpoint::ProcessHandshakeAtClient,
            tp->_urma_ep) != 0) {
        tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
    } else {
        // ProcessHandshakeAtClient adopts this reference in its
        // SocketUniquePtr constructor.
        s.release();
    }
}

void UrmaConnect::StopConnect(Socket*) {}

void UrmaConnect::Run() {
    if (_done) {
        auto cb = _done;
        _done = nullptr;
        cb(_error, _data);
    }
}

// ============================================================================
// Debug / polling-mode stubs.
// ============================================================================

std::string UrmaEndpoint::GetStateStr() const {
    switch (_state.load(butil::memory_order_acquire)) {
    case UNINIT:        return "UNINIT";
    case C_ALLOC_RES:   return "C_ALLOC_RES";
    case C_HELLO_SEND:  return "C_HELLO_SEND";
    case C_HELLO_WAIT:  return "C_HELLO_WAIT";
    case C_IMPORT_PEER: return "C_IMPORT_PEER";
    case C_ACK_SEND:    return "C_ACK_SEND";
    case S_HELLO_WAIT:  return "S_HELLO_WAIT";
    case S_ALLOC_RES:   return "S_ALLOC_RES";
    case S_IMPORT_PEER: return "S_IMPORT_PEER";
    case S_HELLO_SEND:  return "S_HELLO_SEND";
    case S_ACK_WAIT:    return "S_ACK_WAIT";
    case ESTABLISHED:   return "ESTABLISHED";
    case FALLBACK_TCP:  return "FALLBACK_TCP";
    case FAILED:        return "FAILED";
    }
    return "UNKNOWN";
}

void UrmaEndpoint::DebugInfo(std::ostream& os, butil::StringPiece) const {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    os << "state=" << GetStateStr()
       << " sq_size=" << _sq_size << " rq_size=" << _rq_size
       << " remote_recv_block_size=" << _remote_recv_block_size
       << " sq_window=" << _sq_window_size.load(butil::memory_order_relaxed)
       << " remote_rq_window=" << _remote_rq_window_size.load(butil::memory_order_relaxed)
       << " read_slot_capacity=" << _read_slot_capacity
       << " read_max_chunk=" << _read_max_chunk
       << " peer_read_max_chunk=" << _read_peer_max_chunk
       << " read_offer_pending=" << _read_tx_offer_pending
       << " read_post_pending=" << _read_post_pending
       << " read_done_pending=" << _read_done_pending
       << " handshake_version=" << _handshake_version
       << " read_buf=" << _input_processor.read_buf().size();
}

int UrmaEndpoint::WaitCqEvent(SocketUniquePtr& s,
                              urma_jfc_t** event_jfc) {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || !_resource || !_resource->jfce || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    *event_jfc = nullptr;
    int count = urma_wait_jfc(_resource->jfce, 1, 0, event_jfc);
    if (count < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return 0;
        }
        const int saved_errno = errno;
        PLOG(ERROR) << "Fail to wait URMA JFC event from "
                    << s->description();
        s->SetFailed(saved_errno, "Fail to wait URMA JFC event: %s",
                     berror(saved_errno));
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (*event_jfc != _resource->jfc) {
        LOG(ERROR) << "Unexpected URMA JFC event on " << s->description();
        errno = EPROTO;
        s->SetFailed(EPROTO, "Unexpected URMA JFC event");
        return -1;
    }
    return 1;
}

int UrmaEndpoint::ReqNotifyCq() {
    std::lock_guard<std::recursive_mutex> io_lock(_io_mutex);
    if (_closing || !_resource || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    const int rc = urma_rearm_jfc(_resource->jfc, false);
    if (rc != URMA_SUCCESS) {
        errno = rc;
        PLOG(WARNING) << "Fail to rearm URMA JFC";
        _socket->SetFailed(rc, "Fail to rearm URMA JFC: %s", berror(rc));
        return -1;
    }
    return 0;
}

int UrmaEndpoint::PollingModeInitialize(
        bthread_tag_t tag, std::function<void()> callback,
        std::function<void()> init_fn, std::function<void()> release_fn) {
    if (!FLAGS_urma_use_polling) {
        return 0;
    }
    if (tag >= _poller_groups.size() ||
        _poller_groups[tag].pollers.empty()) {
        errno = EINVAL;
        return -1;
    }
    auto& group = _poller_groups[tag];
    bool expected = false;
    if (!group.running.compare_exchange_strong(expected, true)) {
        return 0;
    }
    struct FnArgs {
        Poller* poller;
        butil::atomic<bool>* running;
    };
    auto fn = [](void* p) -> void* {
        std::unique_ptr<FnArgs> args(static_cast<FnArgs*>(p));
        Poller* poller = args->poller;
        butil::atomic<bool>* running = args->running;
        std::unordered_set<SocketId> cq_sids;
        CqSidOp op;

        if (poller->init_fn) {
            poller->init_fn();
        }
        while (running->load(butil::memory_order_relaxed)) {
            while (poller->op_queue.Dequeue(op)) {
                if (op.type == CqSidOp::ADD) {
                    cq_sids.emplace(op.sid);
                } else {
                    cq_sids.erase(op.sid);
                }
            }
            for (SocketId sid : cq_sids) {
                SocketUniquePtr s;
                if (Socket::Address(sid, &s) == 0) {
                    PollCq(s.get());
                }
            }
            if (poller->callback) {
                poller->callback();
            }
            if (FLAGS_urma_poller_yield || cq_sids.empty()) {
                bthread_yield();
            }
        }
        if (poller->release_fn) {
            poller->release_fn();
        }
        return nullptr;
    };

    auto& pollers = group.pollers;
    for (size_t i = 0; i < pollers.size(); ++i) {
        pollers[i].callback = callback;
        pollers[i].init_fn = init_fn;
        pollers[i].release_fn = release_fn;
        std::unique_ptr<FnArgs> args(new (std::nothrow)
                                        FnArgs{&pollers[i], &group.running});
        if (!args) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = ENOMEM;
            return -1;
        }
        bthread_attr_t attr = FLAGS_urma_disable_bthread
                                  ? BTHREAD_ATTR_PTHREAD
                                  : BTHREAD_ATTR_NORMAL;
        attr.tag = tag;
        bthread_attr_set_name(&attr, "UrmaPolling");
        const int rc = bthread_start_background(
            &pollers[i].tid, &attr, fn, args.get());
        if (rc != 0) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = rc;
            return -1;
        }
        args.release();
    }
    return 0;
}

void UrmaEndpoint::PollingModeRelease(bthread_tag_t tag) {
    if (!FLAGS_urma_use_polling || tag >= _poller_groups.size()) {
        return;
    }
    auto& group = _poller_groups[tag];
    group.running.store(false, butil::memory_order_relaxed);
    for (auto& poller : group.pollers) {
        if (poller.tid != INVALID_BTHREAD) {
            bthread_join(poller.tid, nullptr);
            poller.tid = INVALID_BTHREAD;
        }
    }
}

void UrmaEndpoint::PollerAddCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _poller_groups.empty()) {
        return;
    }
    _poller_tag = bthread_self_tag();
    if (_poller_tag >= _poller_groups.size()) {
        return;
    }
    auto& pollers = _poller_groups[_poller_tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index =
        butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(
        CqSidOp{CqSidOp::ADD, _cq_sid});
}

void UrmaEndpoint::PollerRemoveCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _poller_groups.empty() ||
        _poller_tag >= _poller_groups.size()) {
        return;
    }
    auto& pollers = _poller_groups[_poller_tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index =
        butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(
        CqSidOp{CqSidOp::REMOVE, _cq_sid});
}

int UrmaEndpoint::GlobalInitialize() {
    // Pre-allocate the prepared jetty pool. Skipped if URMA init is skipped
    // (unit-test mode).
    if (FLAGS_urma_use_polling && _poller_groups.empty()) {
        if (FLAGS_urma_poller_num <= 0) {
            LOG(ERROR) << "urma_poller_num must be positive";
            errno = EINVAL;
            return -1;
        }
        size_t ntags = static_cast<size_t>(FLAGS_task_group_ntags);
        if (ntags == 0) {
            ntags = 1;
        }
        _poller_groups = std::vector<PollerGroup>(ntags);
    }
    if (g_prepared_cnt > 0) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        return 0;
    }
    const int prepared_jetty_count = PreparedJettyCount();
    for (int i = 0; i < prepared_jetty_count; ++i) {
        auto* r = new (std::nothrow) UrmaResource();
        if (!r) {
            break;
        }
        r->jfce = urma_create_jfce(ctx);
        if (!r->jfce ||
            (!FLAGS_urma_use_polling && r->jfce->fd < 0)) {
            delete r;
            break;
        }
        urma_jfc_cfg_t jfc_cfg{};
        jfc_cfg.depth = static_cast<uint32_t>(FLAGS_urma_sq_size + FLAGS_urma_rq_size);
        jfc_cfg.jfce = r->jfce;
        r->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!r->jfc) {
            delete r;
            break;
        }
        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(FLAGS_urma_rq_size);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge = 1;
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = r->jfc;
        r->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!r->jfr) {
            delete r;
            break;
        }
        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(FLAGS_urma_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge =
            static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = r->jfc;
        jetty_cfg.shared.jfr = r->jfr;
        jetty_cfg.shared.jfc = r->jfc;
        r->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!r->jetty) {
            delete r;
            break;
        }
        r->next = g_prepared_list;
        g_prepared_list = r;
        ++g_prepared_cnt;
    }
    return 0;
}

void UrmaEndpoint::GlobalRelease() {
    {
        BAIDU_SCOPED_LOCK(g_prepared_mutex);
        while (g_prepared_list) {
            UrmaResource* next = g_prepared_list->next;
            delete g_prepared_list;
            g_prepared_list = next;
        }
        g_prepared_cnt = 0;
    }
    for (size_t tag = 0; tag < _poller_groups.size(); ++tag) {
        PollingModeRelease(static_cast<bthread_tag_t>(tag));
    }
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
