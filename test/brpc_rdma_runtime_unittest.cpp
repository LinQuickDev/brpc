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


#include <netinet/in.h>
#include <sys/socket.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>
#if BRPC_WITH_RDMA
#include <errno.h>
#include <unistd.h>
#include <functional>
#include <vector>
#include <google/protobuf/descriptor.h>
#include "butil/endpoint.h"
#include "butil/fd_guard.h"
#include "butil/iobuf.h"
#include "butil/sys_byteorder.h"
#include "butil/time.h"
#include "brpc/acceptor.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/input_messenger.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/errno.pb.h"
#include "brpc/rdma_transport.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_handshake.h"
#include "brpc/rdma/rdma_handshake_constants.h"
#include "brpc/rdma/rdma_handshake.pb.h"
#include "brpc/rdma/rdma_helper.h"
#include "brpc/rdma/rdma_recovery_policy.h"
#include "echo.pb.h"

using namespace brpc;

namespace brpc {
namespace rdma {
extern butil::atomic<bool> g_rdma_available;
extern bool g_skip_rdma_init;
}  // namespace rdma
}  // namespace brpc

static butil::EndPoint g_ep;
static const int64_t WAIT_TIMEOUT_US = 5000000;
static const int64_t WAIT_INTERVAL_US = 1000;

static bool WaitUntil(const std::function<bool()>& pred,
                      int64_t timeout_us = WAIT_TIMEOUT_US) {
    const int64_t deadline = butil::gettimeofday_us() + timeout_us;
    while (!pred()) {
        if (butil::gettimeofday_us() >= deadline) {
            return false;
        }
        usleep((useconds_t)WAIT_INTERVAL_US);
    }
    return true;
}

// write(2) may transfer less than asked for, so a test that cares about the
// whole buffer reaching the peer has to loop.
static bool WriteAll(int fd, const void* buf, size_t len) {
    const uint8_t* p = (const uint8_t*)buf;
    for (size_t done = 0; done < len; ) {
        ssize_t n = write(fd, p + done, len - done);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        done += n;
    }
    return true;
}

static void ConnectToServer(butil::fd_guard* sockfd) {
    sockaddr_in addr;
    bzero((char*)&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_ep.port);
    addr.sin_addr = g_ep.ip;
    sockfd->reset(socket(AF_INET, SOCK_STREAM, 0));
    ASSERT_TRUE(*sockfd >= 0);
    timeval tv;
    tv.tv_sec = WAIT_TIMEOUT_US / 1000000;
    tv.tv_usec = WAIT_TIMEOUT_US % 1000000;
    ASSERT_EQ(0, setsockopt(*sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)));
    ASSERT_EQ(0, connect(*sockfd, (sockaddr*)&addr, sizeof(addr)));
}

class RecoveryEchoService : public ::test::EchoService {
    void Echo(google::protobuf::RpcController*, const ::test::EchoRequest*,
              ::test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        ClosureGuard done_guard(done);
        response->set_message("MyEchoService");
    }
};

class RdmaRuntimeFixture : public ::testing::Test {
protected:
    RdmaRuntimeFixture() {
        EXPECT_EQ(0, butil::str2endpoint("127.0.0.1:8123", &g_ep));
        EXPECT_EQ(0, _server.AddService(&_service, SERVER_DOESNT_OWN_SERVICE));
    }

    void StartServer(bool use_rdma = true) {
        ServerOptions options;
        options.enabled_protocols = "baidu_std";
        options.socket_mode = use_rdma ? SOCKET_MODE_RDMA : SOCKET_MODE_TCP;
        options.idle_timeout_sec = 5;
        ASSERT_EQ(0, _server.Start("127.0.0.1:0", &options));
        g_ep = _server.listen_address();
    }

    void StopServer() {
        _server.Stop(0);
        _server.Join();
    }

    Socket* GetSocketFromServer(size_t index) {
        std::vector<SocketId> sids;
        _server._am->ListConnections(&sids);
        if (index >= sids.size()) {
            return nullptr;
        }
        SocketUniquePtr socket;
        return Socket::Address(sids[index], &socket) == 0 ? socket.get() : nullptr;
    }

    Socket* WaitForServerSocket() {
        Socket* socket = nullptr;
        WaitUntil([this, &socket] {
            return (socket = GetSocketFromServer(0)) != nullptr;
        });
        return socket;
    }

    bool WaitForServerSocketGone() {
        return WaitUntil([this] { return GetSocketFromServer(0) == nullptr; });
    }

    Server _server;
    RecoveryEchoService _service;
};

static RdmaTransport* RdmaTransportOf(Socket* s) {
    return static_cast<RdmaTransport*>(s->_transport.get());
}
static RdmaTransport* RdmaTransportOf(const SocketUniquePtr& s) {
    return RdmaTransportOf(s.get());
}

// Polls until the endpoint reaches `expected` and returns the last state seen,
// so that ASSERT_RDMA_STATE() reports what the endpoint actually settled on.
static rdma::RdmaEndpoint::State WaitForRdmaState(
        RdmaTransport* transport, rdma::RdmaEndpoint::State expected) {
    rdma::RdmaEndpoint::State state = transport->_rdma_ep->_state;
    WaitUntil([transport, expected, &state] {
        state = transport->_rdma_ep->_state;
        return state == expected;
    });
    return state;
}

// Waits for `transport` to reach `expected`, failing the test if it does not.
#define ASSERT_RDMA_STATE(expected, transport) \
    ASSERT_EQ(expected, WaitForRdmaState(transport, expected))

static void MakeV2ClientHello(uint8_t (&data)[rdma::HELLO_V2_MSG_LEN_MIN]) {
    rdma::v2_wire::HelloMessage msg{};
    msg.msg_len = rdma::HELLO_V2_MSG_LEN_MIN;
    msg.hello_ver = rdma::HELLO_V2_VERSION;
    msg.impl_ver = rdma::IMPL_V2_VERSION;
    msg.sq_size = 16;
    msg.rq_size = 16;
    msg.block_size = 8192;
    msg.qp_num = 0;
    msg.gid = rdma::GetRdmaGid();
    memcpy(data, "RDMA", 4);
    msg.Serialize(data + 4);
}

std::string MakeV3Packet(const rdma::RdmaHello& msg) {
    std::string body;
    EXPECT_TRUE(msg.SerializeToString(&body));
    std::string packet;
    packet.reserve(4 + 4 + body.size());
    packet.append("RDM3", 4);
    uint32_t pb_size_be =
        butil::HostToNet32(static_cast<uint32_t>(body.size()));
    packet.append(reinterpret_cast<const char*>(&pb_size_be), 4);
    packet.append(body);
    return packet;
}

// Build a fully-valid RdmaHello: all 6 required fields are set, with
// values that pass RdmaHelloV3Wire::RdmaHelloValid().
//   - block_size = 8192 (>= MIN_BLOCK_SIZE)
//   - sq_size / rq_size = 16 (>= MIN_QP_SIZE)
//   - gid = exactly 16B (sizeof(ibv_gid))
//   - qp_num = 0  (allowed because g_skip_rdma_init in UT)
rdma::RdmaHello MakeValidV3Hello() {
    rdma::RdmaHello msg;
    msg.set_block_size(8192);
    msg.set_sq_size(16);
    msg.set_rq_size(16);
    msg.set_lid(0);
    ibv_gid gid = rdma::GetRdmaGid();
    msg.set_gid(std::string(reinterpret_cast<const char*>(gid.raw),
                            sizeof(gid.raw)));
    msg.set_qp_num(0);
    return msg;
}


class RdmaAdmissionTest : public RdmaRuntimeFixture {
protected:
    void SetUp() override {
        _saved_mode = rdma::GetRdmaRecoveryMode();
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::AUTO);
    }

    void TearDown() override {
        StopServer();
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(_saved_mode);
    }

    void CallEchoOverFallback() {
        Channel channel;
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        options.connect_timeout_ms = 1000;
        options.timeout_ms = 1000;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init(g_ep, &options));
        Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message("runtime recovery");
        test::EchoService::Stub(&channel).Echo(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        EXPECT_EQ("MyEchoService", res.message());
        SocketUniquePtr s;
        ASSERT_EQ(0, Socket::Address(cntl._single_server_id, &s));
        EXPECT_EQ(RdmaTransport::RDMA_OFF, RdmaTransportOf(s)->_rdma_state);
        EXPECT_EQ(rdma::RdmaEndpoint::FALLBACK_TCP,
                  RdmaTransportOf(s)->_rdma_ep->_state);
        EXPECT_FALSE(s->Failed());
    }

private:
    rdma::RdmaRecoveryMode _saved_mode;
};

TEST_F(RdmaAdmissionTest, tcp_only_carries_rpc_to_tcp_server) {
    StartServer(false);
    rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::TCP_ONLY);
    ASSERT_NO_FATAL_FAILURE(CallEchoOverFallback());
}

TEST_F(RdmaAdmissionTest, cooled_peer_carries_rpc_over_tcp) {
    StartServer();
    auto& policy = rdma::GlobalRdmaRecoveryPolicy();
    const std::string peer = butil::endpoint2str(g_ep).c_str();
    uint64_t epoch;
    ASSERT_TRUE(policy.AllowNewConnection(peer, &epoch));
    policy.ReportPeerFailure(peer, epoch);
    ASSERT_FALSE(policy.AllowNewConnection(peer));
    ASSERT_NO_FATAL_FAILURE(CallEchoOverFallback());
}

static int PollRnrCompletion(ibv_cq* cq, int, ibv_wc* wc) {
    bool* delivered = static_cast<bool*>(cq->cq_context);
    if (*delivered) {
        return 0;
    }
    *delivered = true;
    memset(wc, 0, sizeof(*wc));
    wc->status = IBV_WC_RNR_RETRY_EXC_ERR;
    return 1;
}

TEST_F(RdmaAdmissionTest, rnr_completion_cools_peer_before_tcp_rpc) {
    StartServer();
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_RDMA;
    options.remote_side = g_ep;
    options.user = get_client_side_messenger();
    SocketId sid;
    ASSERT_EQ(0, Socket::Create(options, &sid));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(sid, &socket));
    auto* ep = RdmaTransportOf(socket)->_rdma_ep;
    SocketOptions cq_options;
    cq_options.user = ep;
    SocketId cq_sid;
    ASSERT_EQ(0, Socket::Create(cq_options, &cq_sid));
    SocketUniquePtr cq_socket;
    ASSERT_EQ(0, Socket::Address(cq_sid, &cq_socket));

    auto& policy = rdma::GlobalRdmaRecoveryPolicy();
    const std::string peer = butil::endpoint2str(g_ep).c_str();
    uint64_t epoch;
    ASSERT_TRUE(policy.AllowNewConnection(peer, &epoch));
    bool delivered = false;
    ibv_context context{};
    context.ops.poll_cq = PollRnrCompletion;
    ibv_cq cq{};
    cq.context = &context;
    cq.cq_context = &delivered;
    rdma::RdmaResource resource;
    resource.polling_cq = &cq;
    ep->_resource = &resource;
    ep->_cq_sid = cq_sid;
    ep->_recovery_epoch = epoch;
    ep->_state.store(rdma::RdmaEndpoint::ESTABLISHED);
    const bool saved_polling = rdma::FLAGS_rdma_use_polling;
    rdma::FLAGS_rdma_use_polling = true;

    rdma::RdmaEndpoint::PollCq(cq_socket.get());
    const auto first_report = rdma::GetRdmaRecoveryStatus();
    ep->ReportFailure(rdma::RdmaFailureReason::QP_FATAL);
    EXPECT_EQ(first_report.failures[static_cast<size_t>(rdma::RdmaFailureReason::QP_FATAL)],
              rdma::GetRdmaRecoveryStatus().failures[
                  static_cast<size_t>(rdma::RdmaFailureReason::QP_FATAL)]);
    EXPECT_TRUE(delivered);
    EXPECT_TRUE(socket->Failed());
    EXPECT_FALSE(policy.AllowNewConnection(peer));
    EXPECT_TRUE(policy.AllowNewConnection("127.0.0.1:9999"));

    // The test owns the mock CQ and resource; ordinary recycling owns neither.
    ep->_resource = nullptr;
    ep->_cq_sid = INVALID_SOCKET_ID;
    resource.polling_cq = nullptr;
    cq_socket->_user = nullptr;
    cq_socket->SetFailed();
    rdma::FLAGS_rdma_use_polling = saved_polling;
    ASSERT_NO_FATAL_FAILURE(CallEchoOverFallback());
}

TEST_F(RdmaAdmissionTest, server_declines_v2_and_v3_when_tcp_only) {
    StartServer();
    rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::TCP_ONLY);
    for (int version : {2, 3}) {
        butil::fd_guard fd;
        ASSERT_NO_FATAL_FAILURE(ConnectToServer(&fd));
        if (version == 2) {
            uint8_t hello[rdma::HELLO_V2_MSG_LEN_MIN];
            MakeV2ClientHello(hello);
            ASSERT_TRUE(WriteAll(fd, hello, sizeof(hello)));
        } else {
            const std::string hello = MakeV3Packet(MakeValidV3Hello());
            ASSERT_TRUE(WriteAll(fd, hello.data(), hello.size()));
        }
        Socket* s = WaitForServerSocket();
        ASSERT_TRUE(s != nullptr);
        auto* transport = RdmaTransportOf(s);
        ASSERT_RDMA_STATE(rdma::RdmaEndpoint::S_ACK_WAIT, transport);
        EXPECT_EQ(RdmaTransport::RDMA_OFF, transport->_rdma_state);
        EXPECT_EQ(nullptr, transport->_rdma_ep->_resource);
        const uint32_t ack = 0;
        ASSERT_TRUE(WriteAll(fd, &ack, sizeof(ack)));
        ASSERT_RDMA_STATE(rdma::RdmaEndpoint::FALLBACK_TCP, transport);
        EXPECT_FALSE(s->Failed());
        fd.reset(-1);
        ASSERT_TRUE(WaitForServerSocketGone());
    }
}

TEST(RdmaCompletionRecoveryTest, classifies_path_failures) {
    EXPECT_TRUE(rdma::IsRdmaPathFailure(IBV_WC_RETRY_EXC_ERR));
    EXPECT_TRUE(rdma::IsRdmaPathFailure(IBV_WC_RNR_RETRY_EXC_ERR));
    EXPECT_FALSE(rdma::IsRdmaPathFailure(IBV_WC_SUCCESS));
    EXPECT_FALSE(rdma::IsRdmaPathFailure(IBV_WC_WR_FLUSH_ERR));
    EXPECT_FALSE(rdma::IsRdmaPathFailure(IBV_WC_LOC_PROT_ERR));
    EXPECT_FALSE(rdma::IsRdmaPathFailure(IBV_WC_REM_ACCESS_ERR));
}

TEST_F(RdmaAdmissionTest, async_errors_validate_owner_and_report_once) {
    for (ibv_event_type type : {IBV_EVENT_QP_FATAL, IBV_EVENT_CQ_ERR,
                               IBV_EVENT_QP_ACCESS_ERR}) {
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::AUTO);
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        options.remote_side = g_ep;
        options.user = get_client_side_messenger();
        SocketId sid;
        ASSERT_EQ(0, Socket::Create(options, &sid));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(sid, &socket));
        auto* ep = RdmaTransportOf(socket)->_rdma_ep;
        auto& policy = rdma::GlobalRdmaRecoveryPolicy();
        const std::string peer = butil::endpoint2str(g_ep).c_str();
        ASSERT_TRUE(policy.AllowNewConnection(peer, &ep->_recovery_epoch));
        ibv_qp qp{};
        ibv_qp other_qp{};
        ibv_cq cq{};
        ibv_cq other_cq{};
        rdma::RdmaResource resource;
        resource.qp = &qp;
        resource.polling_cq = &cq;
        ep->_resource = &resource;
        ep->BindResourceToSocket();
        EXPECT_EQ(sid, reinterpret_cast<SocketId>(qp.qp_context));
        EXPECT_EQ(sid, reinterpret_cast<SocketId>(cq.cq_context));
        ep->_state.store(rdma::RdmaEndpoint::ESTABLISHED);
        const auto before = policy.GetStatus();
        ibv_async_event event{};
        event.event_type = type;
        if (type == IBV_EVENT_CQ_ERR) {
            event.element.cq = &other_cq;
        } else {
            event.element.qp = &other_qp;
        }
        rdma::RdmaEndpoint::OnAsyncError(socket.get(), event);
        EXPECT_FALSE(socket->Failed());
        if (type == IBV_EVENT_CQ_ERR) {
            event.element.cq = &cq;
        } else {
            event.element.qp = &qp;
        }
        rdma::RdmaEndpoint::OnAsyncError(socket.get(), event);
        EXPECT_TRUE(socket->Failed());
        const auto reason = type == IBV_EVENT_CQ_ERR ? rdma::RdmaFailureReason::CQ_ERROR :
            type == IBV_EVENT_QP_FATAL ? rdma::RdmaFailureReason::QP_FATAL :
                                      rdma::RdmaFailureReason::ACCESS_ERROR;
        EXPECT_EQ(before.failures[static_cast<size_t>(reason)] + 1,
                  policy.GetStatus().failures[static_cast<size_t>(reason)]);
        EXPECT_EQ(type == IBV_EVENT_QP_ACCESS_ERR, policy.AllowNewConnection(peer));
        ep->ReportFailure(rdma::RdmaFailureReason::RNR);
        EXPECT_EQ(before.failures[static_cast<size_t>(rdma::RdmaFailureReason::RNR)],
                  policy.GetStatus().failures[static_cast<size_t>(rdma::RdmaFailureReason::RNR)]);
        const std::string exposed = bvar::Variable::describe_exposed("rdma_recovery_status");
        EXPECT_NE(std::string::npos, exposed.find("failures"));
        EXPECT_NE(std::string::npos, exposed.find("qp_fatal"));
        ep->_resource = nullptr;
        resource.qp = nullptr;
        resource.polling_cq = nullptr;
    }
}

static int FailAckPost(ibv_qp*, ibv_send_wr*, ibv_send_wr**) {
    return EIO;
}

TEST_F(RdmaAdmissionTest, ack_post_error_propagates_and_cools_peer) {
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_RDMA;
    options.remote_side = g_ep;
    options.user = get_client_side_messenger();
    SocketId sid;
    ASSERT_EQ(0, Socket::Create(options, &sid));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(sid, &socket));
    auto* ep = RdmaTransportOf(socket)->_rdma_ep;
    auto& policy = rdma::GlobalRdmaRecoveryPolicy();
    const std::string peer = butil::endpoint2str(g_ep).c_str();
    ASSERT_TRUE(policy.AllowNewConnection(peer, &ep->_recovery_epoch));
    ibv_context context{};
    context.ops.post_send = FailAckPost;
    ibv_qp qp{};
    qp.context = &context;
    rdma::RdmaResource resource;
    resource.qp = &qp;
    ep->_resource = &resource;
    ep->_state.store(rdma::RdmaEndpoint::ESTABLISHED);
    ep->_remote_window_capacity = 0;
    ep->_new_rq_wrs.store(1);
    ibv_wc wc{};
    wc.opcode = IBV_WC_SEND;
    EXPECT_EQ(-1, ep->HandleCompletion(wc));
    EXPECT_EQ(EIO, errno);
    EXPECT_FALSE(policy.AllowNewConnection(peer));
    EXPECT_EQ(rdma::RdmaFailureReason::SEND_POST, ep->_runtime_failure.load());
    ep->_resource = nullptr;
    resource.qp = nullptr;
    socket->SetFailed();
}

#endif  // BRPC_WITH_RDMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_RDMA
    rdma::g_rdma_available.store(true, butil::memory_order_relaxed);
    rdma::g_skip_rdma_init = true;
#endif
    return RUN_ALL_TESTS();
}
