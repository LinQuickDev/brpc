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
#include <utility>
#include <vector>
#include "butil/endpoint.h"
#include "butil/fd_guard.h"
#include "butil/time.h"
#include "brpc/acceptor.h"
#include "brpc/channel.h"
#include "brpc/input_messenger.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/rdma_transport.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_handshake.h"
#include "brpc/rdma/rdma_helper.h"
#include "brpc/details/load_balancer_with_naming.h"
#include "butil/files/temp_file.h"

using namespace brpc;

namespace brpc {
namespace rdma {
extern butil::atomic<bool> g_rdma_available;
extern bool g_skip_rdma_init;
extern int (*IbvModifyQp)(ibv_qp*, ibv_qp_attr*, ibv_qp_attr_mask);
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

class RdmaQosTest : public ::testing::Test {
protected:
    RdmaQosTest() {
        EXPECT_EQ(0, butil::str2endpoint("127.0.0.1:8123", &g_ep));
        EXPECT_EQ(0, _server_list.save(butil::endpoint2str(g_ep).c_str()));
        _naming_url = std::string("File://") + _server_list.fname();
    }

    void TearDown() override { StopServer(); }

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

    butil::TempFile _server_list;
    std::string _naming_url;
    Server _server;
};

static RdmaTransport* RdmaTransportOf(Socket* s) {
    return static_cast<RdmaTransport*>(s->_transport.get());
}
static RdmaTransport* RdmaTransportOf(const SocketUniquePtr& s) {
    return RdmaTransportOf(s.get());
}

TEST_F(RdmaQosTest, qos_rejects_invalid_options_before_connecting) {
    for (const auto& values : {std::pair<int, int>(-1, 0), {16, 0}, {0, -1}, {0, 256}}) {
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        options.rdma_qos.service_level = values.first;
        options.rdma_qos.traffic_class = values.second;
        Channel channel;
        EXPECT_EQ(-1, channel.Init(g_ep, &options));
        ServerOptions server_options;
        server_options.socket_mode = SOCKET_MODE_RDMA;
        server_options.rdma_qos = options.rdma_qos;
        Server server;
        EXPECT_EQ(-1, server.Start("127.0.0.1:0", &server_options));
        SocketOptions socket_options;
        socket_options.socket_mode = SOCKET_MODE_RDMA;
        socket_options.rdma_qos = options.rdma_qos;
        SocketId sid;
        EXPECT_EQ(-1, Socket::Create(socket_options, &sid));
        EXPECT_EQ(EINVAL, errno);
    }
}

TEST_F(RdmaQosTest, qos_isolates_channels_and_preserves_equal_config_sharing) {
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_RDMA;
    Channel a, b, same, different_tc, default_rdma, tcp, tcp_with_qos;
    ASSERT_EQ(0, default_rdma.Init(g_ep, &options));
    ASSERT_EQ(0, tcp.Init(g_ep, nullptr));
    EXPECT_NE(default_rdma._server_id, tcp._server_id);
    options.rdma_qos.service_level = 3;
    options.rdma_qos.traffic_class = 106;
    ASSERT_EQ(0, a.Init(g_ep, &options));
    ASSERT_EQ(0, same.Init(g_ep, &options));
    EXPECT_EQ(a._server_id, same._server_id);
    EXPECT_NE(a._server_id, default_rdma._server_id);
    options.rdma_qos.service_level = 4;
    ASSERT_EQ(0, b.Init(g_ep, &options));
    EXPECT_NE(a._server_id, b._server_id);
    options.rdma_qos.service_level = 3;
    options.rdma_qos.traffic_class = 104;
    ASSERT_EQ(0, different_tc.Init(g_ep, &options));
    EXPECT_NE(a._server_id, different_tc._server_id);
    options.socket_mode = SOCKET_MODE_TCP;
    ASSERT_EQ(0, tcp_with_qos.Init(g_ep, &options));
    EXPECT_EQ(tcp._server_id, tcp_with_qos._server_id);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(a._server_id, &socket));
    EXPECT_EQ(3, socket->rdma_qos().service_level);
    EXPECT_EQ(106, socket->rdma_qos().traffic_class);
}

TEST_F(RdmaQosTest, qos_follows_pooled_short_and_named_connections) {
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_RDMA;
    options.rdma_qos.service_level = 7;
    options.rdma_qos.traffic_class = 255;
    Channel direct, named;
    ASSERT_EQ(0, direct.Init(g_ep, &options));
    ASSERT_EQ(0, named.Init(_naming_url.c_str(), "rr", &options));
    SocketUniquePtr main_socket;
    ASSERT_EQ(0, Socket::Address(direct._server_id, &main_socket));
    SocketUniquePtr pooled, short_socket, selected;
    ASSERT_EQ(0, main_socket->GetPooledSocket(&pooled));
    ASSERT_EQ(0, main_socket->GetShortSocket(&short_socket));
    LoadBalancer::SelectIn input{};
    LoadBalancer::SelectOut output(&selected);
    ASSERT_EQ(0, named._lb->SelectServer(input, &output));
    for (Socket* s : {pooled.get(), short_socket.get(), selected.get()}) {
        EXPECT_EQ(7, s->rdma_qos().service_level);
        EXPECT_EQ(255, s->rdma_qos().traffic_class);
    }
    EXPECT_EQ(main_socket->id(), selected->id());
    EXPECT_EQ(0, pooled->ReturnToPool());
    short_socket->SetFailed();
}

TEST_F(RdmaQosTest, qos_server_options_follow_accept_and_restart) {
    for (int sl : {3, 7}) {
        ServerOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        options.rdma_qos.service_level = sl;
        options.rdma_qos.traffic_class = 106;
        ASSERT_EQ(0, _server.Start("127.0.0.1:0", &options));
        g_ep = _server.listen_address();
        butil::fd_guard fd;
        ASSERT_NO_FATAL_FAILURE(ConnectToServer(&fd));
        Socket* socket = WaitForServerSocket();
        ASSERT_TRUE(socket != nullptr);
        EXPECT_EQ(sl, socket->rdma_qos().service_level);
        EXPECT_EQ(106, socket->rdma_qos().traffic_class);
        fd.reset(-1);
        ASSERT_TRUE(WaitForServerSocketGone());
        StopServer();
    }
}

static ibv_ah_attr g_qos_rtr_address;
static int g_qos_rtr_calls;
static int CaptureQosModify(ibv_qp*, ibv_qp_attr* attr, ibv_qp_attr_mask mask) {
    if (mask & IBV_QP_AV) {
        g_qos_rtr_address = attr->ah_attr;
        ++g_qos_rtr_calls;
    }
    return 0;
}

TEST_F(RdmaQosTest, qos_applies_to_rtr_on_each_qp_bringup) {
    for (const auto& values : {std::pair<int, int>(15, 255), {0, 0}}) {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        options.rdma_qos.service_level = values.first;
        options.rdma_qos.traffic_class = values.second;
        SocketId sid;
        ASSERT_EQ(0, Socket::Create(options, &sid));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(sid, &socket));
        auto* ep = RdmaTransportOf(socket)->_rdma_ep;
        ibv_qp qp{};
        rdma::RdmaResource resource;
        resource.qp = &qp;
        ep->_resource = &resource;
        ep->_rq_size = 0;
        rdma::ParsedHello remote{};
        const bool skip = rdma::g_skip_rdma_init;
        const auto modify = rdma::IbvModifyQp;
        rdma::g_skip_rdma_init = false;
        rdma::IbvModifyQp = CaptureQosModify;
        g_qos_rtr_calls = 0;
        // Reapply attributes on both client and server bringup.
        EXPECT_EQ(0, ep->BringUpQp(remote, false));
        EXPECT_EQ(0, ep->BringUpQp(remote, true));
        rdma::g_skip_rdma_init = skip;
        rdma::IbvModifyQp = modify;
        EXPECT_EQ(2, g_qos_rtr_calls);
        EXPECT_EQ(values.first, g_qos_rtr_address.sl);
        EXPECT_EQ(values.second, g_qos_rtr_address.grh.traffic_class);
        ep->_resource = nullptr;
        resource.qp = nullptr;
        socket->SetFailed();
    }
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
