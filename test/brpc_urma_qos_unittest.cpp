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

#include <cstring>
#include <sstream>
#include <vector>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "brpc/acceptor.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/input_messenger.h"
#include "brpc/load_balancer.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"
#include "echo.pb.h"

using namespace brpc;

namespace brpc {
namespace urma {
DECLARE_int32(urma_client_handshake_version);
extern bool g_skip_urma_init;
DECLARE_int32(urma_buffer_count);
DECLARE_int32(urma_prepared_jetty_cnt);

class UrmaQosTestPeer {
public:
    static int Allocate(UrmaEndpoint* ep) { return ep->AllocateResources(); }
    static int Priority(UrmaEndpoint* ep) {
        return ep->_resource->jetty->jetty_cfg.jfs_cfg.priority;
    }
};

}  // namespace urma
}  // namespace brpc

TEST(UrmaHelperTest, explicit_priority_requires_ctp_capability) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[0].tp_type.bs.ctp = 1;
    attr.dev_cap.priority_info[15].tp_type.bs.ctp = 1;
    attr.dev_cap.priority_info[15].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[3].tp_type.bs.rtp = 1;
    EXPECT_TRUE(urma::IsUrmaCtpPrioritySupported(attr, 0));
    EXPECT_TRUE(urma::IsUrmaCtpPrioritySupported(attr, 15));
    for (int priority : {-2, -1, 3, 4, 16}) {
        EXPECT_FALSE(urma::IsUrmaCtpPrioritySupported(attr, priority));
    }
}

class UrmaQosTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        urma_init_attr_t attr{};
        const auto status = urma_init(&attr);
        if (status != URMA_SUCCESS && status != URMA_EEXIST) {
            return;
        }
        int count = 0;
        auto** devices = urma_get_device_list(&count);
        _mock = devices && count > 0 &&
            strcmp(devices[0]->name, "mock_urma_device") == 0;
        urma_free_device_list(devices);
        if (status == URMA_SUCCESS) {
            urma_uninit();
        }
        if (!_mock) {
            return;
        }
        urma::g_skip_urma_init = false;
        urma::FLAGS_urma_buffer_count = 1024;
        urma::FLAGS_urma_prepared_jetty_cnt = 2;
        urma::GlobalUrmaInitializeOrDie();
    }
    void SetUp() override {
        if (!_mock) {
            GTEST_SKIP() << "URMA QoS integration tests use the link-time mock";
        }
        ASSERT_TRUE(urma::IsUrmaAvailable());
        ASSERT_NE(nullptr, urma::GetUrmaContext());
    }
    static bool _mock;
    static urma::UrmaEndpoint* Endpoint(SocketUniquePtr& socket) {
        return static_cast<UrmaTransport*>(socket->_transport.get())->GetUrmaEp();
    }
};
bool UrmaQosTest::_mock = false;

TEST_F(UrmaQosTest, rejects_invalid_and_unsupported_priority) {
    butil::EndPoint address;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:8123", &address));
    for (int priority : {-2, 16, 3, 9}) {
        ChannelOptions co;
        co.socket_mode = SOCKET_MODE_URMA;
        co.urma_qos.priority = priority;
        Channel channel;
        EXPECT_EQ(-1, channel.Init(address, &co));
        ServerOptions so;
        so.socket_mode = SOCKET_MODE_URMA;
        so.urma_qos = co.urma_qos;
        Server server;
        EXPECT_EQ(-1, server.Start("127.0.0.1:0", &so));
        if (!co.urma_qos.IsValid()) {
            SocketOptions options;
            options.socket_mode = SOCKET_MODE_URMA;
            options.urma_qos = co.urma_qos;
            SocketId sid;
            EXPECT_EQ(-1, Socket::Create(options, &sid));
            EXPECT_EQ(EINVAL, errno);
        } else {
            SocketOptions options;
            options.socket_mode = SOCKET_MODE_URMA;
            options.urma_qos = co.urma_qos;
            SocketId sid;
            ASSERT_EQ(0, Socket::Create(options, &sid));
            SocketUniquePtr socket;
            ASSERT_EQ(0, Socket::Address(sid, &socket));
            EXPECT_EQ(-1, urma::UrmaQosTestPeer::Allocate(Endpoint(socket)));
            EXPECT_EQ(EINVAL, errno);
            socket->SetFailed();
        }
    }
}

TEST_F(UrmaQosTest, channels_isolate_priority_and_inherit_to_child_connections) {
    ChannelOptions co;
    co.socket_mode = SOCKET_MODE_URMA;
    Channel automatic, low, same, high, named, tcp, tcp_with_qos;
    ASSERT_EQ(0, automatic.Init("127.0.0.1:8123", &co));
    co.urma_qos.priority = 6;
    ASSERT_EQ(0, low.Init("127.0.0.1:8123", &co));
    ASSERT_EQ(0, same.Init("127.0.0.1:8123", &co));
    ASSERT_EQ(0, named.Init("list://127.0.0.1:8123", "rr", &co));
    co.urma_qos.priority = 10;
    ASSERT_EQ(0, high.Init("127.0.0.1:8123", &co));
    EXPECT_EQ(low._server_id, same._server_id);
    EXPECT_NE(low._server_id, high._server_id);
    EXPECT_NE(low._server_id, automatic._server_id);
    co.socket_mode = SOCKET_MODE_TCP;
    ASSERT_EQ(0, tcp_with_qos.Init("127.0.0.1:8123", &co));
    ASSERT_EQ(0, tcp.Init("127.0.0.1:8123", nullptr));
    EXPECT_EQ(tcp._server_id, tcp_with_qos._server_id);
    EXPECT_NE(tcp._server_id, automatic._server_id);
    SocketUniquePtr main, pooled, short_socket, selected;
    ASSERT_EQ(0, Socket::Address(low._server_id, &main));
    ASSERT_EQ(0, main->GetPooledSocket(&pooled));
    ASSERT_EQ(0, main->GetShortSocket(&short_socket));
    LoadBalancer::SelectIn input{};
    LoadBalancer::SelectOut output(&selected);
    ASSERT_EQ(0, named._lb->SelectServer(input, &output));
    for (auto* socket : {main.get(), pooled.get(), short_socket.get(), selected.get()}) {
        EXPECT_EQ(6, socket->urma_qos().priority);
    }
    EXPECT_EQ(main->id(), selected->id());
    EXPECT_EQ(0, pooled->ReturnToPool());
    short_socket->SetFailed();
}

TEST_F(UrmaQosTest, prepared_jettys_are_used_only_for_matching_priority) {
    urma::UrmaEndpoint::GlobalRelease();
    ASSERT_EQ(0, urma::UrmaEndpoint::GlobalInitialize());
    // Allocate priority 10 before consuming the two prepared default Jettys.
    for (int priority : {10, -1, 6, 10}) {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        options.urma_qos.priority = priority;
        SocketId sid;
        ASSERT_EQ(0, Socket::Create(options, &sid));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(sid, &socket));
        auto* ep = Endpoint(socket);
        ASSERT_EQ(0, urma::UrmaQosTestPeer::Allocate(ep));
        EXPECT_EQ(priority == -1 ? 6 : priority, urma::UrmaQosTestPeer::Priority(ep));
        std::ostringstream debug;
        ep->DebugInfo(debug);
        EXPECT_NE(std::string::npos, debug.str().find("urma_effective_priority="));
        socket->SetFailed();
    }
}

class UrmaQosEcho : public ::test::EchoService {
    void Echo(google::protobuf::RpcController*, const ::test::EchoRequest* request,
              ::test::EchoResponse* response, google::protobuf::Closure* done) override {
        response->set_message(request->message());
        done->Run();
    }
};

TEST_F(UrmaQosTest, request_and_response_priorities_apply_across_restart) {
    GFLAGS_NAMESPACE::FlagSaver saved_flags;
    UrmaQosEcho service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    for (int response_priority : {6, 10}) {
        urma::FLAGS_urma_client_handshake_version = response_priority == 6 ? 2 : 3;
        ServerOptions so;
        so.socket_mode = SOCKET_MODE_URMA;
        so.urma_qos.priority = response_priority;
        ASSERT_EQ(0, server.Start("127.0.0.1:0", &so));
        ChannelOptions co;
        co.socket_mode = SOCKET_MODE_URMA;
        co.urma_qos.priority = response_priority == 6 ? 10 : 6;
        co.timeout_ms = 2000;
        co.max_retry = 0;
        co.connection_type = "single";
        Channel channel;
        ASSERT_EQ(0, channel.Init(server.listen_address(), &co));
        Controller cntl;
        ::test::EchoRequest request;
        ::test::EchoResponse response;
        request.set_message("URMA QoS");
        ::test::EchoService_Stub(&channel).Echo(&cntl, &request, &response, nullptr);
        EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
        EXPECT_EQ(request.message(), response.message());
        SocketUniquePtr client;
        ASSERT_EQ(0, Socket::Address(channel._server_id, &client));
        EXPECT_EQ(co.urma_qos.priority, urma::UrmaQosTestPeer::Priority(Endpoint(client)));
        std::vector<SocketId> sockets;
        server._am->ListConnections(&sockets);
        ASSERT_EQ(1u, sockets.size());
        SocketUniquePtr accepted;
        ASSERT_EQ(0, Socket::Address(sockets[0], &accepted));
        EXPECT_EQ(response_priority, accepted->urma_qos().priority);
        EXPECT_EQ(response_priority, urma::UrmaQosTestPeer::Priority(Endpoint(accepted)));
        accepted.reset();
        client.reset();
        server.Stop(0);
        server.Join();
    }
}

#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
