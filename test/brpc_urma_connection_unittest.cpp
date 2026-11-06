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

// Connection lifecycle of UrmaTransport. A server and channels run in this
// process over the link-time URMA mock (-DWITH_URMA=ON -DURMA_USE_MOCK=ON);
// the tests skip themselves when the binary links a real liburma.

#include <sstream>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/synchronization/lock.h"
#include "butil/time.h"
#include "bthread/bthread.h"
#include "brpc/acceptor.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/details/server_private_accessor.h"
#include "brpc/urma/urma_helper.h"
#include "echo.pb.h"

namespace brpc {
namespace urma {
DECLARE_int32(urma_buffer_count);
DECLARE_string(urma_device);
}  // namespace urma
}  // namespace brpc

namespace {

// Server-side connections that are neither failed nor recycled.
std::vector<brpc::SocketId> LiveServerConnections(const brpc::Server* server) {
    std::vector<brpc::SocketId> ids;
    brpc::ServerPrivateAccessor(server).acceptor()->ListConnections(&ids);
    std::vector<brpc::SocketId> live;
    for (size_t i = 0; i < ids.size(); ++i) {
        brpc::SocketUniquePtr s;
        if (brpc::Socket::Address(ids[i], &s) == 0) {
            live.push_back(ids[i]);
        }
    }
    return live;
}

// Echo that also records, while the call holds its connection open, how the
// server's live connections are set up.
class ConnEchoService : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController*,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        response->set_message(request->message());
        std::ostringstream os;
        const std::vector<brpc::SocketId> live = LiveServerConnections(_server);
        for (size_t i = 0; i < live.size(); ++i) {
            brpc::Socket::DebugSocket(os, live[i]);
            os << '\n';
        }
        BAIDU_SCOPED_LOCK(_mutex);
        _connections = os.str();
    }

    void set_server(const brpc::Server* server) { _server = server; }

    std::string connections() {
        BAIDU_SCOPED_LOCK(_mutex);
        return _connections;
    }

private:
    const brpc::Server* _server{nullptr};
    butil::Mutex _mutex;
    std::string _connections;
};

class UrmaConnectionTest : public ::testing::Test {
protected:
    static void SetUpTestCase() {
        // Keep the registered buffer pool small for CI machines.
        brpc::urma::FLAGS_urma_buffer_count = 1024;
        brpc::urma::FLAGS_urma_device = "mock_urma_device";
        brpc::urma::GlobalUrmaInitializeOrDie();
        if (!brpc::urma::IsUrmaAvailable()) {
            return;
        }
        s_server = new brpc::Server;
        s_service = new ConnEchoService;
        s_service->set_server(s_server);
        ASSERT_EQ(0, s_server->AddService(s_service,
                                          brpc::SERVER_DOESNT_OWN_SERVICE));
        brpc::ServerOptions options;
        options.socket_mode = brpc::SOCKET_MODE_URMA;
        ASSERT_EQ(0, s_server->Start("127.0.0.1:0", &options));
    }

    static void TearDownTestCase() {
        if (s_server) {
            s_server->Stop(0);
            s_server->Join();
            delete s_server;
            s_server = nullptr;
        }
        delete s_service;
        s_service = nullptr;
    }

    void SetUp() override {
        if (s_server == nullptr) {
            GTEST_SKIP() << "URMA is unavailable; these tests need the mock";
        }
    }

    static brpc::Server* s_server;
    static ConnEchoService* s_service;
};

brpc::Server* UrmaConnectionTest::s_server = nullptr;
ConnEchoService* UrmaConnectionTest::s_service = nullptr;

// When a client closes its connection, by exiting or otherwise, the server
// must read the FIN and fail its side, which releases the URMA resources of
// the connection. It used to keep the socket, the TCP connection in
// CLOSE_WAIT and the URMA resources until the server exited: once the
// handshake started, OnNewDataFromTcp stopped releasing the socket's read
// events, so no read handler ran on the socket again.
TEST_F(UrmaConnectionTest, server_fails_socket_after_client_closes) {
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.socket_mode = brpc::SOCKET_MODE_URMA;
    // A short connection is closed by the client right after its call.
    options.connection_type = "short";
    options.timeout_ms = 5000;
    options.max_retry = 0;
    ASSERT_EQ(0, channel.Init(s_server->listen_address(), &options));
    test::EchoService_Stub stub(&channel);

    for (int i = 0; i < 3; ++i) {
        brpc::Controller cntl;
        test::EchoRequest req;
        test::EchoResponse res;
        req.set_message("hello");
        stub.Echo(&cntl, &req, &res, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ("hello", res.message());
        // The call ran over URMA, not over a TCP fallback (which always
        // noticed a closed connection).
        const std::string conns = s_service->connections();
        ASSERT_NE(std::string::npos, conns.find("state=ESTABLISHED"))
            << conns;

        const int64_t deadline_ms = butil::monotonic_time_ms() + 5000;
        while (!LiveServerConnections(s_server).empty() &&
               butil::monotonic_time_ms() < deadline_ms) {
            bthread_usleep(10000);
        }
        const std::vector<brpc::SocketId> live =
            LiveServerConnections(s_server);
        std::ostringstream os;
        for (size_t j = 0; j < live.size(); ++j) {
            brpc::Socket::DebugSocket(os, live[j]);
            os << '\n';
        }
        ASSERT_TRUE(live.empty())
            << "call " << i << ": the server still holds a connection the "
               "client closed:\n" << os.str();
    }
}

}  // namespace

#else  // BRPC_WITH_URMA

TEST(UrmaConnectionTest, disabled) {}

#endif  // BRPC_WITH_URMA
