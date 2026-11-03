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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#if BRPC_WITH_URMA
#include <dlfcn.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/urma/mock_urma.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"
#include "bthread/bthread.h"
#include "butil/time.h"
#include "echo.pb.h"

DEFINE_bool(urma_stability_datapath, false, "Run end-to-end URMA tests");
DEFINE_int32(urma_stability_connections, 8, "Independent live URMA connections");
DEFINE_int32(urma_stability_rounds, 2, "Mixed-size rounds per connection");

namespace brpc {
namespace urma {
extern bool g_skip_urma_init;
}  // namespace urma
}  // namespace brpc
namespace butil {
namespace iobuf {
extern void* (*blockmem_allocate)(size_t);
extern void (*blockmem_deallocate)(void*);
}  // namespace iobuf
}  // namespace butil

namespace {
using namespace brpc;
using namespace brpc::urma;

// Resolve mock-only entry points so this binary also links with real liburma.
auto mock_fault =
    reinterpret_cast<decltype(&brpc_urma_mock_fault)>(dlsym(RTLD_DEFAULT, "brpc_urma_mock_fault"));
auto mock_release = reinterpret_cast<decltype(&brpc_urma_mock_release)>(
    dlsym(RTLD_DEFAULT, "brpc_urma_mock_release"));
auto mock_resources = reinterpret_cast<decltype(&brpc_urma_mock_resources)>(
    dlsym(RTLD_DEFAULT, "brpc_urma_mock_resources"));

class UrmaFaultTest : public testing::Test {
   protected:
    void SetUp() override {
        if (!mock_fault || !mock_release || !mock_resources) {
            GTEST_SKIP() << "Requires the URMA link-time mock";
        }
        before = mock_resources();
        for (size_t i = 0; i < 2; ++i) {
            SocketId id;
            ASSERT_EQ(0, Socket::Create(SocketOptions(), &id));
            ASSERT_EQ(0, Socket::Address(id, &sockets[i]));
            endpoints[i].reset(new UrmaEndpoint(sockets[i].get()));
            InitEndpoint(endpoints[i].get());
            ASSERT_FALSE(HasFatalFailure());
        }
        for (size_t i = 0; i < 2; ++i) {
            ParsedHello hello;
            endpoints[1 - i]->MakeLocalParsedHello(&hello);
            endpoints[i]->ApplyRemoteHello(hello);
            ASSERT_EQ(0, endpoints[i]->ImportPeer(hello));
        }
    }

    // Construct resources without registering a background CQ drainer. Tests
    // advance actual endpoint completions explicitly to select each interleaving.
    void InitEndpoint(UrmaEndpoint* ep) {
        auto* ctx = GetUrmaContext();
        ep->_resource = new UrmaResource;
        auto* r = ep->_resource;
        r->jfce = urma_create_jfce(ctx);
        ASSERT_NE(nullptr, r->jfce);
        urma_jfc_cfg_t cq{};
        cq.depth = ep->_sq_size + ep->_rq_size;
        cq.jfce = r->jfce;
        r->jfc = urma_create_jfc(ctx, &cq);
        ASSERT_NE(nullptr, r->jfc);
        urma_jfr_cfg_t rq{};
        rq.depth = ep->_rq_size;
        rq.trans_mode = URMA_TM_RM;
        rq.max_sge = 1;
        rq.jfc = r->jfc;
        r->jfr = urma_create_jfr(ctx, &rq);
        ASSERT_NE(nullptr, r->jfr);
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = ep->_sq_size;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.max_sge = GetUrmaMaxSge();
        cfg.jfs_cfg.jfc = r->jfc;
        cfg.shared.jfr = r->jfr;
        cfg.shared.jfc = r->jfc;
        r->jetty = urma_create_jetty(ctx, &cfg);
        ASSERT_NE(nullptr, r->jetty);
        ep->_sbuf.resize(ep->_sq_size - ep->RESERVED_WR_NUM);
        ep->_rbuf.resize(ep->_rq_size);
        ep->_rbuf_data.resize(ep->_rq_size, nullptr);
        ASSERT_EQ(0, ep->PostRecv(ep->_rq_size, true));
    }

    void TearDown() override {
        for (size_t i = 0; i < 2; ++i) {
            endpoints[i].reset();
            if (sockets[i]) sockets[i]->SetFailed();
        }
        if (mock_resources) {
            const auto after = mock_resources();
            EXPECT_EQ(before.jettys, after.jettys);
            EXPECT_EQ(before.jfcs, after.jfcs);
            EXPECT_EQ(before.jfrs, after.jfrs);
            EXPECT_EQ(before.jfces, after.jfces);
            EXPECT_EQ(before.targets, after.targets);
            EXPECT_EQ(before.segments, after.segments);
            EXPECT_EQ(before.deferred, after.deferred);
        }
    }

    ssize_t Send(size_t i, const std::string& payload) {
        butil::IOBuf buf;
        buf.append(payload);
        butil::IOBuf* data[] = {&buf};
        return endpoints[i]->CutFromIOBufList(data, 1);
    }

    void Pump() {
        for (size_t turn = 0; turn < 256; ++turn) {
            int count = 0;
            for (const auto& ep : endpoints) {
                urma_cr_t cr[32]{};
                const int n = urma_poll_jfc(ep->_resource->jfc, 32, cr);
                ASSERT_GE(n, 0);
                count += n;
                for (int i = 0; i < n; ++i) {
                    ASSERT_GE(ep->HandleCompletion(cr[i]), 0);
                }
            }
            if (count == 0) return;
        }
        FAIL() << "Completion processing did not quiesce";
    }

    MockResources before{};
    SocketUniquePtr sockets[2];
    std::unique_ptr<UrmaEndpoint> endpoints[2];
};

TEST_F(UrmaFaultTest, delayed_credit_stalls_then_resumes_without_consuming_input) {
    auto* sender = endpoints[0].get();
    auto* receiver = endpoints[1].get();
    mock_fault(receiver->_resource->jetty, MOCK_HOLD_CREDIT, -1);
    const auto capacity = sender->_local_window_capacity;
    for (size_t i = 0; i < capacity; ++i) ASSERT_EQ(1, Send(0, "x"));
    Pump();
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(0, sender->_remote_rq_window_size.load());
    EXPECT_EQ(capacity, sender->_sq_window_size.load());
    butil::IOBuf blocked;
    blocked.append("resume");
    butil::IOBuf* data[] = {&blocked};
    EXPECT_EQ(-1, sender->CutFromIOBufList(data, 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ("resume", blocked.to_string());
    ASSERT_GT(mock_release(receiver->_resource->jetty), 0u);
    Pump();
    ASSERT_EQ(6, sender->CutFromIOBufList(data, 1));
    Pump();
    EXPECT_EQ(std::string(capacity, 'x') + "resume",
              receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaFaultTest, delayed_send_completions_hold_sq_until_buffers_are_reclaimed) {
    auto* sender = endpoints[0].get();
    mock_fault(sender->_resource->jetty, MOCK_HOLD_SEND_COMPLETION, -1);
    const auto capacity = sender->_local_window_capacity;
    for (size_t i = 0; i < capacity; ++i) ASSERT_EQ(1, Send(0, "s"));
    Pump();
    EXPECT_EQ(0, sender->_sq_window_size.load());
    EXPECT_GT(sender->_remote_rq_window_size.load(), 0);
    EXPECT_EQ(-1, Send(0, "blocked"));
    EXPECT_EQ(EAGAIN, errno);
    ASSERT_EQ(capacity, mock_release(sender->_resource->jetty));
    Pump();
    EXPECT_EQ(capacity, sender->_sq_window_size.load());
    for (const auto& buf : sender->_sbuf) EXPECT_TRUE(buf.empty());
    ASSERT_EQ(2, Send(0, "ok"));
    Pump();
}

TEST_F(UrmaFaultTest, rejected_data_post_restores_windows) {
    auto* sender = endpoints[0].get();
    const auto sq = sender->_sq_window_size.load();
    const auto remote = sender->_remote_rq_window_size.load();
    mock_fault(sender->_resource->jetty, MOCK_FAIL_SEND, 1);
    EXPECT_EQ(-1, Send(0, "rejected"));
    EXPECT_EQ(EIO, errno);
    EXPECT_EQ(sq, sender->_sq_window_size.load());
    EXPECT_EQ(remote, sender->_remote_rq_window_size.load());
    urma_cr_t cr{};
    EXPECT_EQ(0, urma_poll_jfc(sender->_resource->jfc, 1, &cr));
    sender->Reset();
    EXPECT_EQ(nullptr, sender->_resource);
    EXPECT_TRUE(sender->_sbuf.empty());
}

TEST_F(UrmaFaultTest, rejected_credit_post_retains_credit_for_retry) {
    auto* receiver = endpoints[1].get();
    const auto budget = receiver->_sq_imm_window_size;
    mock_fault(receiver->_resource->jetty, MOCK_FAIL_CREDIT, 1);
    const size_t count = receiver->_remote_window_capacity / 2 + 1;
    for (size_t i = 0; i < count; ++i) ASSERT_EQ(1, Send(0, "c"));
    Pump();
    EXPECT_EQ(count, receiver->_new_rq_wrs.load());
    EXPECT_EQ(budget, receiver->_sq_imm_window_size);
    ASSERT_EQ(0, receiver->SendAck(0));
    Pump();
    EXPECT_EQ(0, receiver->_new_rq_wrs.load());
    EXPECT_EQ(budget, receiver->_sq_imm_window_size);
    EXPECT_EQ(endpoints[0]->_local_window_capacity, endpoints[0]->_remote_rq_window_size.load());
}

TEST_F(UrmaFaultTest, completion_error_does_not_restore_a_successful_send_slot) {
    auto* sender = endpoints[0].get();
    mock_fault(sender->_resource->jetty, MOCK_ERROR_COMPLETION, 1);
    ASSERT_EQ(3, Send(0, "bad"));
    urma_cr_t cr{};
    ASSERT_EQ(1, urma_poll_jfc(sender->_resource->jfc, 1, &cr));
    EXPECT_EQ(-1, sender->HandleCompletion(cr));
    EXPECT_EQ(EIO, errno);
    EXPECT_EQ(sender->_local_window_capacity - 1, sender->_sq_window_size.load());
}

TEST_F(UrmaFaultTest, receive_repost_failure_is_reported) {
    auto* receiver = endpoints[1].get();
    mock_fault(receiver->_resource->jetty, MOCK_FAIL_RECV, 1);
    ASSERT_EQ(7, Send(0, "receive"));
    urma_cr_t cr{};
    ASSERT_EQ(1, urma_poll_jfc(receiver->_resource->jfc, 1, &cr));
    EXPECT_EQ(-1, receiver->HandleCompletion(cr));
    EXPECT_EQ(ENOMEM, errno);
}

TEST_F(UrmaFaultTest, invalid_and_duplicate_credit_cannot_overflow_window) {
    auto* sender = endpoints[0].get();
    urma_cr_t cr{};
    cr.status = URMA_CR_SUCCESS;
    cr.flag.bs.s_r = 1;
    cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
    cr.imm_data = sender->_local_window_capacity + 1;
    EXPECT_EQ(-1, sender->HandleCompletion(cr));
    EXPECT_EQ(EPROTO, errno);
    cr.imm_data = 1;
    EXPECT_EQ(-1, sender->HandleCompletion(cr));
    EXPECT_EQ(EPROTO, errno);
    EXPECT_EQ(sender->_local_window_capacity, sender->_remote_rq_window_size.load());
}

TEST_F(UrmaFaultTest, reset_discards_old_completions_before_recreating_resources) {
    auto* sender = endpoints[0].get();
    const auto old_id = sender->_resource->jetty->jetty_id.id;
    mock_fault(sender->_resource->jetty, MOCK_HOLD_SEND_COMPLETION, -1);
    ASSERT_EQ(3, Send(0, "old"));
    ASSERT_GT(mock_resources().deferred, before.deferred);
    sender->Reset();
    EXPECT_EQ(before.deferred, mock_resources().deferred);
    InitEndpoint(sender);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_NE(old_id, sender->_resource->jetty->jetty_id.id);
    urma_cr_t cr{};
    EXPECT_EQ(0, urma_poll_jfc(sender->_resource->jfc, 1, &cr));
    EXPECT_EQ(0, sender->_sq_sent);
    EXPECT_EQ(0, sender->_sq_current);
}

TEST_F(UrmaFaultTest, pool_exhaustion_rejects_unregistered_send_and_recovers) {
    std::string value;
    ASSERT_TRUE(gflags::GetCommandLineOption("urma_buffer_count", &value));
    const size_t capacity = std::stoul(value);
    ASSERT_TRUE(gflags::GetCommandLineOption("urma_buffer_size", &value));
    const size_t size = std::stoul(value);
    using Block = std::unique_ptr<void, void (*)(void*)>;
    std::vector<Block> held;
    void* fallback = nullptr;
    for (size_t i = 0; i <= capacity; ++i) {
        void* p = butil::iobuf::blockmem_allocate(size);
        ASSERT_NE(nullptr, p);
        if (!GetPoolSegFor(p)) {
            fallback = p;
            break;
        }
        held.emplace_back(p, butil::iobuf::blockmem_deallocate);
    }
    ASSERT_NE(nullptr, fallback);
    butil::IOBuf buf;
    ASSERT_EQ(0, buf.append_user_data(fallback, 16, butil::iobuf::blockmem_deallocate));
    butil::IOBuf* data[] = {&buf};
    EXPECT_EQ(-1, endpoints[0]->CutFromIOBufList(data, 1));
    EXPECT_EQ(ERDMAMEM, errno);
    buf.clear();
    held.clear();
    ASSERT_EQ(9, Send(0, "recovered"));
    Pump();
    EXPECT_EQ("recovered", endpoints[1]->_input_processor.read_buf().to_string());
}

TEST(UrmaPoolTest, exhaustion_falls_back_and_released_blocks_are_reusable) {
    if (!mock_resources) GTEST_SKIP() << "Requires the URMA mock";
    std::string value;
    ASSERT_TRUE(gflags::GetCommandLineOption("urma_buffer_count", &value));
    const size_t capacity = std::stoul(value);
    ASSERT_TRUE(gflags::GetCommandLineOption("urma_buffer_size", &value));
    const size_t size = std::stoul(value);
    std::vector<void*> blocks;
    size_t registered = 0;
    for (size_t i = 0; i <= capacity; ++i) {
        void* p = butil::iobuf::blockmem_allocate(size);
        ASSERT_NE(nullptr, p);
        blocks.push_back(p);
        registered += GetPoolSegFor(p) != nullptr;
    }
    EXPECT_GT(registered, 0u);
    EXPECT_LT(registered, blocks.size());
    for (auto* p : blocks) butil::iobuf::blockmem_deallocate(p);
    void* reused = butil::iobuf::blockmem_allocate(size);
    ASSERT_NE(nullptr, reused);
    EXPECT_NE(nullptr, GetPoolSegFor(reused));
    butil::iobuf::blockmem_deallocate(reused);
}

class StabilityEchoService : public test::EchoService {
   public:
    void Echo(google::protobuf::RpcController* controller, const test::EchoRequest* request,
              test::EchoResponse* response, google::protobuf::Closure* done) override {
        ClosureGuard guard(done);
        auto* cntl = static_cast<Controller*>(controller);
        response->set_message(request->message());
        response->set_receiving_socket_id(cntl->_current_call.sending_sock->id());
        cntl->response_attachment().append(cntl->request_attachment());
        if (request->close_fd()) {
            entered.store(true);
            while (!release.load()) bthread_usleep(1000);
        }
    }
    std::atomic<bool> entered{false};
    std::atomic<bool> release{false};
};

class UrmaStabilityDatapathTest : public testing::Test {
   protected:
    void SetUp() override {
        if (!FLAGS_urma_stability_datapath) GTEST_SKIP() << "Pass --urma_stability_datapath";
        ASSERT_TRUE(IsUrmaAvailable());
        ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
        ServerOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        ASSERT_EQ(0, server.Start("127.0.0.1:0", &options));
    }
    void TearDown() override {
        service.release.store(true);
        server.Stop(0);
        server.Join();
    }
    void InitChannel(Channel* channel, const std::string& group) {
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        options.timeout_ms = 3000;
        options.connect_timeout_ms = 3000;
        options.max_retry = 0;
        options.connection_group = group;
        ASSERT_EQ(0, channel->Init(server.listen_address(), &options));
    }
    void CheckEcho(Channel* channel, size_t size, size_t sequence, SocketId* peer = nullptr) {
        test::EchoService_Stub stub(channel);
        test::EchoRequest request;
        test::EchoResponse response;
        Controller cntl;
        std::string payload(size, '\0');
        for (size_t i = 0; i < size; ++i) payload[i] = (i * 31 + sequence * 17) % 251;
        request.set_message("request-" + std::to_string(sequence));
        cntl.request_attachment().append(payload);
        stub.Echo(&cntl, &request, &response, nullptr);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        ASSERT_EQ(request.message(), response.message());
        ASSERT_TRUE(payload == cntl.response_attachment().to_string())
            << "size=" << size << " sequence=" << sequence;
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(response.receiving_socket_id(), &socket));
        ASSERT_EQ(SOCKET_MODE_URMA, socket->socket_mode());
        auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
        ASSERT_EQ(UrmaTransport::URMA_ON, transport->_urma_state.load());
        if (peer) *peer = socket->id();
    }
    StabilityEchoService service;
    Server server;
};

TEST_F(UrmaStabilityDatapathTest, mixed_sizes_on_independent_live_connections) {
    ASSERT_GT(FLAGS_urma_stability_connections, 0);
    ASSERT_GT(FLAGS_urma_stability_rounds, 0);
    std::vector<std::unique_ptr<Channel>> channels;
    std::vector<SocketId> channel_peers;
    std::set<SocketId> peers;
    for (int i = 0; i < FLAGS_urma_stability_connections; ++i) {
        channels.emplace_back(new Channel);
        InitChannel(channels.back().get(), "stability-" + std::to_string(i));
        ASSERT_FALSE(HasFatalFailure());
        SocketId peer;
        CheckEcho(channels.back().get(), 1, i, &peer);
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_TRUE(peers.insert(peer).second);
        channel_peers.push_back(peer);
    }
    ASSERT_EQ(static_cast<size_t>(FLAGS_urma_stability_connections), peers.size());
    const size_t block = GetUrmaRecvBlockSize();
    const size_t sizes[] = {0,     1,         511,   512,   513,   block - 1,
                            block, block + 1, 65535, 65536, 65537, 128 * 1024};
    for (int round = 0; round < FLAGS_urma_stability_rounds; ++round) {
        for (size_t first = 0; first < channels.size(); first += 8) {
            std::vector<std::thread> workers;
            for (size_t i = first; i < channels.size() && i < first + 8; ++i) {
                workers.emplace_back([&, i] {
                    SocketId peer = INVALID_SOCKET_ID;
                    CheckEcho(channels[i].get(), sizes[(i + round) % 12],
                              channels.size() * round + i, &peer);
                    EXPECT_EQ(channel_peers[i], peer);
                });
            }
            for (auto& worker : workers) worker.join();
            ASSERT_FALSE(HasFatalFailure());
        }
        // Exercise every boundary even with a small connection count.
        for (size_t i = 0; i < 12; ++i) {
            SocketId peer = INVALID_SOCKET_ID;
            CheckEcho(channels[i % channels.size()].get(), sizes[i], round * 12 + i, &peer);
            EXPECT_EQ(channel_peers[i % channels.size()], peer);
            ASSERT_FALSE(HasFatalFailure());
        }
    }
}

TEST_F(UrmaStabilityDatapathTest, peer_close_fails_inflight_rpc_and_new_connection_recovers) {
    Channel channel;
    InitChannel(&channel, "peer-close");
    SocketId peer;
    CheckEcho(&channel, 512, 1, &peer);
    ASSERT_FALSE(HasFatalFailure());
    test::EchoService_Stub stub(&channel);
    test::EchoRequest request;
    test::EchoResponse response;
    Controller cntl;
    request.set_message("inflight");
    request.set_close_fd(true);
    const auto call = cntl.call_id();
    const int64_t start = butil::gettimeofday_us();
    stub.Echo(&cntl, &request, &response, DoNothing());
    const int64_t deadline = start + 2000000;
    while (!service.entered.load() && butil::gettimeofday_us() < deadline) bthread_usleep(1000);
    EXPECT_TRUE(service.entered.load());
    SocketUniquePtr socket;
    if (Socket::Address(peer, &socket) == 0) {
        shutdown(socket->fd(), SHUT_RDWR);
        socket->SetFailed(ECONNRESET, "Injected peer close");
    }
    // Let the CQ drainer run even when user code is processed inline.
    service.release.store(true);
    Join(call);
    EXPECT_TRUE(cntl.Failed());
    EXPECT_LT(butil::gettimeofday_us() - start, 4000000);
    Channel recovered;
    InitChannel(&recovered, "peer-recovered");
    SocketId new_peer;
    CheckEcho(&recovered, 65537, 2, &new_peer);
    EXPECT_NE(peer, new_peer);
}

TEST_F(UrmaStabilityDatapathTest, peer_close_while_waiting_for_credit_finishes_writer) {
    if (!mock_fault) GTEST_SKIP() << "Requires credit injection in the URMA mock";
    Channel channel;
    InitChannel(&channel, "credit-close");
    SocketId peer;
    CheckEcho(&channel, 1, 1, &peer);
    ASSERT_FALSE(HasFatalFailure());
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(peer, &socket));
    auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->GetUrmaEp();
    const auto deferred = mock_resources().deferred;
    mock_fault(ep->_resource->jetty, MOCK_HOLD_CREDIT, -1);
    test::EchoService_Stub stub(&channel);
    test::EchoRequest request;
    test::EchoResponse response;
    Controller cntl;
    request.set_message("waiting-for-credit");
    cntl.request_attachment().append(std::string(256 * 1024, 'w'));
    const auto call = cntl.call_id();
    const int64_t start = butil::gettimeofday_us();
    stub.Echo(&cntl, &request, &response, DoNothing());
    while (mock_resources().deferred == deferred && butil::gettimeofday_us() < start + 2000000)
        bthread_usleep(1000);
    EXPECT_GT(mock_resources().deferred, deferred);
    shutdown(socket->fd(), SHUT_RDWR);
    socket->SetFailed(ECONNRESET, "Injected close while writer waits for credit");
    Join(call);
    EXPECT_TRUE(cntl.Failed());
    EXPECT_LT(butil::gettimeofday_us() - start, 4000000);
    Channel recovered;
    InitChannel(&recovered, "credit-recovered");
    CheckEcho(&recovered, 65537, 3);
}

TEST(UrmaHandshakeFaultTest, peer_exit_during_handshake_finishes_rpc) {
    if (!FLAGS_urma_stability_datapath) GTEST_SKIP() << "Pass --urma_stability_datapath";
    const int listener = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listener, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(0, bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)));
    ASSERT_EQ(0, listen(listener, 1));
    socklen_t len = sizeof(addr);
    ASSERT_EQ(0, getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len));
    std::thread peer([listener] {
        const int fd = accept(listener, nullptr, nullptr);
        if (fd >= 0) {
            char magic[4];
            (void)recv(fd, magic, sizeof(magic), MSG_WAITALL);
            close(fd);
        }
    });
    Channel channel;
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_URMA;
    options.timeout_ms = 1000;
    options.connect_timeout_ms = 1000;
    options.max_retry = 0;
    const int rc = channel.Init("127.0.0.1", ntohs(addr.sin_port), &options);
    EXPECT_EQ(0, rc);
    if (rc == 0) {
        test::EchoService_Stub stub(&channel);
        Controller cntl;
        test::EchoRequest request;
        test::EchoResponse response;
        request.set_message("handshake");
        const int64_t start = butil::gettimeofday_us();
        stub.Echo(&cntl, &request, &response, nullptr);
        EXPECT_TRUE(cntl.Failed());
        EXPECT_LT(butil::gettimeofday_us() - start, 2000000);
    }
    shutdown(listener, SHUT_RDWR);
    peer.join();
    close(listener);
}
}  // namespace
#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
#if BRPC_WITH_URMA
    gflags::SetCommandLineOption("urma_buffer_count", "4096");
    gflags::SetCommandLineOption("urma_sq_size", "16");
    gflags::SetCommandLineOption("urma_rq_size", "16");
    gflags::SetCommandLineOption("urma_prepared_jetty_cnt", "0");
#endif
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_URMA
    brpc::urma::g_skip_urma_init = !(mock_resources || FLAGS_urma_stability_datapath);
    if (!brpc::urma::g_skip_urma_init) brpc::urma::GlobalUrmaInitializeOrDie();
#endif
    return RUN_ALL_TESTS();
}
