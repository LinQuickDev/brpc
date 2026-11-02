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

#include <cerrno>
#include <sstream>
#include <thread>

#if BRPC_WITH_URMA
#include "brpc/builtin/sockets_service.h"
#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/span.h"
#include "brpc/urma/urma_observability.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"
#include "bthread/bthread.h"
#include "butil/third_party/rapidjson/document.h"
#include "butil/time.h"
#include "bvar/variable.h"
#include "echo.pb.h"

namespace butil {
namespace iobuf {
extern void* (*blockmem_allocate)(size_t);
extern void (*blockmem_deallocate)(void*);
}  // namespace iobuf
}  // namespace butil

DEFINE_bool(urma_test_datapath, false,
            "Exercise the complete URMA data path (use a mock build or URMA hardware)");

namespace brpc {
namespace urma {
DECLARE_int32(urma_trace_sample_every);
extern bool g_skip_urma_init;
}  // namespace urma
}  // namespace brpc

using namespace brpc;
using namespace brpc::urma;

TEST(UrmaObservabilityTest, counters_export_and_reset) {
    ConnectionStats stats;
    const auto before = bvar::Variable::describe_exposed("urma_send_bytes");
    stats.Add(send_bytes, 123);
    ConnectionSnapshot snapshot;
    stats.Snapshot(&snapshot);
    EXPECT_EQ(123u, snapshot.counters[send_bytes]);
    const uint64_t generation = snapshot.generation;
    EXPECT_EQ(std::stoll(before) + 123,
              std::stoll(bvar::Variable::describe_exposed("urma_send_bytes")));
    stats.Reset();
    stats.Snapshot(&snapshot);
    EXPECT_EQ(0u, snapshot.counters[send_bytes]);
    EXPECT_EQ(generation + 1, snapshot.generation);
}

TEST(UrmaObservabilityTest, concurrent_updates_and_snapshots) {
    ConnectionStats stats;
    std::thread writer([&] {
        for (size_t i = 0; i < 10000; ++i) {
            stats.Add(recv_bytes);
        }
    });
    ConnectionSnapshot snapshot;
    for (size_t i = 0; i < 100; ++i) {
        stats.Snapshot(&snapshot);
        EXPECT_LE(snapshot.counters[recv_bytes], 10000u);
    }
    writer.join();
    stats.Snapshot(&snapshot);
    EXPECT_EQ(10000u, snapshot.counters[recv_bytes]);
}

TEST(UrmaObservabilityTest, json_preserves_identifiers_and_escapes_reason) {
    ConnectionSnapshot snapshot;
    snapshot.socket_id = UINT64_MAX - 1;
    snapshot.reason = "quoted \"reason\"\n";
    std::ostringstream os;
    DescribeConnectionJson(os, snapshot);
    butil::rapidjson::Document document;
    document.Parse(os.str().c_str());
    ASSERT_FALSE(document.HasParseError());
    EXPECT_EQ(std::to_string(snapshot.socket_id), document["socket_id"].GetString());
    EXPECT_EQ(snapshot.reason, document["reason"].GetString());
}

TEST(UrmaObservabilityTest, pool_watermarks_survive_free) {
    const auto before = GetPoolSnapshot();
    PoolAllocated();
    PoolAllocated();
    EXPECT_EQ(before.in_use + 2, GetPoolSnapshot().in_use);
    PoolDeallocated();
    PoolDeallocated();
    EXPECT_EQ(before.in_use, GetPoolSnapshot().in_use);
    EXPECT_GE(GetPoolSnapshot().peak_in_use, before.in_use + 2);
}

TEST(UrmaObservabilityTest, history_is_bounded) {
    TraceHistory history;
    for (size_t i = 0; i < 100; ++i) {
        TraceRecord record;
        record.span_id = i;
        history.Push(record);
    }
    const auto records = history.Snapshot();
    ASSERT_EQ(64u, records.size());
    EXPECT_EQ(36u, records.front().span_id);
    EXPECT_EQ(99u, records.back().span_id);
}

TEST(UrmaObservabilityTest, trace_split_and_merged_wrs_keep_rpc_identity) {
    auto history = std::make_shared<TraceHistory>();
    auto span1 = Span::CreateClientSpan("first", 0);
    auto span2 = Span::CreateClientSpan("second", 0);
    auto first = std::make_shared<WriteTrace>(span1, 7, history);
    auto second = std::make_shared<WriteTrace>(span2, 7, history);
    // First RPC spans two WRs; the second WR also carries a second RPC.
    first->Posted(100);
    first->Completed();  // May complete before posting has finished.
    EXPECT_TRUE(history->Snapshot().empty());
    first->Posted(20);
    second->Posted(40);
    first->Waited(true, false, 23);
    first->PostingDone();
    second->PostingDone();
    std::thread completion([&] {
        first->Completed();
        second->Completed();
    });
    completion.join();
    const auto records = history->Snapshot();
    ASSERT_EQ(2u, records.size());
    EXPECT_EQ(span1->span_id(), records[0].span_id);
    EXPECT_EQ(120u, records[0].bytes);
    EXPECT_EQ(2u, records[0].completed_wrs);
    EXPECT_EQ(23u, records[0].sq_wait_us);
    EXPECT_EQ(span2->span_id(), records[1].span_id);
    EXPECT_EQ(40u, records[1].bytes);
    EXPECT_EQ(1u, records[1].completed_wrs);
}

TEST(UrmaObservabilityTest, failed_post_is_not_counted_as_submitted) {
    auto history = std::make_shared<TraceHistory>();
    auto span = Span::CreateClientSpan("failed", 0);
    WriteTrace trace(span, 9, history);
    trace.Posted(512);
    trace.Rejected(512);
    trace.PostingDone(EIO);
    trace.PostingDone(EIO);  // Failure cleanup must not duplicate the record.
    auto records = history->Snapshot();
    ASSERT_EQ(1u, records.size());
    EXPECT_EQ(0u, records[0].wrs);
    EXPECT_EQ(0u, records[0].bytes);
    EXPECT_EQ(EIO, records[0].error);
}

TEST(UrmaObservabilityTest, transport_trace_is_opt_in) {
    ConnectionStats stats;
    const int previous = FLAGS_urma_trace_sample_every;
    auto span = Span::CreateClientSpan("sample", 0);
    FLAGS_urma_trace_sample_every = 0;
    EXPECT_FALSE(stats.NewTrace(span, 1));
    FLAGS_urma_trace_sample_every = 1;
    EXPECT_TRUE(stats.NewTrace(span, 1));
    EXPECT_FALSE(stats.NewTrace(nullptr, 1));
    FLAGS_urma_trace_sample_every = previous;
}

class UrmaDiagnosticsTest : public testing::Test {
   protected:
    void SetUp() override {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        ASSERT_EQ(0, Socket::Create(options, &_sid));
        ASSERT_EQ(0, Socket::Address(_sid, &_socket));
        _transport = static_cast<UrmaTransport*>(_socket->_transport.get());
        _endpoint = _transport->GetUrmaEp();
    }
    void TearDown() override {
        if (_socket) { _socket->SetFailed(); _socket.reset(); }
    }
    SocketId _sid;
    SocketUniquePtr _socket;
    UrmaTransport* _transport;
    UrmaEndpoint* _endpoint;
};

TEST_F(UrmaDiagnosticsTest, negotiated_capacity_uses_peer_limits) {
    ParsedHello hello;
    hello.buffer_size = 8192;
    hello.recv_buffer_cnt = 15;
    _endpoint->ApplyRemoteHello(hello);
    const auto snapshot = _endpoint->GetSnapshot();
    EXPECT_EQ(13u, snapshot.window_capacity);  // min(local SQ, peer RQ) - 3.
    EXPECT_EQ(13u, snapshot.sq_remaining);
    EXPECT_EQ(13u, snapshot.remote_credit_remaining);
}

TEST_F(UrmaDiagnosticsTest, separate_wait_causes_and_reset) {
    _endpoint->RecordWait(true, false, 40);
    _endpoint->RecordWait(false, true, 80);
    auto snapshot = _endpoint->GetSnapshot();
    EXPECT_EQ(40u, snapshot.counters[sq_wait_us]);
    EXPECT_EQ(80u, snapshot.counters[remote_credit_wait_us]);
    _endpoint->Reset();
    snapshot = _endpoint->GetSnapshot();
    EXPECT_EQ(0u, snapshot.counters[sq_wait_us]);
    EXPECT_EQ(0u, snapshot.counters[remote_credit_wait_us]);
    EXPECT_EQ("UNINIT", snapshot.state);
}

TEST_F(UrmaDiagnosticsTest, fallback_reason_is_available_after_resources_release) {
    _endpoint->FallbackToTcp(_transport, false, "injected allocation failure", ENOMEM);
    std::ostringstream os;
    EXPECT_TRUE(_socket->DescribeUrmaJson(os));
    butil::rapidjson::Document document;
    document.Parse(os.str().c_str());
    ASSERT_FALSE(document.HasParseError());
    EXPECT_STREQ("FALLBACK_TCP", document["state"].GetString());
    EXPECT_STREQ("injected allocation failure", document["reason"].GetString());
    EXPECT_EQ(ENOMEM, document["error"].GetInt());
    EXPECT_EQ(1u, document["counters"]["tcp_fallback"].GetUint64());
}

TEST_F(UrmaDiagnosticsTest, failed_handshake_retains_reason) {
    _endpoint->FailHandshake(_transport, EPROTO, "invalid peer hello");
    const auto snapshot = _endpoint->GetSnapshot();
    EXPECT_EQ("FAILED", snapshot.state);
    EXPECT_TRUE(snapshot.socket_failed);
    EXPECT_EQ("invalid peer hello", snapshot.reason);
    EXPECT_EQ(1u, snapshot.counters[handshake_failed]);
}

TEST_F(UrmaDiagnosticsTest, builtin_inventory_returns_structured_snapshot) {
    Controller controller;
    controller.http_request()._unresolved_path = "urma";
    SocketsService service;
    service.default_method(&controller, nullptr, nullptr, nullptr);
    ASSERT_FALSE(controller.Failed());
    butil::rapidjson::Document document;
    document.Parse(controller.response_attachment().to_string().c_str());
    ASSERT_FALSE(document.HasParseError());
    EXPECT_EQ(1, document["schema_version"].GetInt());
    bool found = false;
    for (auto it = document["connections"].Begin(); it != document["connections"].End(); ++it) {
        if ((*it)["socket_id"].GetString() == std::to_string(_sid)) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

class DiagnosticEchoService : public test::EchoService {
    void Echo(google::protobuf::RpcController*, const test::EchoRequest* request,
              test::EchoResponse* response, google::protobuf::Closure* done) override {
        ClosureGuard guard(done);
        response->set_message(request->message());
    }
};

TEST(UrmaDatapathTest, large_concurrent_rpcs_have_counters_and_trace_completion) {
    if (!FLAGS_urma_test_datapath) {
        GTEST_SKIP() << "Pass --urma_test_datapath in a mock build or on URMA hardware";
    }
    gflags::SetCommandLineOption("urma_buffer_count", "256");
    gflags::SetCommandLineOption("urma_sq_size", "16");
    gflags::SetCommandLineOption("urma_rq_size", "16");
    gflags::SetCommandLineOption("urma_prepared_jetty_cnt", "0");
    FLAGS_enable_rpcz = true;
    FLAGS_urma_trace_sample_every = 1;
    DiagnosticEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    Channel channel;
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_URMA;
    options.timeout_ms = 5000;
    options.max_retry = 0;
    ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
    test::EchoService_Stub stub(&channel);
    test::EchoRequest requests[4];
    test::EchoResponse responses[4];
    Controller controllers[4];
    CallId calls[4];
    for (size_t i = 0; i < 4; ++i) {
        requests[i].set_message(std::string(128 * 1024, 'a' + i));
        calls[i] = controllers[i].call_id();
        stub.Echo(&controllers[i], &requests[i], &responses[i], DoNothing());
    }
    for (size_t i = 0; i < 4; ++i) {
        Join(calls[i]);
        if (controllers[i].Failed()) {
            for (auto sid : ListDiagnosticSockets(1024)) {
                SocketUniquePtr socket;
                if (Socket::AddressFailedAsWell(sid, &socket) >= 0) {
                    std::ostringstream debug;
                    socket->DescribeUrmaJson(debug);
                    ADD_FAILURE() << controllers[i].ErrorText() << " " << debug.str();
                    socket->SetFailed();
                }
            }
            server.Stop(0);
            server.Join();
            return;
        }
        EXPECT_TRUE(requests[i].message() == responses[i].message())
            << "payload mismatch in RPC " << i;
    }
    uint64_t bulk = 0, submitted = 0, completed = 0, traced = 0;
    // The peer can respond before our final local send completion is drained.
    for (size_t attempt = 0; attempt < 1000; ++attempt) {
        bulk = submitted = completed = traced = 0;
        for (auto sid : ListDiagnosticSockets(1024)) {
            SocketUniquePtr socket;
            if (Socket::Address(sid, &socket) != 0 || socket->socket_mode() != SOCKET_MODE_URMA) {
                continue;
            }
            auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
            auto snapshot = transport->GetUrmaEp()->GetSnapshot();
            bulk += snapshot.counters[bulk_writes];
            submitted += snapshot.counters[send_wrs];
            completed += snapshot.counters[send_completed_wrs];
            for (const auto& trace : snapshot.traces) {
                if (trace.wrs > 1 && trace.error == 0) {
                    EXPECT_EQ(trace.wrs, trace.completed_wrs);
                    ++traced;
                }
            }
        }
        if (bulk >= 8 && submitted == completed && traced >= 4) {
            break;
        }
        bthread_usleep(1000);
    }
    EXPECT_GE(bulk, 8u);
    EXPECT_GT(submitted, 8u);
    EXPECT_EQ(submitted, completed);
    EXPECT_GE(traced, 4u);
    EXPECT_GT(GetPoolSnapshot().peak_in_use, 0u);
    for (auto sid : ListDiagnosticSockets(1024)) {
        SocketUniquePtr socket;
        if (Socket::Address(sid, &socket) != 0 || socket->socket_mode() != SOCKET_MODE_URMA) {
            continue;
        }
        auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
        auto* endpoint = transport->GetUrmaEp();
        const auto before = endpoint->GetSnapshot();
        if (before.state != "ESTABLISHED") {
            continue;
        }
        butil::IOBuf probe;
        probe.append("probe");
        butil::IOBuf* data[] = {&probe};
        const auto sq = endpoint->_sq_window_size.exchange(0, butil::memory_order_acq_rel);
        EXPECT_EQ(-1, endpoint->CutFromIOBufList(data, 1));
        EXPECT_EQ(EAGAIN, errno);
        const timespec deadline = butil::milliseconds_from_now(2);
        transport->WaitEpollOut(socket->_epollout_butex, false, deadline);
        endpoint->_sq_window_size.store(sq, butil::memory_order_release);
        const auto remote =
            endpoint->_remote_rq_window_size.exchange(0, butil::memory_order_acq_rel);
        EXPECT_EQ(-1, endpoint->CutFromIOBufList(data, 1));
        EXPECT_EQ(EAGAIN, errno);
        endpoint->_remote_rq_window_size.store(remote, butil::memory_order_release);
        const auto after = endpoint->GetSnapshot();
        EXPECT_GT(after.counters[sq_blocked], before.counters[sq_blocked]);
        EXPECT_GT(after.counters[remote_credit_blocked], before.counters[remote_credit_blocked]);
        EXPECT_GT(after.counters[sq_wait_us], before.counters[sq_wait_us]);
        EXPECT_EQ("probe", probe.to_string());
        break;
    }
    server.Stop(0);
    server.Join();
}

TEST(UrmaDatapathTest, pool_exhaustion_exports_allocation_fallback) {
    if (!FLAGS_urma_test_datapath) {
        GTEST_SKIP() << "Requires mock or URMA hardware";
    }
    gflags::SetCommandLineOption("urma_buffer_count", "256");
    gflags::SetCommandLineOption("urma_prepared_jetty_cnt", "0");
    GlobalUrmaInitializeOrDie();
    const auto before = GetPoolSnapshot();
    ASSERT_GT(before.capacity, 0u);
    const auto misses = std::stoll(bvar::Variable::describe_exposed("urma_pool_exhausted"));
    std::vector<void*> allocations;
    for (size_t i = 0; i <= before.capacity; ++i) {
        allocations.push_back(butil::iobuf::blockmem_allocate(before.buffer_size));
    }
    EXPECT_EQ(before.capacity, GetPoolSnapshot().in_use);
    EXPECT_GT(std::stoll(bvar::Variable::describe_exposed("urma_pool_exhausted")), misses);
    for (auto allocation : allocations) {
        if (allocation) {
            butil::iobuf::blockmem_deallocate(allocation);
        }
    }
    EXPECT_EQ(before.in_use, GetPoolSnapshot().in_use);
}

#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_URMA
    brpc::urma::g_skip_urma_init = !FLAGS_urma_test_datapath;
#endif
    return RUN_ALL_TESTS();
}
