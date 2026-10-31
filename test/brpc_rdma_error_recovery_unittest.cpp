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

#include <cerrno>
#include <cstring>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#if BRPC_WITH_RDMA
#include "brpc/global.h"
#include "brpc/input_messenger.h"
#include "brpc/rdma/rdma_endpoint.h"
#include "brpc/rdma/rdma_helper.h"
#include "brpc/rdma/rdma_recovery_policy.h"
#include "brpc/rdma_transport.h"
#include "brpc/socket.h"

namespace brpc {
namespace rdma {
extern bool g_skip_rdma_init;
extern butil::atomic<bool> g_rdma_available;
}  // namespace rdma
}  // namespace brpc

namespace {
using namespace brpc;

struct CompletionSource {
    ibv_wc_status status;
    bool delivered = false;
};

int PollError(ibv_cq* cq, int, ibv_wc* wc) {
    auto* source = static_cast<CompletionSource*>(cq->cq_context);
    if (source->delivered) {
        return 0;
    }
    source->delivered = true;
    std::memset(wc, 0, sizeof(*wc));
    wc->status = source->status;
    return 1;
}

int FailRecvPost(ibv_qp*, ibv_recv_wr*, ibv_recv_wr**) {
    return EIO;
}

class RdmaErrorRecoveryTest : public testing::Test {
protected:
    void SetUp() override {
        GlobalInitializeOrDie();
        _saved_mode = rdma::GetRdmaRecoveryMode();
        ResetPolicy();
    }

    void TearDown() override {
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(_saved_mode);
    }

    void ResetPolicy() {
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::AUTO);
    }

    void CreateSocket(SocketUniquePtr* socket) {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_RDMA;
        ASSERT_EQ(0, butil::str2endpoint("127.0.0.1:8123", &options.remote_side));
        options.user = get_client_side_messenger();
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        ASSERT_EQ(0, Socket::Address(id, socket));
    }

    static rdma::RdmaEndpoint* Endpoint(const SocketUniquePtr& socket) {
        return static_cast<RdmaTransport*>(socket->_transport.get())->GetRdmaEp();
    }

    void CheckCompletion(ibv_wc_status status, rdma::RdmaFailureReason reason,
                         bool cooldown) {
        ResetPolicy();
        SocketUniquePtr socket;
        ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
        auto* endpoint = Endpoint(socket);
        auto& policy = rdma::GlobalRdmaRecoveryPolicy();
        ASSERT_TRUE(policy.AllowNewConnection(_peer, &endpoint->_recovery_epoch));
        const auto before = policy.GetStatus();
        SocketOptions carrier_options;
        carrier_options.user = endpoint;
        SocketId carrier_id;
        ASSERT_EQ(0, Socket::Create(carrier_options, &carrier_id));
        SocketUniquePtr carrier;
        ASSERT_EQ(0, Socket::Address(carrier_id, &carrier));

        CompletionSource source{status};
        ibv_context context{};
        context.ops.poll_cq = PollError;
        ibv_cq cq{};
        cq.context = &context;
        cq.cq_context = &source;
        rdma::RdmaResource resource;
        resource.polling_cq = &cq;
        endpoint->_resource = &resource;
        endpoint->_cq_sid = carrier_id;
        endpoint->_state.store(rdma::RdmaEndpoint::ESTABLISHED);
        rdma::FLAGS_rdma_use_polling = true;

        rdma::RdmaEndpoint::PollCq(carrier.get());
        EXPECT_TRUE(source.delivered);
        EXPECT_TRUE(socket->Failed());
        EXPECT_EQ(reason, endpoint->_runtime_failure.load());
        EXPECT_EQ(!cooldown, policy.AllowNewConnection(_peer));
        EXPECT_TRUE(policy.AllowNewConnection("127.0.0.1:9999"));
        const auto after = policy.GetStatus();
        for (size_t i = 1; i < static_cast<size_t>(rdma::RdmaFailureReason::COUNT); ++i) {
            EXPECT_EQ(before.failures[i] + (i == static_cast<size_t>(reason)),
                      after.failures[i]);
        }
        if (reason != rdma::RdmaFailureReason::NONE) {
            endpoint->ReportFailure(rdma::RdmaFailureReason::CQ_ERROR);
            EXPECT_EQ(after.failures[static_cast<size_t>(rdma::RdmaFailureReason::CQ_ERROR)],
                      policy.GetStatus().failures[
                          static_cast<size_t>(rdma::RdmaFailureReason::CQ_ERROR)]);
        }

        endpoint->_resource = nullptr;
        endpoint->_cq_sid = INVALID_SOCKET_ID;
        resource.polling_cq = nullptr;
        carrier->_user = nullptr;
        carrier->SetFailed();
    }

    const std::string _peer = "127.0.0.1:8123";
    GFLAGS_NAMESPACE::FlagSaver _flags;
    rdma::RdmaRecoveryMode _saved_mode;
};

TEST_F(RdmaErrorRecoveryTest, fatal_timeout_and_general_completions_cool_peer) {
    for (auto status : {IBV_WC_FATAL_ERR, IBV_WC_RESP_TIMEOUT_ERR, IBV_WC_GENERAL_ERR}) {
        ASSERT_NO_FATAL_FAILURE(CheckCompletion(
            status, rdma::RdmaFailureReason::TRANSPORT_ERROR, true));
    }
}

TEST_F(RdmaErrorRecoveryTest, retry_and_rnr_completions_cool_peer) {
    ASSERT_NO_FATAL_FAILURE(CheckCompletion(
        IBV_WC_RETRY_EXC_ERR, rdma::RdmaFailureReason::RETRY_EXHAUSTED, true));
    ASSERT_NO_FATAL_FAILURE(CheckCompletion(
        IBV_WC_RNR_RETRY_EXC_ERR, rdma::RdmaFailureReason::RNR, true));
}

TEST_F(RdmaErrorRecoveryTest, access_and_flush_completions_preserve_admission) {
    for (auto status : {IBV_WC_LOC_PROT_ERR, IBV_WC_LOC_ACCESS_ERR,
                        IBV_WC_REM_ACCESS_ERR, IBV_WC_MW_BIND_ERR}) {
        ASSERT_NO_FATAL_FAILURE(CheckCompletion(
            status, rdma::RdmaFailureReason::ACCESS_ERROR, false));
    }
    ASSERT_NO_FATAL_FAILURE(CheckCompletion(
        IBV_WC_WR_FLUSH_ERR, rdma::RdmaFailureReason::NONE, false));
}

TEST_F(RdmaErrorRecoveryTest, receive_post_error_preserves_errno_and_reports_once) {
    for (bool established : {false, true}) {
        ResetPolicy();
        SocketUniquePtr socket;
        ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
        auto* endpoint = Endpoint(socket);
        auto& policy = rdma::GlobalRdmaRecoveryPolicy();
        ASSERT_TRUE(policy.AllowNewConnection(_peer, &endpoint->_recovery_epoch));
        const auto before = policy.GetStatus();
        ibv_context context{};
        context.ops.post_recv = FailRecvPost;
        ibv_qp qp{};
        qp.context = &context;
        rdma::RdmaResource resource;
        resource.qp = &qp;
        endpoint->_resource = &resource;
        endpoint->_state.store(established ? rdma::RdmaEndpoint::ESTABLISHED :
                                             rdma::RdmaEndpoint::C_ALLOC_QPCQ);
        char data[32]{};
        for (int attempt = 0; attempt < 2; ++attempt) {
            errno = EALREADY;
            EXPECT_EQ(-1, endpoint->DoPostRecv(data, sizeof(data)));
            EXPECT_EQ(EIO, errno);
        }
        EXPECT_EQ(!established, policy.AllowNewConnection(_peer));
        const size_t index = static_cast<size_t>(rdma::RdmaFailureReason::RECV_POST);
        EXPECT_EQ(before.failures[index] + established, policy.GetStatus().failures[index]);
        endpoint->_resource = nullptr;
        resource.qp = nullptr;
        socket->SetFailed();
    }
}
}  // namespace
#endif  // BRPC_WITH_RDMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_RDMA
    brpc::rdma::g_rdma_available.store(true, butil::memory_order_relaxed);
    brpc::rdma::g_skip_rdma_init = true;
#endif
    return RUN_ALL_TESTS();
}
