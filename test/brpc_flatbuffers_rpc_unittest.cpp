// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <atomic>
#include <cstring>
#include <string>
#include <gtest/gtest.h>
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/errno.pb.h"
#include "brpc/server.h"
#include "../example/benchmark_fb/test.brpc.fb.h"

namespace {

using brpc::flatbuffers::Message;
using brpc::flatbuffers::MessageBuilder;

class ValidatingService : public test::BenchmarkService {
public:
    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};

    void Test(google::protobuf::RpcController* controller,
              const Message* request, Message* response,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        if (request == nullptr || !request->Verify<test::BenchmarkRequest>()) {
            ++rejected;
            static_cast<brpc::Controller*>(controller)->SetFailed(
                brpc::EREQUEST, "Invalid FlatBuffers BenchmarkRequest");
            return;
        }
        const auto* root = request->GetRoot<test::BenchmarkRequest>();
        MessageBuilder builder;
        const auto text = builder.CreateString(
            root->message() ? root->message()->str() : std::string());
        const auto result = test::CreateBenchmarkResponse(
            builder, root->opcode(), root->echo_attachment(),
            root->attachment_size(), root->request_id(), root->reserved(), text);
        builder.Finish(result);
        *response = builder.ReleaseMessage();
        ++accepted;
    }
};

class FlatBuffersRpcTest : public ::testing::Test {
protected:
    void SetUp() override {
        ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, server.Start("127.0.0.1:0", nullptr));
        brpc::ChannelOptions options;
        options.protocol = "fb_rpc";
        options.timeout_ms = 3000;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
    }

    void TearDown() override {
        if (server.IsRunning()) {
            server.Stop(0);
            server.Join();
        }
    }

    Message MakeRequest() {
        MessageBuilder builder;
        const auto text = builder.CreateString("rpc round trip");
        const auto root = test::CreateBenchmarkRequest(
            builder, 7, 0, 0, 123, 9, text);
        builder.Finish(root);
        return builder.ReleaseMessage();
    }

    void CheckValidCall() {
        Message request = MakeRequest();
        Message response;
        brpc::Controller controller;
        test::BenchmarkServiceStub stub(&channel);
        stub.Test(&controller, &request, &response, nullptr);
        ASSERT_FALSE(controller.Failed()) << controller.ErrorText();
        ASSERT_TRUE(response.Verify<test::BenchmarkResponse>());
        const auto* root = response.GetRoot<test::BenchmarkResponse>();
        EXPECT_EQ(7, root->opcode());
        EXPECT_EQ(0, root->echo_attachment());
        EXPECT_EQ(0, root->attachment_size());
        EXPECT_EQ(123, root->request_id());
        EXPECT_EQ(9, root->reserved());
        ASSERT_NE(nullptr, root->message());
        EXPECT_EQ("rpc round trip", root->message()->str());
    }

    void CheckRejectedCall() {
        Message request = MakeRequest();
        ASSERT_GE(request.size(), sizeof(::flatbuffers::uoffset_t));
        std::memset(request.mutable_data(), 0xff, sizeof(::flatbuffers::uoffset_t));
        Message response;
        brpc::Controller controller;
        test::BenchmarkServiceStub stub(&channel);
        stub.Test(&controller, &request, &response, nullptr);
        ASSERT_TRUE(controller.Failed());
        // The protocol transmits the error code, not the service error text.
        EXPECT_EQ(brpc::EREQUEST, controller.ErrorCode()) << controller.ErrorText();
    }

    // Keep the non-owned service alive until the server has stopped.
    ValidatingService service;
    brpc::Server server;
    brpc::Channel channel;
};

TEST_F(FlatBuffersRpcTest, ValidRequestRoundTrip) {
    ASSERT_NO_FATAL_FAILURE(CheckValidCall());
    EXPECT_EQ(1, service.accepted.load());
    EXPECT_EQ(0, service.rejected.load());
}

TEST_F(FlatBuffersRpcTest, CorruptRequestIsRejectedByService) {
    ASSERT_NO_FATAL_FAILURE(CheckRejectedCall());
    EXPECT_EQ(0, service.accepted.load());
    EXPECT_EQ(1, service.rejected.load());
}

TEST_F(FlatBuffersRpcTest, ValidRequestSucceedsAfterRejection) {
    ASSERT_NO_FATAL_FAILURE(CheckValidCall());
    ASSERT_NO_FATAL_FAILURE(CheckRejectedCall());
    ASSERT_NO_FATAL_FAILURE(CheckValidCall());
    EXPECT_EQ(2, service.accepted.load());
    EXPECT_EQ(1, service.rejected.load());
}

}  // namespace
