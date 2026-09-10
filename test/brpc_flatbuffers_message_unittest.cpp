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

#include <cstring>
#include <utility>
#include <gtest/gtest.h>
#include "brpc/details/flatbuffers_impl.h"
#include "../example/benchmark_fb/test_generated.h"

namespace {

using brpc::flatbuffers::Message;
using brpc::flatbuffers::MessageBuilder;

Message BuildRequest(const char* value) {
    MessageBuilder builder;
    const auto text = builder.CreateString(value);
    const auto root = test::CreateBenchmarkRequest(
        builder, 7, 1, 64, 123, 9, text);
    builder.Finish(root);
    return builder.ReleaseMessage();
}

TEST(FlatBuffersMessageTest, ReleasedMessageSurvivesBuilderDestruction) {
    Message message = BuildRequest("hello");
    ASSERT_TRUE(message.Verify<test::BenchmarkRequest>());
    const auto* root = message.GetRoot<test::BenchmarkRequest>();
    EXPECT_EQ(7, root->opcode());
    EXPECT_EQ(1, root->echo_attachment());
    EXPECT_EQ(64, root->attachment_size());
    EXPECT_EQ(123, root->request_id());
    EXPECT_EQ(9, root->reserved());
    ASSERT_NE(nullptr, root->message());
    EXPECT_EQ("hello", root->message()->str());
}

TEST(FlatBuffersMessageTest, MoveConstructorPreservesBuffer) {
    Message source = BuildRequest("move constructor");
    const auto* data = source.data();
    const auto size = source.size();
    Message target(std::move(source));
    EXPECT_EQ(data, target.data());
    EXPECT_EQ(size, target.size());
    EXPECT_EQ(0u, source.size());
    source.Clear();
    ASSERT_TRUE(target.Verify<test::BenchmarkRequest>());
    ASSERT_NE(nullptr, target.GetRoot<test::BenchmarkRequest>()->message());
    EXPECT_EQ("move constructor",
              target.GetRoot<test::BenchmarkRequest>()->message()->str());
}

TEST(FlatBuffersMessageTest, MoveAssignmentReplacesExistingMessage) {
    Message target = BuildRequest("old contents");
    const uint8_t* data = nullptr;
    size_t size = 0;
    {
        Message source = BuildRequest("replacement");
        data = source.data();
        size = source.size();
        target = std::move(source);
        EXPECT_EQ(0u, source.size());
    }
    EXPECT_EQ(data, target.data());
    EXPECT_EQ(size, target.size());
    ASSERT_TRUE(target.Verify<test::BenchmarkRequest>());
    ASSERT_NE(nullptr, target.GetRoot<test::BenchmarkRequest>()->message());
    EXPECT_EQ("replacement",
              target.GetRoot<test::BenchmarkRequest>()->message()->str());
}

TEST(FlatBuffersMessageTest, RejectsCorruptRootOffset) {
    Message message = BuildRequest("corrupt");
    ASSERT_TRUE(message.Verify<test::BenchmarkRequest>());
    ASSERT_GE(message.size(), sizeof(::flatbuffers::uoffset_t));
    std::memset(message.mutable_data(), 0xff,
                sizeof(::flatbuffers::uoffset_t));
    EXPECT_FALSE(message.Verify<test::BenchmarkRequest>());
}

}  // namespace
