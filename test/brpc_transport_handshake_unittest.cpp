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

#include <gtest/gtest.h>

#include <string>

#include "brpc/transport_handshake.h"

namespace brpc {
namespace handshake {

TEST(TransportHandshakeTest, publish_fallback_after_tcp_state) {
    HandshakeSession handshake;
    int tcp_active = 0;

    handshake.SetPhase(NEGOTIATING);
    handshake.PublishFallback([&tcp_active]() { tcp_active = 1; });

    ASSERT_EQ(1, tcp_active);
    ASSERT_EQ(FALLBACK_TCP, handshake.phase());
}

TEST(TransportHandshakeTest, reset_clears_terminal_state) {
    HandshakeSession handshake;
    handshake.set_protocol_version(3);
    handshake.MarkEstablished();
    ASSERT_EQ(ESTABLISHED, handshake.phase());
    ASSERT_EQ(3, handshake.protocol_version());

    handshake.Reset(NULL);
    ASSERT_EQ(UNINITIALIZED, handshake.phase());
    ASSERT_EQ(0, handshake.protocol_version());
}

TEST(TransportHandshakeTest, client_driver_runs_common_sequence) {
    HandshakeSession session;
    std::string calls;
    bool high_speed_active = false;

    ClientHandshakeCallbacks callbacks{};
    callbacks.phases = HandshakePhases{1, 2, 3, 4, 5, 6};
    callbacks.prepare_local = [&]() {
        calls += "prepare ";
        return STEP_OK;
    };
    callbacks.send_local_hello = [&]() {
        calls += "send_hello ";
        return STEP_OK;
    };
    callbacks.receive_remote_hello = [&]() {
        calls += "recv_hello ";
        return STEP_OK;
    };
    callbacks.negotiate_resources = [&]() {
        calls += "negotiate ";
        return STEP_OK;
    };
    callbacks.send_ack = [&](bool enabled) {
        EXPECT_TRUE(enabled);
        calls += "send_ack ";
        return STEP_OK;
    };
    callbacks.set_high_speed_active = [&]() {
        high_speed_active = true;
        calls += "activate";
    };
    callbacks.set_tcp_active = []() {};
    callbacks.on_failed = []() {};

    ASSERT_EQ(STEP_OK, session.RunClient(callbacks));
    ASSERT_EQ("prepare send_hello recv_hello negotiate send_ack activate",
              calls);
    ASSERT_TRUE(high_speed_active);
    ASSERT_EQ(ESTABLISHED, session.phase());
}

TEST(TransportHandshakeTest, client_driver_falls_back_after_remote_hello) {
    HandshakeSession session;
    bool ack_enabled = true;
    bool tcp_active = false;

    ClientHandshakeCallbacks callbacks{};
    callbacks.phases = HandshakePhases{1, 2, 3, 4, 5, 6};
    callbacks.prepare_local = []() { return STEP_OK; };
    callbacks.send_local_hello = []() { return STEP_OK; };
    callbacks.receive_remote_hello = []() { return STEP_FALLBACK; };
    callbacks.negotiate_resources = []() {
        ADD_FAILURE() << "resource negotiation must be skipped";
        return STEP_ERROR;
    };
    callbacks.send_ack = [&](bool enabled) {
        ack_enabled = enabled;
        return STEP_OK;
    };
    callbacks.set_high_speed_active = []() {};
    callbacks.set_tcp_active = [&]() { tcp_active = true; };
    callbacks.on_failed = []() {};

    ASSERT_EQ(STEP_FALLBACK, session.RunClient(callbacks));
    ASSERT_FALSE(ack_enabled);
    ASSERT_TRUE(tcp_active);
    ASSERT_EQ(FALLBACK_TCP, session.phase());
}

TEST(TransportHandshakeTest, server_driver_resumes_at_ack_wait) {
    HandshakeSession session;
    int hello_calls = 0;
    int ack_calls = 0;
    bool high_speed_active = false;

    ServerHandshakeCallbacks callbacks{};
    callbacks.phases = HandshakePhases{11, 12, 13, 14, 15, 16};
    callbacks.fallback_on_not_mine = false;
    callbacks.receive_remote_hello = [&]() {
        ++hello_calls;
        return STEP_OK;
    };
    callbacks.prepare_local = []() { return STEP_OK; };
    callbacks.negotiate_resources = []() { return STEP_OK; };
    callbacks.send_local_hello = [](bool enabled) {
        EXPECT_TRUE(enabled);
        return STEP_OK;
    };
    callbacks.receive_ack = [&]() {
        ++ack_calls;
        return ack_calls == 1 ? STEP_NEED_MORE : STEP_OK;
    };
    callbacks.set_high_speed_active = [&]() {
        high_speed_active = true;
    };
    callbacks.set_tcp_active = []() {};
    callbacks.on_failed = []() {};

    ASSERT_EQ(STEP_NEED_MORE, session.RunServer(callbacks));
    ASSERT_EQ(16, session.phase());
    ASSERT_EQ(1, hello_calls);
    ASSERT_EQ(1, ack_calls);

    ASSERT_EQ(STEP_OK, session.RunServer(callbacks));
    ASSERT_EQ(1, hello_calls);
    ASSERT_EQ(2, ack_calls);
    ASSERT_TRUE(high_speed_active);
    ASSERT_EQ(ESTABLISHED, session.phase());
}

TEST(TransportHandshakeTest, server_driver_runs_blocking_handshake) {
    HandshakeSession session;
    std::string calls;

    ServerHandshakeCallbacks callbacks{};
    callbacks.phases = HandshakePhases{11, 12, 13, 14, 15, 16};
    callbacks.fallback_on_not_mine = true;
    callbacks.receive_remote_hello = [&]() {
        calls += "recv_hello ";
        return STEP_OK;
    };
    callbacks.prepare_local = [&]() {
        calls += "prepare ";
        return STEP_OK;
    };
    callbacks.negotiate_resources = [&]() {
        calls += "negotiate ";
        return STEP_OK;
    };
    callbacks.send_local_hello = [&](bool enabled) {
        EXPECT_TRUE(enabled);
        calls += "send_hello ";
        return STEP_OK;
    };
    callbacks.receive_ack = [&]() {
        calls += "recv_ack ";
        return STEP_OK;
    };
    callbacks.set_high_speed_active = [&]() { calls += "activate"; };
    callbacks.set_tcp_active = []() {};
    callbacks.on_failed = []() {};

    ASSERT_EQ(STEP_OK, session.RunServer(callbacks));
    ASSERT_EQ("recv_hello prepare negotiate send_hello recv_ack activate",
              calls);
    ASSERT_EQ(ESTABLISHED, session.phase());
}

TEST(TransportHandshakeTest, server_driver_falls_back_for_other_protocol) {
    HandshakeSession session;
    bool tcp_active = false;

    ServerHandshakeCallbacks callbacks{};
    callbacks.phases = HandshakePhases{11, 12, 13, 14, 15, 16};
    callbacks.fallback_on_not_mine = true;
    callbacks.receive_remote_hello = []() { return STEP_NOT_MINE; };
    callbacks.prepare_local = []() { return STEP_ERROR; };
    callbacks.negotiate_resources = []() { return STEP_ERROR; };
    callbacks.send_local_hello = [](bool) { return STEP_ERROR; };
    callbacks.receive_ack = []() { return STEP_ERROR; };
    callbacks.set_high_speed_active = []() {};
    callbacks.set_tcp_active = [&]() { tcp_active = true; };
    callbacks.on_failed = []() {};

    ASSERT_EQ(STEP_FALLBACK, session.RunServer(callbacks));
    ASSERT_TRUE(tcp_active);
    ASSERT_EQ(FALLBACK_TCP, session.phase());
}

}  // namespace handshake
}  // namespace brpc
