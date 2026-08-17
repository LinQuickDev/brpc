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

}  // namespace handshake
}  // namespace brpc
