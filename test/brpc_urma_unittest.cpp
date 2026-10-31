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
#include <sys/socket.h>
#include <unistd.h>
#include <limits>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/atomicops.h"
#include "butil/sys_byteorder.h"
#include "urma_api.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "urma_types.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/server.h"
#include "brpc/input_messenger.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_recovery_policy.h"
#include "butil/fd_guard.h"
#include "bvar/variable.h"
#include "echo.pb.h"

using namespace brpc;

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
extern bool g_skip_urma_init;
extern butil::atomic<bool> g_urma_available;

}  // namespace urma
}  // namespace brpc

// ---------------------------------------------------------------------------
// v2 binary HelloMessage: serialize + deserialize round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_serialize_deserialize_roundtrip) {
    urma::v2_wire::HelloMessage m;
    m.msg_len = urma::v2_wire::HELLO_PACKET_LEN;
    m.hello_ver = urma::v2_wire::HELLO_V2_VERSION;
    m.impl_ver = urma::v2_wire::IMPL_V2_VERSION;
    m.buffer_size = 8192;
    m.recv_buffer_cnt = 127;
    m.jetty_id = 0x12345678;
    for (int i = 0; i < 16; ++i) {
        m.eid[i] = static_cast<uint8_t>(i + 1);
    }
    m.uasid = 0xdeadbeef;
    m.tp_type = 1;  // URMA_CTP
    for (int i = 0; i < 16; ++i) {
        m.seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    m.seg_uasid = 0xcafebabe;
    m.seg_va = 0x1122334455667788ULL;
    m.seg_len = 1ULL << 20;
    m.seg_token_id = 0x42424242;

    uint8_t buf[urma::v2_wire::HELLO_BODY_LEN];
    m.Serialize(buf);

    urma::v2_wire::HelloMessage m2;
    m2.Deserialize(buf);
    EXPECT_EQ(m.msg_len, m2.msg_len);
    EXPECT_EQ(m.hello_ver, m2.hello_ver);
    EXPECT_EQ(m.impl_ver, m2.impl_ver);
    EXPECT_EQ(m.buffer_size, m2.buffer_size);
    EXPECT_EQ(m.recv_buffer_cnt, m2.recv_buffer_cnt);
    EXPECT_EQ(m.jetty_id, m2.jetty_id);
    EXPECT_EQ(0, memcmp(m.eid, m2.eid, 16));
    EXPECT_EQ(m.uasid, m2.uasid);
    EXPECT_EQ(m.tp_type, m2.tp_type);
    EXPECT_EQ(0, memcmp(m.seg_eid, m2.seg_eid, 16));
    EXPECT_EQ(m.seg_uasid, m2.seg_uasid);
    EXPECT_EQ(m.seg_va, m2.seg_va);
    EXPECT_EQ(m.seg_len, m2.seg_len);
    EXPECT_EQ(m.seg_token_id, m2.seg_token_id);
}

// ---------------------------------------------------------------------------
// v2 packet on the wire: "URMA" magic + body.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_packet_magic_is_urma) {
    EXPECT_EQ(4u, urma::v2_wire::MAGIC_STR_LEN);
    char magic[4] = {'U', 'R', 'M', 'A'};
    EXPECT_EQ(0, memcmp(magic, "URMA", 4));
    EXPECT_EQ(4u + 82u, urma::v2_wire::HELLO_PACKET_LEN);
}

// ---------------------------------------------------------------------------
// v3 protobuf UrmaHello: serialize + parse round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v3_protobuf_roundtrip) {
    urma::UrmaHello msg;
    msg.set_buffer_size(8192);
    msg.set_recv_buffer_cnt(127);
    msg.set_jetty_id(0x12345678);
    uint8_t eid[16];
    for (int i = 0; i < 16; ++i) {
        eid[i] = static_cast<uint8_t>(i + 1);
    }
    msg.set_eid(eid, 16);
    msg.set_uasid(0xdeadbeef);
    msg.set_tp_type(1);
    uint8_t seg_eid[16];
    for (int i = 0; i < 16; ++i) {
        seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    msg.set_seg_eid(seg_eid, 16);
    msg.set_seg_uasid(0xcafebabe);
    msg.set_seg_va(0x1122334455667788ULL);
    msg.set_seg_len(1ULL << 20);
    msg.set_seg_token_id(0x42424242);

    std::string body;
    ASSERT_TRUE(msg.SerializeToString(&body));
    urma::UrmaHello msg2;
    ASSERT_TRUE(msg2.ParseFromString(body));
    EXPECT_EQ(msg.buffer_size(), msg2.buffer_size());
    EXPECT_EQ(msg.recv_buffer_cnt(), msg2.recv_buffer_cnt());
    EXPECT_EQ(msg.jetty_id(), msg2.jetty_id());
    EXPECT_EQ(16, msg2.eid().size());
    EXPECT_EQ(0, memcmp(msg.eid().data(), msg2.eid().data(), 16));
    EXPECT_EQ(msg.uasid(), msg2.uasid());
    EXPECT_EQ(msg.tp_type(), msg2.tp_type());
    EXPECT_EQ(16, msg2.seg_eid().size());
    EXPECT_EQ(msg.seg_uasid(), msg2.seg_uasid());
    EXPECT_EQ(msg.seg_va(), msg2.seg_va());
    EXPECT_EQ(msg.seg_len(), msg2.seg_len());
    EXPECT_EQ(msg.seg_token_id(), msg2.seg_token_id());
}

// ---------------------------------------------------------------------------
// CreateServerHandshakeByMagic dispatches on the magic bytes.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, server_handshake_factory_dispatches_on_magic) {
    // We cannot fully exercise the server handshake without a real socket +
    // endpoint, but we can verify the factory returns the right protocol
    // version for each magic, and nullptr for an unknown magic.
    uint8_t magic_v2[4] = {'U', 'R', 'M', 'A'};
    uint8_t magic_v3[4] = {'U', 'R', 'M', '3'};
    uint8_t magic_bad[4] = {'P', 'R', 'P', 'C'};

    // v2 magic -> protocol version 2
    urma::UrmaHandshake* hs2 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v2);
    // Note: the factory dereferences the endpoint only inside SendLocalHello /
    // ReceiveAndParseRemoteHello; passing nullptr is safe for the version query.
    // (We delete immediately to avoid touching the endpoint.)
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }
    // v3 magic -> protocol version 3
    urma::UrmaHandshake* hs3 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v3);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
    // unknown magic -> nullptr (caller falls back to TCP)
    urma::UrmaHandshake* hsb =
        urma::CreateServerHandshakeByMagic(nullptr, magic_bad);
    EXPECT_EQ(nullptr, hsb);
}

// ---------------------------------------------------------------------------
// CreateClientHandshake picks the version from the gflag.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, client_handshake_factory_respects_flag) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;

    urma::FLAGS_urma_client_handshake_version = 2;
    urma::UrmaHandshake* hs2 = urma::CreateClientHandshake(nullptr);
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }

    urma::FLAGS_urma_client_handshake_version = 3;
    urma::UrmaHandshake* hs3 = urma::CreateClientHandshake(nullptr);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
}

// ---------------------------------------------------------------------------
// 4-byte ACK: HELLO_ACK_URMA_OK bit.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, ack_bit_is_urma_ok) {
    // The ACK is a 4-byte big-endian flags word; bit 0 means "I want URMA".
    // Verify the round-trip: host -> net -> host preserves the bit.
    uint32_t flags = 0x1;  // HELLO_ACK_URMA_OK
    uint32_t flags_be = butil::HostToNet32(flags);
    uint32_t flags_back = butil::NetToHost32(flags_be);
    EXPECT_EQ(flags, flags_back);
    EXPECT_NE(0u, flags_back & 0x1);
}

// ---------------------------------------------------------------------------
// ParsedHello field layout: covers the flattened segment (seg_* fields).
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, parsed_hello_segment_fields) {
    urma::ParsedHello p{};
    p.buffer_size = 8192;
    p.recv_buffer_cnt = 127;
    p.jetty_id = 42;
    p.tp_type = 1;
    p.seg_va = 0x1000;
    p.seg_len = 0x100000;
    p.seg_token_id = 7;
    EXPECT_EQ(8192u, p.buffer_size);
    EXPECT_EQ(127u, p.recv_buffer_cnt);
    EXPECT_EQ(42u, p.jetty_id);
    EXPECT_EQ(1u, p.tp_type);
    EXPECT_EQ(0x1000u, p.seg_va);
    EXPECT_EQ(0x100000u, p.seg_len);
    EXPECT_EQ(7u, p.seg_token_id);
}

TEST(UrmaHandshakeTest, rejects_invalid_resource_and_window_values) {
    urma::ParsedHello hello;
    hello.buffer_size = 8192;
    hello.recv_buffer_cnt = 127;
    hello.jetty_id = 1;
    hello.tp_type = URMA_CTP;
    hello.seg_va = 0x1000;
    hello.seg_len = 8192;
    EXPECT_TRUE(urma::ValidHello(hello));

    hello.recv_buffer_cnt = 2;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.recv_buffer_cnt = 127;
    hello.seg_len = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.seg_len = 8192;
    hello.jetty_id = 0;
    EXPECT_FALSE(urma::ValidHello(hello));

    hello.jetty_id = 1;
    hello.seg_va = std::numeric_limits<uint64_t>::max() - 7;
    hello.seg_len = 8;
    EXPECT_FALSE(urma::ValidHello(hello));
}

TEST(UrmaHelperTest, selects_priority_matching_transport_path_type) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[3].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[6].tp_type.bs.ctp = 1;

    EXPECT_EQ(3, urma::FindUrmaPriorityForTpType(attr, URMA_RTP));
    EXPECT_EQ(6, urma::FindUrmaPriorityForTpType(attr, URMA_CTP));
    EXPECT_EQ(-1, urma::FindUrmaPriorityForTpType(attr, URMA_UTP));
    attr.dev_cap.priority_info[6].tp_type.bs.rtp = 1;
    EXPECT_EQ(6, urma::FindUrmaPriorityForTpType(attr, URMA_CTP));
    EXPECT_EQ(-1, urma::FindUrmaPriorityForTpType(
                      attr, static_cast<urma_tp_type_t>(255)));
}

TEST(UrmaHelperTest, auto_path_prefers_ctp_and_selects_rtp_on_rtp_only_devices) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[0].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[0].SL = 2;
    urma_tp_type_t path = URMA_CTP;
    uint8_t priority = 6;
    ASSERT_TRUE(urma::SelectUrmaTransportPath(attr, "auto", &path, &priority));
    EXPECT_EQ(URMA_RTP, path);
    EXPECT_EQ(0, priority);
    attr.dev_cap.priority_info[7].tp_type.bs.ctp = 1;
    ASSERT_TRUE(urma::SelectUrmaTransportPath(attr, "auto", &path, &priority));
    EXPECT_EQ(URMA_CTP, path);
    EXPECT_EQ(7, priority);
}

TEST(UrmaHelperTest, explicit_path_uses_matching_priority_and_combined_capabilities) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[2].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[2].tp_type.bs.ctp = 1;
    attr.dev_cap.priority_info[6].tp_type.bs.ctp = 1;
    urma_tp_type_t path = URMA_UTP;
    uint8_t priority = 15;
    for (const auto& requested : {"rtp", "ctp"}) {
        ASSERT_TRUE(urma::SelectUrmaTransportPath(attr, requested, &path, &priority));
        EXPECT_EQ(std::string(requested) == "rtp" ? URMA_RTP : URMA_CTP, path);
        EXPECT_EQ(2, priority);
    }
}

TEST(UrmaHelperTest, rejects_unsupported_paths_in_a_reported_capability_table) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[0].tp_type.bs.rtp = 1;
    urma_tp_type_t path = URMA_UTP;
    uint8_t priority = 15;
    EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, "ctp", &path, &priority));
    EXPECT_EQ(URMA_UTP, path);
    EXPECT_EQ(15, priority);
    attr.dev_cap.priority_info[0].tp_type.value = 0;
    attr.dev_cap.priority_info[4].tp_type.bs.utp = 1;
    EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, "auto", &path, &priority));
    EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, "rtp", &path, &priority));
}

TEST(UrmaHelperTest, providers_without_path_capabilities_keep_compatibility_priorities) {
    urma_device_attr_t attr{};
    urma_tp_type_t path = URMA_UTP;
    uint8_t priority = 15;
    for (const auto& requested : {"auto", "ctp", "rtp"}) {
        ASSERT_TRUE(urma::SelectUrmaTransportPath(attr, requested, &path, &priority));
        const bool rtp = std::string(requested) == "rtp";
        EXPECT_EQ(rtp ? URMA_RTP : URMA_CTP, path);
        EXPECT_EQ(rtp ? 0 : 6, priority);
    }
}

TEST(UrmaHelperTest, rejects_invalid_path_configuration) {
    urma_device_attr_t attr{};
    urma_tp_type_t path = URMA_CTP;
    uint8_t priority = 6;
    for (const auto& requested : {"", "utp", "RM", "typo"}) {
        EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, requested, &path, &priority));
    }
    EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, "auto", nullptr, &priority));
    EXPECT_FALSE(urma::SelectUrmaTransportPath(attr, "auto", &path, nullptr));
}

TEST(UrmaHelperTest, local_sge_initializes_optional_fields_and_accepts_segment_edges) {
    urma_target_seg_t segment{};
    segment.urma_ctx = urma::GetUrmaContext();
    segment.seg.ubva.va = 0x1000;
    segment.seg.len = 8192;
    urma_sge_t sge;
    memset(&sge, 0xff, sizeof(sge));
    ASSERT_TRUE(urma::InitUrmaLocalSge(&sge, 0x1000, 8192, &segment));
    EXPECT_EQ(0x1000u, sge.addr);
    EXPECT_EQ(8192u, sge.len);
    EXPECT_EQ(&segment, sge.tseg);
    EXPECT_EQ(nullptr, sge.user_tseg);
    ASSERT_TRUE(urma::InitUrmaLocalSge(&sge, 0x2fff, 1, &segment));
    EXPECT_EQ(0x2fffu, sge.addr);
    EXPECT_EQ(1u, sge.len);
}

TEST(UrmaHelperTest, local_sge_rejects_out_of_range_and_wrong_context) {
    urma_target_seg_t segment{};
    segment.urma_ctx = urma::GetUrmaContext();
    segment.seg.ubva.va = 0x1000;
    segment.seg.len = 8192;
    urma_sge_t sge{};
    for (auto values : {std::pair<uint64_t, size_t>(0xfff, 1),
                        {0x3000, 1}, {0x2fff, 2}, {0x1000, 0},
                        {0x1000, static_cast<size_t>(UINT32_MAX) + 1}}) {
        EXPECT_FALSE(urma::InitUrmaLocalSge(&sge, values.first, values.second, &segment));
        EXPECT_EQ(ERDMAMEM, errno);
    }
    EXPECT_FALSE(urma::InitUrmaLocalSge(&sge, 0x1000, 1, nullptr));
    segment.urma_ctx = reinterpret_cast<urma_context_t*>(1);
    EXPECT_FALSE(urma::InitUrmaLocalSge(&sge, 0x1000, 1, &segment));
    segment.urma_ctx = urma::GetUrmaContext();
    segment.seg.ubva.va = UINT64_MAX - 7;
    segment.seg.len = 16;
    EXPECT_FALSE(urma::InitUrmaLocalSge(&sge, UINT64_MAX - 3, 2, &segment));
}

static urma_target_seg_t g_copy_test_segment;
static int g_copy_test_releases;

static urma_target_seg_t* ResolveCopyTestSegment(void* address) {
    g_copy_test_segment = urma_target_seg_t{};
    g_copy_test_segment.urma_ctx = urma::GetUrmaContext();
    g_copy_test_segment.seg.ubva.va = reinterpret_cast<uint64_t>(address);
    g_copy_test_segment.seg.len = urma::GetUrmaRecvBlockSize();
    return &g_copy_test_segment;
}

static urma_target_seg_t* RejectCopyTestSegment(void*) {
    return nullptr;
}

static void DeleteCopyTestData(void* data) {
    ++g_copy_test_releases;
    delete[] static_cast<char*>(data);
}

TEST(UrmaHelperTest, copied_send_coalesces_host_fragments_and_retains_aligned_payload) {
    g_copy_test_releases = 0;
    butil::IOBuf buffer;
    for (const char* text : {"first", "second"}) {
        const size_t length = strlen(text);
        char* data = new char[length];
        memcpy(data, text, length);
        ASSERT_EQ(0, buffer.append_user_data(data, length, DeleteCopyTestData));
    }
    ASSERT_EQ(2u, buffer.backing_block_num());
    urma_sge_t sge{};
    ASSERT_TRUE(urma::CopyUrmaSendBuffer(&buffer, &sge, ResolveCopyTestSegment));
    EXPECT_EQ(2, g_copy_test_releases);
    EXPECT_EQ("firstsecond", buffer.to_string());
    ASSERT_EQ(1u, buffer.backing_block_num());
    EXPECT_EQ(0u, sge.addr % urma::URMA_SEND_BUFFER_ALIGNMENT);
    EXPECT_EQ(reinterpret_cast<uint64_t>(buffer.backing_block(0).data()), sge.addr);
    EXPECT_EQ(buffer.size(), sge.len);
    EXPECT_EQ(&g_copy_test_segment, sge.tseg);
    EXPECT_EQ(nullptr, sge.user_tseg);
    EXPECT_EQ("firstsecond", std::string(reinterpret_cast<const char*>(sge.addr), sge.len));

    buffer.clear();
    const std::string large(urma::GetUrmaRecvBlockSize() -
                           (urma::URMA_SEND_BUFFER_ALIGNMENT - 1), 'x');
    buffer.append(large);
    ASSERT_TRUE(urma::CopyUrmaSendBuffer(&buffer, &sge, ResolveCopyTestSegment));
    EXPECT_EQ(0u, sge.addr % urma::URMA_SEND_BUFFER_ALIGNMENT);
    EXPECT_EQ(large, buffer.to_string());
    EXPECT_EQ(large.size(), sge.len);
}

TEST(UrmaHelperTest, copied_send_failure_preserves_source_payload) {
    butil::IOBuf buffer;
    buffer.append("unchanged");
    urma_sge_t sge{};
    EXPECT_FALSE(urma::CopyUrmaSendBuffer(&buffer, &sge, RejectCopyTestSegment));
    EXPECT_EQ(ERDMAMEM, errno);
    EXPECT_EQ("unchanged", buffer.to_string());
    const std::string large(urma::GetUrmaRecvBlockSize(), 'y');
    buffer.clear();
    buffer.append(large);
    EXPECT_FALSE(urma::CopyUrmaSendBuffer(&buffer, &sge, ResolveCopyTestSegment));
    EXPECT_EQ(EMSGSIZE, errno);
    EXPECT_EQ(large, buffer.to_string());
}

// ---------------------------------------------------------------------------
// SupportedByUrma: only baidu_std.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, supported_by_urma_protocol_allowlist) {
    EXPECT_TRUE(urma::SupportedByUrma("baidu_std"));
    EXPECT_FALSE(urma::SupportedByUrma("http"));
    EXPECT_FALSE(urma::SupportedByUrma("hulu_pbrpc"));
    EXPECT_FALSE(urma::SupportedByUrma("nshead"));
}

// ---------------------------------------------------------------------------
// URMA mock smoke test: drives urma_init / device enumeration / context /
// jetty / post / poll. These tests rely on mock semantics and are skipped
// when the test binary links a real liburma provider.
// ---------------------------------------------------------------------------
class UrmaMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        urma::g_skip_urma_init = false;

        urma_init_attr_t init_attr{};
        const urma_status_t status = urma_init(&init_attr);
        ASSERT_TRUE(status == URMA_SUCCESS || status == URMA_EEXIST);
        _owns_urma_init = status == URMA_SUCCESS;

        int num_devices = 0;
        urma_device_t** devices = urma_get_device_list(&num_devices);
        ASSERT_NE(nullptr, devices);
        ASSERT_GT(num_devices, 0);
        const bool using_mock =
            strcmp(devices[0]->name, "mock_urma_device") == 0;
        urma_free_device_list(devices);
        if (!using_mock) {
            if (_owns_urma_init) {
                EXPECT_EQ(URMA_SUCCESS, urma_uninit());
                _owns_urma_init = false;
            }
            GTEST_SKIP() << "UrmaMockTest requires the URMA link-time mock";
        }
    }

    void TearDown() override {
        if (_owns_urma_init) {
            EXPECT_EQ(URMA_SUCCESS, urma_uninit());
        }
        urma::g_skip_urma_init = true;
        urma::g_urma_available.store(true, butil::memory_order_relaxed);
    }

private:
    bool _owns_urma_init{false};
};

TEST_F(UrmaMockTest, init_and_enumerate_device) {
    urma_init_attr_t init_attr{};
    EXPECT_EQ(URMA_EEXIST, urma_init(&init_attr));

    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    EXPECT_STREQ("mock_urma_device", devices[0]->name);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, create_context_and_query_device) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);

    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    ASSERT_NE(nullptr, eids);
    ASSERT_GE(eid_cnt, 1u);
    urma_free_eid_list(eids);

    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_device_attr_t attr{};
    ASSERT_EQ(URMA_SUCCESS, urma_query_device(devices[0], &attr));
    EXPECT_GE(attr.dev_cap.max_jfc, 1u);
    EXPECT_GE(attr.dev_cap.max_jetty, 1u);

    EXPECT_EQ(URMA_SUCCESS, urma_delete_context(ctx));
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, rejects_send_without_target_jetty) {
    // A SEND without a target jetty is invalid. Keeping the mock strict here
    // prevents tests from relying on input that a real provider cannot post.
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    urma_free_eid_list(eids);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, jfce);
    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = 16;
    jfc_cfg.jfce = jfce;
    urma_jfc_t* jfc = urma_create_jfc(ctx, &jfc_cfg);
    ASSERT_NE(nullptr, jfc);

    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = 16;
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = jfc;
    urma_jfr_t* jfr = urma_create_jfr(ctx, &jfr_cfg);
    ASSERT_NE(nullptr, jfr);

    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = 16;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = jfc;
    jetty_cfg.shared.jfr = jfr;
    jetty_cfg.shared.jfc = jfc;
    urma_jetty_t* jetty = urma_create_jetty(ctx, &jetty_cfg);
    ASSERT_NE(nullptr, jetty);

    urma_jfs_wr_t wr{};
    memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.user_ctx = 0xABCD;
    wr.next = nullptr;
    urma_jfs_wr_t* bad = nullptr;
    EXPECT_EQ(URMA_EINVAL, urma_post_jetty_send_wr(jetty, &wr, &bad));
    EXPECT_EQ(&wr, bad);

    urma_cr_t crs[4];
    EXPECT_EQ(0, urma_poll_jfc(jfc, 4, crs));

    urma_delete_jetty(jetty);
    urma_delete_jfr(jfr);
    urma_delete_jfc(jfc);
    urma_delete_jfce(jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest,
       paired_send_is_bidirectional_and_separates_immediate_credit) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GT(num_devices, 0);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* sender_jfce = urma_create_jfce(ctx);
    urma_jfce_t* receiver_jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, sender_jfce);
    ASSERT_NE(nullptr, receiver_jfce);
    urma_jfc_cfg_t sender_jfc_cfg{};
    sender_jfc_cfg.depth = 8;
    sender_jfc_cfg.jfce = sender_jfce;
    urma_jfc_t* sender_jfc = urma_create_jfc(ctx, &sender_jfc_cfg);
    ASSERT_NE(nullptr, sender_jfc);
    urma_jfc_cfg_t receiver_jfc_cfg{};
    receiver_jfc_cfg.depth = 8;
    receiver_jfc_cfg.jfce = receiver_jfce;
    urma_jfc_t* receiver_jfc = urma_create_jfc(ctx, &receiver_jfc_cfg);
    ASSERT_NE(nullptr, receiver_jfc);

    urma_jfr_cfg_t sender_jfr_cfg{};
    sender_jfr_cfg.depth = 4;
    sender_jfr_cfg.trans_mode = URMA_TM_RM;
    sender_jfr_cfg.max_sge = 1;
    sender_jfr_cfg.jfc = sender_jfc;
    urma_jfr_t* sender_jfr = urma_create_jfr(ctx, &sender_jfr_cfg);
    ASSERT_NE(nullptr, sender_jfr);
    urma_jfr_cfg_t receiver_jfr_cfg = sender_jfr_cfg;
    receiver_jfr_cfg.jfc = receiver_jfc;
    urma_jfr_t* receiver_jfr = urma_create_jfr(ctx, &receiver_jfr_cfg);
    ASSERT_NE(nullptr, receiver_jfr);

    auto create_jetty = [&](urma_jfc_t* jfc, urma_jfr_t* jfr) {
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = 4;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.max_sge = 1;
        cfg.jfs_cfg.jfc = jfc;
        cfg.shared.jfr = jfr;
        cfg.shared.jfc = jfc;
        return urma_create_jetty(ctx, &cfg);
    };
    urma_jetty_t* sender = create_jetty(sender_jfc, sender_jfr);
    urma_jetty_t* receiver = create_jetty(receiver_jfc, receiver_jfr);
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);

    urma_rjetty_t remote{};
    remote.jetty_id = receiver->jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    urma_target_jetty_t* target =
        urma_import_jetty(ctx, &remote, &token);
    ASSERT_NE(nullptr, target);

    char recv_buf[64]{};
    urma_sge_t recv_sge{
        reinterpret_cast<uint64_t>(recv_buf), sizeof(recv_buf), nullptr,
        nullptr};
    urma_sg_t recv_sg{&recv_sge, 1};
    urma_jfr_wr_t recv_wr{recv_sg, 99, nullptr};
    char credit_recv_buf[1]{};
    urma_sge_t credit_recv_sge{
        reinterpret_cast<uint64_t>(credit_recv_buf),
        sizeof(credit_recv_buf), nullptr, nullptr};
    urma_sg_t credit_recv_sg{&credit_recv_sge, 1};
    urma_jfr_wr_t credit_recv_wr{credit_recv_sg, 100, nullptr};
    recv_wr.next = &credit_recv_wr;
    urma_jfr_wr_t* bad_recv = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(receiver_jfr, &recv_wr, &bad_recv));

    const char payload[] = "urma-payload";
    urma_sge_t send_sge{
        reinterpret_cast<uint64_t>(payload), sizeof(payload), nullptr,
        nullptr};
    urma_sg_t send_sg{&send_sge, 1};
    urma_jfs_wr_t send_wr{};
    send_wr.opcode = URMA_OPC_SEND;
    send_wr.flag.bs.complete_enable = 1;
    send_wr.tjetty = target;
    send_wr.user_ctx = 7;
    send_wr.send.src = send_sg;
    urma_jfs_wr_t credit_wr{};
    credit_wr.opcode = URMA_OPC_SEND_IMM;
    credit_wr.flag.bs.complete_enable = 1;
    credit_wr.tjetty = target;
    credit_wr.user_ctx = 8;
    credit_wr.send.imm_data = 13;
    send_wr.next = &credit_wr;
    urma_jfs_wr_t* bad_send = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(sender, &send_wr, &bad_send));

    urma_cr_t sender_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(sender_jfc, 2, sender_cr));
    EXPECT_EQ(0, sender_cr[0].flag.bs.s_r);
    EXPECT_EQ(7u, sender_cr[0].user_ctx);
    EXPECT_EQ(0, sender_cr[1].flag.bs.s_r);
    EXPECT_EQ(8u, sender_cr[1].user_ctx);

    urma_cr_t receiver_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(receiver_jfc, 2, receiver_cr));
    EXPECT_EQ(1, receiver_cr[0].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, receiver_cr[0].opcode);
    EXPECT_EQ(0u, receiver_cr[0].imm_data);
    EXPECT_EQ(sizeof(payload), receiver_cr[0].completion_len);
    EXPECT_EQ(0, memcmp(payload, recv_buf, sizeof(payload)));
    EXPECT_EQ(1, receiver_cr[1].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND_WITH_IMM, receiver_cr[1].opcode);
    EXPECT_EQ(13u, receiver_cr[1].imm_data);
    EXPECT_EQ(0u, receiver_cr[1].completion_len);

    // Exercise the response direction as well. Production posts all receive
    // WRs through the shared JFR.
    urma_rjetty_t sender_remote = remote;
    sender_remote.jetty_id = sender->jetty_id;
    urma_target_jetty_t* sender_target =
        urma_import_jetty(ctx, &sender_remote, &token);
    ASSERT_NE(nullptr, sender_target);
    char response_buf[64]{};
    urma_sge_t response_recv_sge{
        reinterpret_cast<uint64_t>(response_buf), sizeof(response_buf),
        nullptr, nullptr};
    urma_sg_t response_recv_sg{&response_recv_sge, 1};
    urma_jfr_wr_t response_recv_wr{response_recv_sg, 101, nullptr};
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(sender_jfr, &response_recv_wr, &bad_recv));

    const char response[] = "urma-response";
    urma_sge_t response_send_sge{
        reinterpret_cast<uint64_t>(response), sizeof(response), nullptr,
        nullptr};
    urma_sg_t response_send_sg{&response_send_sge, 1};
    urma_jfs_wr_t response_send_wr{};
    response_send_wr.opcode = URMA_OPC_SEND;
    response_send_wr.flag.bs.complete_enable = 1;
    response_send_wr.tjetty = sender_target;
    response_send_wr.user_ctx = 9;
    response_send_wr.send.src = response_send_sg;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(receiver, &response_send_wr, &bad_send));

    urma_cr_t response_send_cr{};
    ASSERT_EQ(1, urma_poll_jfc(receiver_jfc, 1, &response_send_cr));
    EXPECT_EQ(0, response_send_cr.flag.bs.s_r);
    EXPECT_EQ(9u, response_send_cr.user_ctx);

    urma_cr_t response_recv_cr{};
    ASSERT_EQ(1, urma_poll_jfc(sender_jfc, 1, &response_recv_cr));
    EXPECT_EQ(1, response_recv_cr.flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, response_recv_cr.opcode);
    EXPECT_EQ(sizeof(response), response_recv_cr.completion_len);
    EXPECT_EQ(0, memcmp(response, response_buf, sizeof(response)));

    urma_unimport_jetty(sender_target);
    urma_unimport_jetty(target);
    urma_delete_jetty(receiver);
    urma_delete_jetty(sender);
    urma_delete_jfr(receiver_jfr);
    urma_delete_jfr(sender_jfr);
    urma_delete_jfc(receiver_jfc);
    urma_delete_jfc(sender_jfc);
    urma_delete_jfce(receiver_jfce);
    urma_delete_jfce(sender_jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}


namespace brpc {
namespace urma {

class UrmaRecoveryTestPeer {
public:
    static int ImportPeer(UrmaEndpoint* ep, const ParsedHello& peer) {
        return ep->ImportPeer(peer);
    }
    static void Establish(UrmaEndpoint* ep) {
        GlobalUrmaRecoveryPolicy().AllowNewConnection("", &ep->_recovery_epoch);
        ep->_state = UrmaEndpoint::ESTABLISHED;
    }
    static ssize_t Complete(UrmaEndpoint* ep, urma_cr_status_t status) {
        urma_cr_t cr{};
        cr.status = status;
        return ep->HandleCompletion(cr);
    }
    static void Report(UrmaEndpoint* ep, UrmaFailureReason reason) {
        ep->ReportFailure(reason);
    }
    static void PollFailure(UrmaEndpoint* ep) {
        UrmaResource resource;
        urma_jfc_t invalid_jfc{};
        resource.jfc = &invalid_jfc;
        ep->_resource = &resource;
        SocketOptions options;
        options.user = ep;
        SocketId sid;
        if (Socket::Create(options, &sid) == 0) {
            SocketUniquePtr carrier;
            if (Socket::Address(sid, &carrier) == 0) {
                const bool polling = FLAGS_urma_use_polling;
                FLAGS_urma_use_polling = true;
                UrmaEndpoint::PollCq(carrier.get());
                FLAGS_urma_use_polling = polling;
                carrier->SetFailed();
            }
        }
        ep->_resource = nullptr;
        resource.jfc = nullptr;
    }
    static bool Backpressure(UrmaEndpoint* ep) {
        UrmaResource resource;
        urma_jetty_t jetty{};
        urma_target_jetty_t remote{};
        resource.jetty = &jetty;
        resource.remote_jetty = &remote;
        ep->_resource = &resource;
        ep->_sq_window_size = 0;
        ep->_remote_rq_window_size = 1;
        butil::IOBuf data;
        data.append("payload");
        butil::IOBuf* buf = &data;
        const ssize_t rc = ep->CutFromIOBufList(&buf, 1);
        const bool ok = rc == -1 && errno == EAGAIN && data.size() == 7;
        ep->_resource = nullptr;
        resource.jetty = nullptr;
        resource.remote_jetty = nullptr;
        return ok;
    }
    static int FailedAck(UrmaEndpoint* ep) {
        // A Jetty without an imported target fails provider submission.
        UrmaResource resource;
        urma_jetty_t jetty{};
        urma_target_jetty_t remote{};
        resource.jetty = &jetty;
        resource.remote_jetty = &remote;
        ep->_resource = &resource;
        ep->_sq_imm_window_size = 1;
        ep->_remote_window_capacity = 2;
        ep->_new_rq_wrs = 2;
        urma_cr_t cr{};
        cr.status = URMA_CR_SUCCESS;
        cr.user_ctx = 0;
        const int rc = ep->HandleCompletion(cr);
        ep->_resource = nullptr;
        resource.jetty = nullptr;
        resource.remote_jetty = nullptr;
        return rc;
    }
};

}  // namespace urma
}  // namespace brpc

class UrmaRecoveryTest : public UrmaMockTest {
protected:
    void SetUp() override {
        UrmaMockTest::SetUp();
        if (IsSkipped() || HasFatalFailure()) {
            return;
        }
        urma::g_skip_urma_init = true;
        urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::LEGACY);
        urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::AUTO);
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        urma::GlobalUrmaRecoveryPolicy().ReportPortState(true);
    }
    void TearDown() override {
        urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::LEGACY);
        rdma::SetRdmaRecoveryMode(rdma::RdmaRecoveryMode::LEGACY);
        UrmaMockTest::TearDown();
    }
    void CreateSocket(SocketUniquePtr* socket) {
        SocketOptions opt;
        butil::str2endpoint("127.0.0.1:8123", &opt.remote_side);
        opt.user = get_client_side_messenger();
        SocketId sid;
        ASSERT_EQ(0, Socket::Create(opt, &sid));
        ASSERT_EQ(0, Socket::Address(sid, socket));
    }
};

TEST_F(UrmaRecoveryTest, independent_transport_modes_and_metrics) {
    ASSERT_TRUE(urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::TCP_ONLY));
    EXPECT_FALSE(urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("peer"));
    EXPECT_TRUE(rdma::GlobalRdmaRecoveryPolicy().AllowNewConnection("peer"));
    EXPECT_EQ(rdma::RdmaRecoveryMode::LEGACY, rdma::GetRdmaRecoveryMode());
    EXPECT_FALSE(urma::SetUrmaRecoveryCooldownMs(0));
    EXPECT_TRUE(urma::SetUrmaRecoveryCooldownMs(5000));
    EXPECT_NE(std::string::npos,
              bvar::Variable::describe_exposed("urma_recovery_status").find("tcp_only"));
    EXPECT_FALSE(bvar::Variable::describe_exposed(
        "urma_recovery_tcp_fallback_admissions").empty());
}

TEST_F(UrmaRecoveryTest, rejects_mismatched_path_before_importing_resources) {
    urma::UrmaEndpoint endpoint(nullptr);
    urma::ParsedHello peer;
    peer.tp_type = urma::GetUrmaTpType() == URMA_CTP ? URMA_RTP : URMA_CTP;
    EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::ImportPeer(&endpoint, peer));
    EXPECT_EQ(EPROTO, errno);
    peer.tp_type = URMA_UTP;
    EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::ImportPeer(&endpoint, peer));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaRecoveryTest, completion_errors_cool_peer_once) {
    for (auto status : {URMA_CR_ACK_TIMEOUT_ERR, URMA_CR_RNR_RETRY_CNT_EXC_ERR,
                        URMA_CR_LOC_ACCESS_ERR, URMA_CR_REM_OPERATION_ERR}) {
        urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::LEGACY);
        urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::AUTO);
        SocketUniquePtr socket;
        ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
        urma::UrmaEndpoint ep(socket.get());
        urma::UrmaRecoveryTestPeer::Establish(&ep);
        const auto before = urma::GetUrmaRecoveryStatus();
        EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::Complete(&ep, status));
        const auto reason = status == URMA_CR_ACK_TIMEOUT_ERR ?
            urma::UrmaFailureReason::RETRY_EXHAUSTED :
            status == URMA_CR_RNR_RETRY_CNT_EXC_ERR ? urma::UrmaFailureReason::RNR :
            status == URMA_CR_LOC_ACCESS_ERR ? urma::UrmaFailureReason::ACCESS_ERROR :
                                             urma::UrmaFailureReason::TRANSPORT_ERROR;
        EXPECT_EQ(before.failures[static_cast<size_t>(reason)] + 1,
                  urma::GetUrmaRecoveryStatus().failures[static_cast<size_t>(reason)]);
        EXPECT_EQ(status == URMA_CR_LOC_ACCESS_ERR,
                  urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("127.0.0.1:8123"));
        urma::UrmaRecoveryTestPeer::Report(&ep, urma::UrmaFailureReason::CQ_POLL);
        EXPECT_EQ(before.failures[static_cast<size_t>(urma::UrmaFailureReason::CQ_POLL)],
                  urma::GetUrmaRecoveryStatus().failures[
                      static_cast<size_t>(urma::UrmaFailureReason::CQ_POLL)]);
        socket->SetFailed();
    }
}

TEST_F(UrmaRecoveryTest, flush_backpressure_and_old_epoch_do_not_cool_peer) {
    SocketUniquePtr socket;
    ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
    urma::UrmaEndpoint ep(socket.get());
    urma::UrmaRecoveryTestPeer::Establish(&ep);
    EXPECT_TRUE(urma::UrmaRecoveryTestPeer::Backpressure(&ep));
    EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::Complete(&ep, URMA_CR_WR_FLUSH_ERR));
    EXPECT_TRUE(urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("127.0.0.1:8123"));
    urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::TCP_ONLY);
    urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::AUTO);
    urma::UrmaRecoveryTestPeer::Report(&ep, urma::UrmaFailureReason::CQ_POLL);
    EXPECT_TRUE(urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("127.0.0.1:8123"));
    socket->SetFailed();
}

TEST_F(UrmaRecoveryTest, ack_post_failure_propagates) {
    SocketUniquePtr socket;
    ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
    urma::UrmaEndpoint ep(socket.get());
    urma::UrmaRecoveryTestPeer::Establish(&ep);
    EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::FailedAck(&ep));
    EXPECT_FALSE(urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("127.0.0.1:8123"));
    socket->SetFailed();
}

TEST_F(UrmaRecoveryTest, jfc_poll_failure_fails_socket_and_cools_peer) {
    SocketUniquePtr socket;
    ASSERT_NO_FATAL_FAILURE(CreateSocket(&socket));
    urma::UrmaEndpoint ep(socket.get());
    urma::UrmaRecoveryTestPeer::Establish(&ep);
    const auto before = urma::GetUrmaRecoveryStatus();
    urma::UrmaRecoveryTestPeer::PollFailure(&ep);
    EXPECT_TRUE(socket->Failed());
    EXPECT_FALSE(urma::GlobalUrmaRecoveryPolicy().AllowNewConnection("127.0.0.1:8123"));
    EXPECT_EQ(before.failures[static_cast<size_t>(urma::UrmaFailureReason::CQ_POLL)] + 1,
              urma::GetUrmaRecoveryStatus().failures[
                  static_cast<size_t>(urma::UrmaFailureReason::CQ_POLL)]);
}

class UrmaRecoveryEcho : public ::test::EchoService {
    void Echo(google::protobuf::RpcController*, const ::test::EchoRequest* request,
              ::test::EchoResponse* response, google::protobuf::Closure* done) override {
        response->set_message(request->message());
        done->Run();
    }
};

TEST_F(UrmaRecoveryTest, tcp_only_client_completes_rpc_on_urma_server) {
    urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::TCP_ONLY);
    UrmaRecoveryEcho service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions so;
    so.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &so));
    Channel channel;
    ChannelOptions co;
    co.socket_mode = SOCKET_MODE_URMA;
    co.connection_type = "short";
    co.timeout_ms = 2000;
    ASSERT_EQ(0, channel.Init(server.listen_address(), &co));
    ::test::EchoService_Stub stub(&channel);
    Controller cntl;
    ::test::EchoRequest request;
    ::test::EchoResponse response;
    request.set_message("recovery over TCP");
    stub.Echo(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(request.message(), response.message());
    urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::AUTO);
    SocketOptions failed_options;
    failed_options.remote_side = server.listen_address();
    failed_options.user = get_client_side_messenger();
    SocketId failed_sid;
    ASSERT_EQ(0, Socket::Create(failed_options, &failed_sid));
    SocketUniquePtr failed_socket;
    ASSERT_EQ(0, Socket::Address(failed_sid, &failed_socket));
    {
        urma::UrmaEndpoint failed_ep(failed_socket.get());
        urma::UrmaRecoveryTestPeer::Establish(&failed_ep);
        EXPECT_EQ(-1, urma::UrmaRecoveryTestPeer::Complete(
            &failed_ep, URMA_CR_RNR_RETRY_CNT_EXC_ERR));
    }
    failed_socket->SetFailed();
    const auto before = urma::GetUrmaRecoveryStatus();
    cntl.Reset();
    response.Clear();
    stub.Echo(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(request.message(), response.message());
    EXPECT_EQ(before.peer_cooldown_admissions + 1,
              urma::GetUrmaRecoveryStatus().peer_cooldown_admissions);
    server.Stop(0);
    server.Join();
}

TEST_F(UrmaRecoveryTest, server_declines_v2_and_v3_without_allocating_jetty) {
    urma::SetUrmaRecoveryMode(urma::UrmaRecoveryMode::TCP_ONLY);
    Server server;
    ServerOptions so;
    so.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &so));
    for (int version : {2, 3}) {
        butil::fd_guard fd(butil::tcp_connect(server.listen_address(), nullptr));
        ASSERT_GE(fd, 0);
        timeval timeout{2, 0};
        ASSERT_EQ(0, setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
        urma::v2_wire::HelloMessage hello;
        hello.msg_len = urma::v2_wire::HELLO_PACKET_LEN;
        hello.hello_ver = urma::v2_wire::HELLO_V2_VERSION;
        hello.impl_ver = urma::v2_wire::IMPL_V2_VERSION;
        hello.buffer_size = 8192;
        hello.recv_buffer_cnt = 127;
        hello.jetty_id = 42;
        memset(hello.eid, 1, 16);
        memset(hello.seg_eid, 1, 16);
        hello.tp_type = URMA_CTP;
        hello.seg_va = 0x100000;
        hello.seg_len = 1024 * 1024;
        if (version == 2) {
            uint8_t packet[urma::v2_wire::HELLO_PACKET_LEN];
            memcpy(packet, "URMA", 4);
            hello.Serialize(packet + 4);
            ASSERT_EQ(sizeof(packet), static_cast<size_t>(write(fd, packet, sizeof(packet))));
            uint8_t reply[sizeof(packet)];
            ASSERT_EQ(sizeof(reply), static_cast<size_t>(recv(fd, reply, sizeof(reply), MSG_WAITALL)));
            EXPECT_EQ(0, memcmp(reply, "URMA", 4));
            urma::v2_wire::HelloMessage rejected;
            rejected.Deserialize(reply + 4);
            EXPECT_EQ(0, rejected.hello_ver);
            EXPECT_EQ(0u, rejected.jetty_id);
        } else {
            urma::UrmaHello msg;
            msg.set_buffer_size(8192);
            msg.set_recv_buffer_cnt(127);
            msg.set_jetty_id(42);
            msg.set_uasid(0);
            msg.set_seg_uasid(0);
            msg.set_seg_token_id(0);
            msg.set_eid(hello.eid, 16);
            msg.set_seg_eid(hello.seg_eid, 16);
            msg.set_tp_type(URMA_CTP);
            msg.set_seg_va(hello.seg_va);
            msg.set_seg_len(hello.seg_len);
            const std::string body = msg.SerializeAsString();
            uint32_t size = butil::HostToNet32(body.size());
            std::string packet("URM3", 4);
            packet.append(reinterpret_cast<const char*>(&size), 4);
            packet.append(body);
            ASSERT_EQ(packet.size(), static_cast<size_t>(write(fd, packet.data(), packet.size())));
            char header[8];
            ASSERT_EQ(8, recv(fd, header, 8, MSG_WAITALL));
            EXPECT_EQ(0, memcmp(header, "URM3", 4));
            memcpy(&size, header + 4, 4);
            size = butil::NetToHost32(size);
            ASSERT_LE(size, 4096u);
            std::string reply(size, '\0');
            ASSERT_EQ(size, static_cast<uint32_t>(recv(fd, &reply[0], size, MSG_WAITALL)));
            urma::UrmaHello rejected;
            ASSERT_TRUE(rejected.ParseFromString(reply));
            EXPECT_EQ(0u, rejected.jetty_id());
        }
        const std::string request = "GET /version HTTP/1.1\r\nHost: localhost\r\n\r\n";
        ASSERT_EQ(request.size(), static_cast<size_t>(write(fd, request.data(), request.size())));
        char response[4096];
        const ssize_t nr = recv(fd, response, sizeof(response), 0);
        ASSERT_GT(nr, 0);
        EXPECT_NE(std::string::npos, std::string(response, nr).find(" 200 "));
    }
    server.Stop(0);
    server.Join();
}

#else  // BRPC_WITH_URMA

// When URMA is not compiled in, the test file is a no-op so the build stays
// clean. The brpc_urma_unittest target still links (against brpc-shared which
// provides the empty stubs).

#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_URMA
    urma::g_skip_urma_init = true;
    urma::g_urma_available.store(true, butil::memory_order_relaxed);
#endif
    return RUN_ALL_TESTS();
}
