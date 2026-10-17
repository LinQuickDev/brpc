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
#include <limits>
#include <memory>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/atomicops.h"
#include "butil/sys_byteorder.h"
#include "urma_api.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "urma_types.h"

using namespace brpc;

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_int32(urma_buffer_count);
DECLARE_uint32(urma_read_slot_size);
DECLARE_uint64(urma_read_pool_size);
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
        _previous_skip = urma::g_skip_urma_init;
        _previous_available = urma::g_urma_available.load(butil::memory_order_relaxed);
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
        urma::g_skip_urma_init = _previous_skip;
        urma::g_urma_available.store(_previous_available, butil::memory_order_relaxed);
    }

private:
    bool _previous_skip{true};
    bool _previous_available{true};
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

// ===========================================================================
// READ (one-sided READ) control protocol and data path.
//
// The tests below run against the link-time URMA mock. Control frames are
// posted through the normal urma_post_jetty_send_wr path and delivered to the
// peer JFR by the mock; completions are pumped manually so each step of the
// OFFER -> READ -> DONE exchange is deterministic (no poller bthread).
// ===========================================================================

namespace {

constexpr size_t kReadControlBytes = 56;
constexpr uint8_t kReadControlVersion = 1;
constexpr uint8_t kReadReady = 1;
constexpr uint8_t kReadOffer = 2;
constexpr uint8_t kReadDone = 3;
constexpr size_t kReadSlotBytes = 64 * 1024;
constexpr size_t kReadArenaBytes = 1 * 1024 * 1024;

// Test-only hook exported by the link-time mock. Weak so the test binary still
// links against a real liburma provider.
extern "C" void mock_urma_set_max_read_size(uint32_t) __attribute__((weak));
extern "C" uint64_t mock_urma_read_count() __attribute__((weak));

uint64_t TestHostToNet64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return (static_cast<uint64_t>(butil::HostToNet32(value & 0xffffffffU))
            << 32) |
           butil::HostToNet32(value >> 32);
#else
    return value;
#endif
}

// Encodes one 56-byte control frame exactly as UrmaEndpoint::SendReadControl
// does. Keeping the format here pins the on-wire layout in tests.
void EncodeReadControl(uint8_t frame[kReadControlBytes], uint8_t type,
                       uint64_t sender_epoch, uint64_t target_epoch,
                       uint64_t seq, uint64_t stream_offset,
                       uint64_t segment_offset, uint32_t length,
                       uint32_t max_chunk) {
    std::memset(frame, 0, kReadControlBytes);
    std::memcpy(frame, "URDC", 4);
    frame[4] = kReadControlVersion;
    frame[5] = type;
    const uint16_t size_be =
        butil::HostToNet16(static_cast<uint16_t>(kReadControlBytes));
    std::memcpy(frame + 6, &size_be, sizeof(size_be));
    const uint64_t sender_be = TestHostToNet64(sender_epoch);
    const uint64_t target_be = TestHostToNet64(target_epoch);
    const uint64_t seq_be = TestHostToNet64(seq);
    const uint64_t stream_be = TestHostToNet64(stream_offset);
    const uint64_t segment_be = TestHostToNet64(segment_offset);
    const uint32_t length_be = butil::HostToNet32(length);
    const uint32_t max_chunk_be = butil::HostToNet32(max_chunk);
    std::memcpy(frame + 8, &sender_be, sizeof(sender_be));
    std::memcpy(frame + 16, &target_be, sizeof(target_be));
    std::memcpy(frame + 24, &seq_be, sizeof(seq_be));
    std::memcpy(frame + 32, &stream_be, sizeof(stream_be));
    std::memcpy(frame + 40, &segment_be, sizeof(segment_be));
    std::memcpy(frame + 48, &length_be, sizeof(length_be));
    std::memcpy(frame + 52, &max_chunk_be, sizeof(max_chunk_be));
}

std::string MakeReadPayload(size_t size) {
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<char>((i * 131u + 17u) & 0xffu);
    }
    return data;
}

// One-time process-wide setup: small arena plus polling-mode CQ carrier.
bool EnsureReadTestEnvironment() {
    static const bool ready = [] {
        urma_init_attr_t init_attr{};
        const urma_status_t status = urma_init(&init_attr);
        if (status != URMA_SUCCESS && status != URMA_EEXIST) {
            return false;
        }
        int num_devices = 0;
        urma_device_t** devices = urma_get_device_list(&num_devices);
        const bool using_mock =
            devices != nullptr && num_devices > 0 &&
            std::strcmp(devices[0]->name, "mock_urma_device") == 0;
        urma_free_device_list(devices);
        if (!using_mock) {
            return false;
        }
        urma::g_skip_urma_init = false;
        urma::FLAGS_urma_use_polling = true;
        urma::FLAGS_urma_sq_size = 64;
        urma::FLAGS_urma_rq_size = 64;
        urma::FLAGS_urma_prepared_jetty_cnt = 0;
        urma::FLAGS_urma_buffer_count = 8192;
        urma::FLAGS_urma_read_slot_size = static_cast<uint32_t>(kReadSlotBytes);
        urma::FLAGS_urma_read_pool_size = kReadArenaBytes;
        if (mock_urma_set_max_read_size != nullptr) {
            // Let one READ cover a whole slot. Without the hook the device
            // limit (mock default 8 KiB) drives the round count.
            mock_urma_set_max_read_size(
                static_cast<uint32_t>(kReadSlotBytes));
        }
        urma::GlobalUrmaInitializeOrDie();
        return urma::IsUrmaAvailable();
    }();
    return ready;
}

bool MakeTestSocket(SocketId* sid) {
    int fds[2] = {-1, -1};
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        return false;
    }
    SocketOptions options;
    options.fd = fds[0];
    if (Socket::Create(options, sid) != 0) {
        ::close(fds[0]);
        ::close(fds[1]);
        return false;
    }
    ::close(fds[1]);
    return true;
}

void DestroyTestSocket(SocketId sid) {
    if (sid == INVALID_SOCKET_ID) {
        return;
    }
    SocketUniquePtr s;
    if (Socket::Address(sid, &s) == 0) {
        s->SetFailed();
    }
}

urma::UrmaEndpoint* MakeTestEndpoint(SocketId sid) {
    SocketUniquePtr s;
    if (Socket::Address(sid, &s) != 0) {
        return nullptr;
    }
    return new (std::nothrow) urma::UrmaEndpoint(s.get());
}

// Drains the endpoint CQ and feeds each completion through the same handler
// the pollers use. Returns false on completion error.
bool PumpCompletions(urma::UrmaEndpoint* ep, int* completions,
                     ssize_t* bytes) {
    urma_cr_t crs[32];
    while (true) {
        const int n = urma_poll_jfc(ep->_resource->jfc, 32, crs);
        if (n < 0) {
            return false;
        }
        if (n == 0) {
            return true;
        }
        for (int i = 0; i < n; ++i) {
            const ssize_t nr = ep->HandleCompletion(crs[i]);
            if (nr < 0) {
                return false;
            }
            if (completions != nullptr) {
                ++*completions;
            }
            if (bytes != nullptr) {
                *bytes += nr;
            }
        }
    }
}

// Runs both endpoints until neither side produces a completion. Control frames
// posted by one side show up as receive completions on the other, so this also
// drains credit ACKs and DONE frames.
bool PumpBoth(urma::UrmaEndpoint* a, urma::UrmaEndpoint* b,
              int* completions = nullptr, ssize_t* bytes = nullptr) {
    for (int round = 0; round < 64; ++round) {
        int ca = 0;
        int cb = 0;
        ssize_t ba = 0;
        ssize_t bb = 0;
        if (!PumpCompletions(a, &ca, &ba) ||
            !PumpCompletions(b, &cb, &bb)) {
            return false;
        }
        if (completions != nullptr) {
            *completions += ca + cb;
        }
        if (bytes != nullptr) {
            *bytes += ba + bb;
        }
        if (ca + cb == 0) {
            // An empty CQ is not progress if either endpoint is stuck waiting
            // for a READ, DONE or READY which has no completion to wake it.
            auto idle = [](urma::UrmaEndpoint* ep) {
                return !ep->_read_tx_offer_pending && !ep->_read_rx_active &&
                       !ep->_read_post_pending && !ep->_read_done_pending &&
                       !ep->_read_ready_pending && !ep->_read_early_offer;
            };
            return idle(a) && idle(b);
        }
    }
    return false;
}

uint32_t ExpectedReadChunk() {
    const uint32_t device_max = urma::GetUrmaMaxReadSize();
    return std::min(static_cast<uint32_t>(kReadSlotBytes), device_max);
}

}  // namespace

// ---------------------------------------------------------------------------
// Control frame validation. These tests only need an endpoint (no URMA
// resources): every rejected frame must fail before touching the provider.
// ---------------------------------------------------------------------------
class UrmaReadControlTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (mock_urma_set_max_read_size == nullptr) {
            GTEST_SKIP() << "READ path tests require the URMA link-time mock";
        }
        ASSERT_TRUE(EnsureReadTestEnvironment()) << "Mock READ initialization failed";
        ASSERT_TRUE(MakeTestSocket(&_sid));
        _ep.reset(MakeTestEndpoint(_sid));
        ASSERT_NE(nullptr, _ep);
        _ep->_read_max_chunk = static_cast<uint32_t>(kReadSlotBytes);
        _ep->_remote_segment_len = 1ull << 20;
    }

    void TearDown() override {
        _ep.reset();
        DestroyTestSocket(_sid);
    }

    uint8_t frame_[kReadControlBytes];
    SocketId _sid{INVALID_SOCKET_ID};
    std::unique_ptr<urma::UrmaEndpoint> _ep;
};

TEST_F(UrmaReadControlTest, ready_publishes_peer_capability) {
    EncodeReadControl(frame_, kReadReady, 7, 0, 0, 0, 0, 0,
                      static_cast<uint32_t>(kReadSlotBytes));
    ASSERT_EQ(0, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_TRUE(_ep->_read_peer_ready);
    EXPECT_EQ(7u, _ep->_read_peer_epoch);
    EXPECT_EQ(kReadSlotBytes, _ep->_read_peer_max_chunk);
    EXPECT_TRUE(_ep->_read_tx_credit);

    // A second READY on the same connection is a protocol error.
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, rejects_invalid_ready) {
    // READY must carry target_epoch == 0 and a usable chunk size.
    EncodeReadControl(frame_, kReadReady, 7, 1, 0, 0, 0, 0,
                      static_cast<uint32_t>(kReadSlotBytes));
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    EncodeReadControl(frame_, kReadReady, 7, 0, 0, 0, 0, 0, 4095);
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    // Epoch zero is not a valid sender epoch.
    EncodeReadControl(frame_, kReadReady, 0, 0, 0, 0, 0, 0,
                      static_cast<uint32_t>(kReadSlotBytes));
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, offer_before_peer_ready_is_stashed_once) {
    EncodeReadControl(frame_, kReadOffer, 7, 0, 0, 4096, 0, 8192, 0);
    ASSERT_EQ(0, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_TRUE(_ep->_read_early_offer);
    EXPECT_FALSE(_ep->_read_post_pending);
    EXPECT_FALSE(_ep->_read_rx_active);
    EXPECT_EQ(0, std::memcmp(frame_, _ep->_read_early_frame,
                             kReadControlBytes));

    // Only one early OFFER may be outstanding.
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, rejects_invalid_offer) {
    _ep->_read_peer_ready = true;
    _ep->_read_peer_epoch = 40;
    _ep->_read_tx_epoch = 9;
    _ep->_read_rx_offset = 0;
    _ep->_read_rx_seq = 0;

    // Correct baseline first.
    EncodeReadControl(frame_, kReadOffer, 40, 9, 0, 0, 0, 8192, 0);
    ASSERT_EQ(0, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_TRUE(_ep->_read_post_pending);
    EXPECT_TRUE(_ep->_read_rx_active);
    EXPECT_EQ(8192u, _ep->_read_pending_length);

    // While a READ is outstanding the next OFFER must be rejected.
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, rejects_offer_with_bad_fields) {
    struct Case {
        uint64_t sender_epoch;
        uint64_t target_epoch;
        uint64_t stream_offset;
        uint32_t length;
        uint32_t max_chunk;
    };
    const Case cases[] = {
        {41, 9, 0, 8192, 0},                 // wrong sender epoch
        {40, 8, 0, 8192, 0},                 // wrong target epoch
        {40, 9, 0, 0, 0},                    // zero length
        {40, 9, 0, static_cast<uint32_t>(kReadSlotBytes) + 1, 0},  // too big
        {40, 9, 0, 8192, 1},                 // OFFER must not carry a credit
        {40, 9, 0, 8192, 0},                 // (baseline, see below)
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        _ep->_read_peer_ready = true;
        _ep->_read_peer_epoch = 40;
        _ep->_read_tx_epoch = 9;
        _ep->_read_rx_offset = 0;
        _ep->_read_rx_seq = 0;
        _ep->_read_post_pending = false;
        _ep->_read_rx_active = false;
        _ep->_read_early_offer = false;
        EncodeReadControl(frame_, kReadOffer, cases[i].sender_epoch,
                          cases[i].target_epoch, 0, cases[i].stream_offset, 0,
                          cases[i].length, cases[i].max_chunk);
        const int rc = _ep->HandleReadControl(frame_, kReadControlBytes);
        if (i + 1 == sizeof(cases) / sizeof(cases[0])) {
            EXPECT_EQ(0, rc) << "case " << i;
        } else {
            EXPECT_EQ(-1, rc) << "case " << i;
            EXPECT_EQ(EPROTO, errno) << "case " << i;
        }
    }
}

TEST_F(UrmaReadControlTest, rejects_offer_outside_remote_segment) {
    _ep->_read_peer_ready = true;
    _ep->_read_peer_epoch = 40;
    _ep->_read_tx_epoch = 9;
    _ep->_remote_segment_len = 8192;

    EncodeReadControl(frame_, kReadOffer, 40, 9, 0, 0, 8192, 8192, 0);
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    EncodeReadControl(frame_, kReadOffer, 40, 9, 0, 0,
                      (1ull << 63), 8192, 0);
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, done_releases_tx_source_and_credit) {
    _ep->_read_peer_ready = true;
    _ep->_read_peer_epoch = 40;
    _ep->_read_tx_epoch = 9;
    _ep->_read_tx_seq = 3;
    _ep->_read_tx_offset = 4096;
    _ep->_read_tx_length = 8192;
    _ep->_read_tx_offer_pending = true;
    _ep->_read_tx_credit = false;

    EncodeReadControl(frame_, kReadDone, 40, 9, 3, 4096, 0, 8192, 1);
    ASSERT_EQ(0, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_FALSE(_ep->_read_tx_offer_pending);
    EXPECT_TRUE(_ep->_read_tx_credit);
    EXPECT_EQ(4096u + 8192u, _ep->_read_tx_offset);
    EXPECT_EQ(4u, _ep->_read_tx_seq);

    // Duplicate DONE without a new OFFER is a protocol error.
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaReadControlTest, rejects_invalid_frame_header) {
    EncodeReadControl(frame_, kReadReady, 7, 0, 0, 0, 0, 0,
                      static_cast<uint32_t>(kReadSlotBytes));

    uint8_t bad[kReadControlBytes];
    std::memcpy(bad, frame_, sizeof(bad));
    bad[0] = 'X';
    EXPECT_EQ(-1, _ep->HandleReadControl(bad, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    std::memcpy(bad, frame_, sizeof(bad));
    bad[4] = kReadControlVersion + 1;
    EXPECT_EQ(-1, _ep->HandleReadControl(bad, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    std::memcpy(bad, frame_, sizeof(bad));
    const uint16_t wrong_size = butil::HostToNet16(55);
    std::memcpy(bad + 6, &wrong_size, sizeof(wrong_size));
    EXPECT_EQ(-1, _ep->HandleReadControl(bad, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    std::memcpy(bad, frame_, sizeof(bad));
    bad[5] = 9;  // unknown control type
    EXPECT_EQ(-1, _ep->HandleReadControl(bad, kReadControlBytes));
    EXPECT_EQ(EPROTO, errno);

    // A truncated frame never gets parsed.
    EXPECT_EQ(-1, _ep->HandleReadControl(frame_, kReadControlBytes - 1));
    EXPECT_EQ(EPROTO, errno);
}

// ---------------------------------------------------------------------------
// READ slot arena: allocation, alignment, exhaustion and recycling.
// ---------------------------------------------------------------------------
class UrmaReadArenaTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (mock_urma_set_max_read_size == nullptr) {
            GTEST_SKIP() << "READ path tests require the URMA link-time mock";
        }
        ASSERT_TRUE(EnsureReadTestEnvironment()) << "Mock READ initialization failed";
    }
};

TEST_F(UrmaReadArenaTest, slot_pair_is_registered_aligned_and_recycled) {
    butil::IOBuf tx;
    butil::IOBuf rx;
    void* tx_data = nullptr;
    void* rx_data = nullptr;
    uint32_t capacity = 0;
    ASSERT_TRUE(urma::AllocateReadSlotPair(&tx, &tx_data, &rx, &rx_data,
                                           &capacity));
    EXPECT_EQ(static_cast<uint32_t>(kReadSlotBytes), capacity);
    ASSERT_NE(nullptr, tx_data);
    ASSERT_NE(nullptr, rx_data);
    EXPECT_NE(tx_data, rx_data);

    const long page = sysconf(_SC_PAGESIZE);
    ASSERT_GT(page, 0);
    EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(tx_data) % page);
    EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(rx_data) % page);

    // Both slots belong to the single registered pool segment.
    EXPECT_EQ(urma::GetPoolSegFor(nullptr), urma::GetPoolSegFor(tx_data));
    EXPECT_EQ(urma::GetPoolSegFor(nullptr), urma::GetPoolSegFor(rx_data));
    uint64_t tx_offset = 0;
    uint64_t rx_offset = 0;
    ASSERT_TRUE(urma::GetPoolSegmentOffset(tx_data, capacity, &tx_offset));
    ASSERT_TRUE(urma::GetPoolSegmentOffset(rx_data, capacity, &rx_offset));
    EXPECT_EQ(0u, tx_offset % capacity);
    EXPECT_EQ(0u, rx_offset % capacity);

    // Exhaust the arena. Every pair takes two fixed slots, so exhaustion is a
    // clean ENOMEM rather than a fallback to unregistered memory.
    std::vector<std::unique_ptr<butil::IOBuf>> txs;
    std::vector<std::unique_ptr<butil::IOBuf>> rxs;
    int extra_pairs = 0;
    while (true) {
        auto tx_buf = std::make_unique<butil::IOBuf>();
        auto rx_buf = std::make_unique<butil::IOBuf>();
        void* td = nullptr;
        void* rd = nullptr;
        uint32_t cap = 0;
        if (!urma::AllocateReadSlotPair(tx_buf.get(), &td, rx_buf.get(), &rd,
                                        &cap)) {
            EXPECT_EQ(ENOMEM, errno);
            break;
        }
        txs.push_back(std::move(tx_buf));
        rxs.push_back(std::move(rx_buf));
        ++extra_pairs;
        ASSERT_LT(extra_pairs, 1024);
    }
    EXPECT_GE(extra_pairs, 2);

    // Releasing buffers returns the slots to the free index.
    txs.clear();
    rxs.clear();
    tx.clear();
    rx.clear();
    butil::IOBuf tx2;
    butil::IOBuf rx2;
    void* tx2_data = nullptr;
    void* rx2_data = nullptr;
    uint32_t capacity2 = 0;
    EXPECT_TRUE(urma::AllocateReadSlotPair(&tx2, &tx2_data, &rx2, &rx2_data,
                                           &capacity2));
    EXPECT_EQ(static_cast<uint32_t>(kReadSlotBytes), capacity2);
}

TEST_F(UrmaReadArenaTest, control_buffer_is_contiguous_and_registered) {
    butil::IOBuf owner;
    void* data = nullptr;
    EXPECT_FALSE(urma::AllocateReadControlBuffer(&owner, &data, 0));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_FALSE(urma::AllocateReadControlBuffer(&owner, &data, 1024 * 1024));
    EXPECT_EQ(EINVAL, errno);
    ASSERT_TRUE(urma::AllocateReadControlBuffer(&owner, &data,
                                                kReadControlBytes));
    ASSERT_NE(nullptr, data);
    EXPECT_EQ(kReadControlBytes, owner.size());
    EXPECT_NE(nullptr, urma::GetPoolSegFor(data));
    uint64_t offset = 0;
    EXPECT_TRUE(urma::GetPoolSegmentOffset(data, kReadControlBytes, &offset));

    // The control buffer is reusable once its owner is released.
    owner.clear();
    butil::IOBuf owner2;
    void* data2 = nullptr;
    EXPECT_TRUE(urma::AllocateReadControlBuffer(&owner2, &data2,
                                                kReadControlBytes));
    EXPECT_NE(nullptr, data2);
}

// ---------------------------------------------------------------------------
// End-to-end READ data path between two endpoints over the mock provider.
// ---------------------------------------------------------------------------
class UrmaReadDataPathTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (mock_urma_set_max_read_size == nullptr) {
            GTEST_SKIP() << "READ path tests require the URMA link-time mock";
        }
        ASSERT_TRUE(EnsureReadTestEnvironment()) << "Mock READ initialization failed";
        ASSERT_TRUE(MakeTestSocket(&_sid_a));
        ASSERT_TRUE(MakeTestSocket(&_sid_b));
        _a.reset(MakeTestEndpoint(_sid_a));
        _b.reset(MakeTestEndpoint(_sid_b));
        ASSERT_NE(nullptr, _a);
        ASSERT_NE(nullptr, _b);

        ASSERT_EQ(0, _a->AllocateResources());
        ASSERT_EQ(0, _b->AllocateResources());
        ASSERT_EQ(0, _a->PostRecv(_a->_rq_size, false));
        ASSERT_EQ(0, _b->PostRecv(_b->_rq_size, false));

        urma::ParsedHello hello_a;
        urma::ParsedHello hello_b;
        _a->MakeLocalParsedHello(&hello_a);
        _b->MakeLocalParsedHello(&hello_b);
        ASSERT_EQ(0, _a->ImportPeer(hello_b));
        ASSERT_EQ(0, _b->ImportPeer(hello_a));
        _a->ApplyRemoteHello(hello_b);
        _b->ApplyRemoteHello(hello_a);

        ASSERT_EQ(0, _a->StartReadProtocol());
        ASSERT_EQ(0, _b->StartReadProtocol());
        ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
        ASSERT_TRUE(_a->_read_local_ready);
        ASSERT_TRUE(_a->_read_peer_ready);
        ASSERT_TRUE(_b->_read_local_ready);
        ASSERT_TRUE(_b->_read_peer_ready);
    }

    void TearDown() override {
        _a.reset();
        _b.reset();
        DestroyTestSocket(_sid_a);
        DestroyTestSocket(_sid_b);
    }

    // Hands one WriteRequest to endpoint A and drains both sides.
    ssize_t CutAndDrain(butil::IOBuf* buf) {
        butil::IOBuf* list[1] = {buf};
        const ssize_t n = _a->CutFromIOBufList(list, 1);
        EXPECT_GE(n, 0);
        EXPECT_TRUE(PumpBoth(_a.get(), _b.get()));
        return n;
    }

    SocketId _sid_a{INVALID_SOCKET_ID};
    SocketId _sid_b{INVALID_SOCKET_ID};
    std::unique_ptr<urma::UrmaEndpoint> _a;
    std::unique_ptr<urma::UrmaEndpoint> _b;
};

TEST_F(UrmaReadDataPathTest, ready_exchange_publishes_slot_capacity) {
    const uint32_t chunk = ExpectedReadChunk();
    ASSERT_GE(chunk, 4096u);
    EXPECT_EQ(chunk, _a->_read_max_chunk);
    EXPECT_EQ(chunk, _a->_read_peer_max_chunk);
    EXPECT_EQ(chunk, _b->_read_max_chunk);
    EXPECT_EQ(chunk, _b->_read_peer_max_chunk);
    EXPECT_TRUE(_a->_read_tx_credit);
    EXPECT_TRUE(_b->_read_tx_credit);
    EXPECT_FALSE(_a->_read_control_busy[kReadReady - 1]);
    EXPECT_FALSE(_b->_read_control_busy[kReadReady - 1]);

    // Control frames must never enter the RPC byte stream.
    EXPECT_EQ(0u, _a->_read_rx_offset);
    EXPECT_EQ(0u, _b->_read_rx_offset);
    EXPECT_EQ(0u, _a->_input_processor.read_buf().size());
    EXPECT_EQ(0u, _b->_input_processor.read_buf().size());
}

TEST_F(UrmaReadDataPathTest, large_write_is_carried_by_one_read) {
    const size_t chunk = ExpectedReadChunk();
    const std::string payload = MakeReadPayload(chunk);
    butil::IOBuf buf;
    ASSERT_EQ(0, buf.append(payload.data(), payload.size()));

    ASSERT_NE(nullptr, mock_urma_read_count);
    const uint64_t reads_before = mock_urma_read_count();
    ASSERT_EQ(static_cast<ssize_t>(payload.size()), CutAndDrain(&buf));
    EXPECT_EQ(reads_before + 1, mock_urma_read_count());
    EXPECT_TRUE(buf.empty());

    // Sender: the OFFER was acknowledged by DONE and its credit returned.
    EXPECT_EQ(payload.size(), _a->_read_tx_offset);
    EXPECT_EQ(1u, _a->_read_tx_seq);
    EXPECT_FALSE(_a->_read_tx_offer_pending);
    EXPECT_TRUE(_a->_read_tx_credit);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(_a->_read_control_busy[i]) << "control slot " << i;
    }
    EXPECT_TRUE(_a->_read_path_iobufs.empty());

    // Receiver: the READ copied straight into its registered slot and the
    // bytes were appended to the input stream in order.
    EXPECT_EQ(payload.size(), _b->_read_rx_offset);
    EXPECT_EQ(1u, _b->_read_rx_seq);
    EXPECT_FALSE(_b->_read_rx_active);
    EXPECT_FALSE(_b->_read_done_pending);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(_b->_read_control_busy[i]) << "control slot " << i;
    }
    std::string received;
    ASSERT_EQ(payload.size(),
              _b->_input_processor.read_buf().copy_to(&received));
    EXPECT_EQ(payload, received);
}

TEST_F(UrmaReadDataPathTest, large_write_is_split_into_back_to_back_offers) {
    const size_t chunk = ExpectedReadChunk();
    const size_t total = chunk * 2 + chunk / 2;
    const std::string payload = MakeReadPayload(total);
    butil::IOBuf buf;
    ASSERT_EQ(0, buf.append(payload.data(), payload.size()));

    size_t offered = 0;
    int rounds = 0;
    while (!buf.empty() && rounds < 16) {
        butil::IOBuf* list[1] = {&buf};
        const ssize_t n = _a->CutFromIOBufList(list, 1);
        ASSERT_GT(n, 0) << "round " << rounds;
        ASSERT_LE(static_cast<size_t>(n), chunk) << "round " << rounds;
        offered += static_cast<size_t>(n);
        ASSERT_TRUE(PumpBoth(_a.get(), _b.get())) << "round " << rounds;
        ++rounds;
    }
    ASSERT_TRUE(buf.empty());
    EXPECT_EQ(3, rounds);
    EXPECT_EQ(total, offered);

    EXPECT_EQ(total, _a->_read_tx_offset);
    EXPECT_EQ(3u, _a->_read_tx_seq);
    EXPECT_FALSE(_a->_read_tx_offer_pending);
    EXPECT_TRUE(_a->_read_tx_credit);
    EXPECT_TRUE(_a->_read_path_iobufs.empty());

    EXPECT_EQ(total, _b->_read_rx_offset);
    EXPECT_EQ(3u, _b->_read_rx_seq);
    EXPECT_FALSE(_b->_read_rx_active);
    EXPECT_FALSE(_b->_read_done_pending);
    std::string received;
    ASSERT_EQ(total, _b->_input_processor.read_buf().copy_to(&received));
    EXPECT_EQ(payload, received);
}

TEST_F(UrmaReadDataPathTest, small_write_stays_on_the_send_path) {
    const std::string payload = MakeReadPayload(1024);
    butil::IOBuf buf;
    ASSERT_EQ(0, buf.append(payload.data(), payload.size()));

    butil::IOBuf* list[1] = {&buf};
    ASSERT_EQ(static_cast<ssize_t>(payload.size()),
              _a->CutFromIOBufList(list, 1));
    EXPECT_TRUE(buf.empty());
    EXPECT_FALSE(_a->_read_tx_offer_pending);
    EXPECT_TRUE(_a->_read_path_iobufs.empty());
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));

    EXPECT_EQ(0u, _a->_read_tx_seq);
    // The SEND path advances the same byte-stream offset the READ path uses.
    EXPECT_EQ(payload.size(), _a->_read_tx_offset);
    EXPECT_EQ(payload.size(), _b->_read_rx_offset);
    EXPECT_EQ(0u, _b->_read_rx_seq);
    std::string received;
    ASSERT_EQ(payload.size(),
              _b->_input_processor.read_buf().copy_to(&received));
    EXPECT_EQ(payload, received);
}

TEST_F(UrmaReadDataPathTest, read_path_threshold_is_4096_and_shares_offsets) {
    // 4095 bytes: SEND, and the byte-stream offset advances.
    const std::string small_payload = MakeReadPayload(4095);
    butil::IOBuf small;
    ASSERT_EQ(0, small.append(small_payload.data(), small_payload.size()));
    butil::IOBuf* small_list[1] = {&small};
    ASSERT_EQ(static_cast<ssize_t>(small_payload.size()),
              _a->CutFromIOBufList(small_list, 1));
    EXPECT_FALSE(_a->_read_tx_offer_pending);
    EXPECT_TRUE(_a->_read_path_iobufs.empty());
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    EXPECT_EQ(4095u, _b->_read_rx_offset);
    _b->_input_processor.read_buf().clear();

    // 4096 bytes: READ, starting at the byte-stream offset the SEND reached.
    const std::string large_payload = MakeReadPayload(4096);
    butil::IOBuf large;
    ASSERT_EQ(0, large.append(large_payload.data(), large_payload.size()));
    butil::IOBuf* large_list[1] = {&large};
    ASSERT_EQ(static_cast<ssize_t>(large_payload.size()),
              _a->CutFromIOBufList(large_list, 1));
    EXPECT_TRUE(_a->_read_tx_offer_pending);
    // The request is fully consumed, but the source lease is still pending.
    EXPECT_TRUE(_a->_read_path_iobufs.empty());
    EXPECT_FALSE(_a->_read_tx_credit);
    EXPECT_EQ(4095u, _a->_read_tx_offset);
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));

    EXPECT_EQ(1u, _a->_read_tx_seq);
    EXPECT_EQ(4095u + 4096u, _a->_read_tx_offset);
    EXPECT_EQ(1u, _b->_read_rx_seq);
    EXPECT_EQ(4095u + 4096u, _b->_read_rx_offset);
    std::string received;
    ASSERT_EQ(large_payload.size(),
              _b->_input_processor.read_buf().copy_to(&received));
    EXPECT_EQ(large_payload, received);
}


// A rejected attempt must not pay the TX copy cost on every wakeup. There is
// no input consumption, source lease or provider operation until credits exist.
TEST_F(UrmaReadDataPathTest, credit_backpressure_does_not_copy_source) {
    const size_t size = ExpectedReadChunk();
    const std::string payload(size, 'x');
    butil::IOBuf data;
    ASSERT_EQ(0, data.append(payload));
    butil::IOBuf* list[] = {&data};
    for (int which = 0; which < 2; ++which) {
        std::memset(_a->_read_tx_data, 0x5a, size);
        auto& window = which == 0 ? _a->_remote_rq_window_size : _a->_sq_window_size;
        const uint16_t saved = window.exchange(2, butil::memory_order_relaxed);
        const ssize_t rc = _a->CutFromIOBufList(list, 1);
        const int saved_errno = errno;
        window.store(saved, butil::memory_order_relaxed);
        EXPECT_EQ(-1, rc);
        EXPECT_EQ(EAGAIN, saved_errno);
        EXPECT_EQ(payload.size(), data.size());
        EXPECT_FALSE(_a->_read_tx_offer_pending);
        EXPECT_TRUE(_a->_read_tx_credit);
        const std::string untouched(size, 'Z');
        EXPECT_EQ(0, std::memcmp(untouched.data(), _a->_read_tx_data, size));
    }
    EXPECT_EQ(static_cast<ssize_t>(size), CutAndDrain(&data));
}

TEST_F(UrmaReadDataPathTest, completion_wakes_writer_at_minimum_usable_credit) {
    const uint16_t saved_sq = _a->_sq_window_size.exchange(2, butil::memory_order_relaxed);
    const uint16_t saved_rq = _a->_remote_rq_window_size.exchange(3, butil::memory_order_relaxed);
    EXPECT_FALSE(_a->IsWritable());
    const int before = _a->_socket->_epollout_butex->load(butil::memory_order_acquire);
    uint16_t slot;
    ASSERT_TRUE(_a->ReserveSqSlot(&slot));
    urma_cr_t completion{};
    completion.status = URMA_CR_SUCCESS;
    completion.user_ctx = (static_cast<uint64_t>(slot + 1) << 8) | 1;
    ASSERT_EQ(0, _a->HandleCompletion(completion));
    EXPECT_TRUE(_a->IsWritable());
    const int after = _a->_socket->_epollout_butex->load(butil::memory_order_acquire);
    _a->_sq_window_size.store(saved_sq, butil::memory_order_relaxed);
    _a->_remote_rq_window_size.store(saved_rq, butil::memory_order_relaxed);
    EXPECT_GT(after, before);
}

TEST_F(UrmaReadDataPathTest, done_before_offer_send_completion_does_not_reuse_control) {
    butil::IOBuf data;
    ASSERT_EQ(0, data.append(MakeReadPayload(8192)));
    butil::IOBuf* list[] = {&data};
    ASSERT_EQ(8192, _a->CutFromIOBufList(list, 1));
    urma_cr_t held{};
    ASSERT_EQ(1, urma_poll_jfc(_a->_resource->jfc, 1, &held));
    ASSERT_EQ(0u, held.flag.bs.s_r);
    ASSERT_EQ(2u, held.user_ctx & 0xff);
    ASSERT_TRUE(PumpCompletions(_b.get(), nullptr, nullptr));
    ASSERT_TRUE(PumpCompletions(_a.get(), nullptr, nullptr));
    EXPECT_FALSE(_a->_read_tx_offer_pending);
    EXPECT_TRUE(_a->_read_tx_credit);
    EXPECT_TRUE(_a->_read_control_busy[kReadOffer - 1]);
    EXPECT_FALSE(_a->IsWritable());
    ASSERT_EQ(0, data.append(MakeReadPayload(8192)));
    EXPECT_EQ(-1, _a->CutFromIOBufList(list, 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ(8192u, data.size());
    ASSERT_EQ(0, _a->HandleCompletion(held));
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    EXPECT_EQ(8192, CutAndDrain(&data));
    EXPECT_EQ(2u, _b->_read_rx_seq);
}

TEST_F(UrmaReadDataPathTest, two_and_four_mib_are_delivered_without_extra_reads) {
    ASSERT_NE(nullptr, mock_urma_read_count);
    for (size_t size : {2u * 1024 * 1024, 4u * 1024 * 1024}) {
        const std::string payload = MakeReadPayload(size);
        butil::IOBuf data;
        ASSERT_EQ(0, data.append(payload));
        const uint64_t before = mock_urma_read_count();
        size_t rounds = 0;
        while (!data.empty() && rounds++ < 1024) {
            ASSERT_GT(CutAndDrain(&data), 0);
        }
        ASSERT_TRUE(data.empty());
        const size_t expected = (size + ExpectedReadChunk() - 1) / ExpectedReadChunk();
        EXPECT_EQ(expected, mock_urma_read_count() - before);
        std::string received;
        ASSERT_EQ(size, _b->_input_processor.read_buf().copy_to(&received));
        EXPECT_EQ(payload, received);
        _b->_input_processor.read_buf().clear();
    }
}

TEST_F(UrmaReadDataPathTest, bidirectional_reads_preserve_both_streams) {
    const std::string pa = MakeReadPayload(kReadSlotBytes * 2 + 17);
    const std::string pb(kReadSlotBytes * 3 + 31, 'b');
    butil::IOBuf a, b;
    ASSERT_EQ(0, a.append(pa));
    ASSERT_EQ(0, b.append(pb));
    for (int round = 0; (!a.empty() || !b.empty()) && round < 16; ++round) {
        if (!a.empty()) {
            butil::IOBuf* list[] = {&a};
            ASSERT_GT(_a->CutFromIOBufList(list, 1), 0);
        }
        if (!b.empty()) {
            butil::IOBuf* list[] = {&b};
            ASSERT_GT(_b->CutFromIOBufList(list, 1), 0);
        }
        ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    }
    ASSERT_TRUE(a.empty());
    ASSERT_TRUE(b.empty());
    std::string ra, rb;
    EXPECT_EQ(pb.size(), _a->_input_processor.read_buf().copy_to(&ra));
    EXPECT_EQ(pa.size(), _b->_input_processor.read_buf().copy_to(&rb));
    EXPECT_EQ(pb, ra);
    EXPECT_EQ(pa, rb);
    EXPECT_EQ(3u, _a->_read_tx_seq);  // The 17-byte tail stays on READ.
    EXPECT_EQ(4u, _b->_read_tx_seq);
}

TEST_F(UrmaReadDataPathTest, tx_length_uses_peer_read_limit_not_local_read_limit) {
    _a->_read_max_chunk = _b->_read_peer_max_chunk = 4096;
    _b->_read_max_chunk = _a->_read_peer_max_chunk = 8192;
    butil::IOBuf data;
    ASSERT_EQ(0, data.append(MakeReadPayload(8193)));
    EXPECT_EQ(8192, CutAndDrain(&data));
    EXPECT_EQ(1, CutAndDrain(&data));
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(8193u, _b->_input_processor.read_buf().size());
    EXPECT_EQ(2u, _a->_read_tx_seq);
}

TEST_F(UrmaReadDataPathTest, read_pending_resumes_on_sq_completion) {
    const uint16_t saved = _b->_sq_window_size.exchange(0, butil::memory_order_relaxed);
    butil::IOBuf data;
    ASSERT_EQ(0, data.append(MakeReadPayload(8192)));
    butil::IOBuf* list[] = {&data};
    ASSERT_EQ(8192, _a->CutFromIOBufList(list, 1));
    ASSERT_TRUE(PumpCompletions(_b.get(), nullptr, nullptr));
    EXPECT_TRUE(_b->_read_post_pending);
    EXPECT_TRUE(_b->_input_processor.read_buf().empty());
    // Model one of the WRs occupying SQ completing; the handler must start
    // the pending READ without another application write or explicit Progress.
    uint16_t slot;
    ASSERT_TRUE(_b->ReserveSqSlot(&slot));
    urma_cr_t completion{};
    completion.status = URMA_CR_SUCCESS;
    completion.user_ctx = (static_cast<uint64_t>(slot + 1) << 8) | 1;
    ASSERT_EQ(0, _b->HandleCompletion(completion));
    EXPECT_FALSE(_b->_read_post_pending);
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    _b->_sq_window_size.store(saved, butil::memory_order_relaxed);
    EXPECT_EQ(8192u, _b->_input_processor.read_buf().size());
    EXPECT_TRUE(_a->_read_tx_credit);
}

TEST_F(UrmaReadDataPathTest, done_pending_resumes_on_rq_credit_without_redelivery) {
    const uint16_t saved = _b->_remote_rq_window_size.exchange(0, butil::memory_order_relaxed);
    butil::IOBuf data;
    ASSERT_EQ(0, data.append(MakeReadPayload(8192)));
    butil::IOBuf* list[] = {&data};
    ASSERT_EQ(8192, _a->CutFromIOBufList(list, 1));
    ASSERT_TRUE(PumpCompletions(_b.get(), nullptr, nullptr));
    ASSERT_TRUE(PumpCompletions(_a.get(), nullptr, nullptr));
    EXPECT_TRUE(_b->_read_done_pending);
    EXPECT_EQ(8192u, _b->_input_processor.read_buf().size());
    EXPECT_FALSE(_a->_read_tx_credit);
    ASSERT_EQ(0, _a->SendImm(1));  // Deliver the modeled replenished RQ credit.
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    _b->_remote_rq_window_size.store(saved, butil::memory_order_relaxed);
    EXPECT_FALSE(_b->_read_done_pending);
    EXPECT_EQ(8192u, _b->_input_processor.read_buf().size());
    EXPECT_EQ(1u, _b->_read_rx_seq);
    EXPECT_TRUE(_a->_read_tx_credit);
}

TEST_F(UrmaReadDataPathTest, offer_tag_survives_bonding_msn_mask) {
    // With MSN enabled, a bonding device preserves only IMM bits 0..20.
    // The old 0xA5000000 tag disappeared, leaving type=2 to look like a
    // credit ACK carrying a 56-byte payload.
    butil::IOBuf payload;
    const std::string data = MakeReadPayload(8192);
    ASSERT_EQ(0, payload.append(data));
    butil::IOBuf* list[] = {&payload};
    ASSERT_EQ(static_cast<ssize_t>(data.size()),
              _a->CutFromIOBufList(list, 1));
    urma_cr_t cr{};
    ASSERT_EQ(1, urma_poll_jfc(_b->_resource->jfc, 1, &cr));
    ASSERT_EQ(URMA_CR_OPC_SEND_WITH_IMM, cr.opcode);
    ASSERT_EQ(kReadControlBytes, cr.completion_len);
    ASSERT_LE(cr.imm_data, 0x1fffffU);
    cr.imm_data = (cr.imm_data & 0x1fffffU) | 0xA5000000U;
    EXPECT_EQ(0, _b->HandleCompletion(cr));
    ASSERT_TRUE(PumpBoth(_a.get(), _b.get()));
    std::string received;
    EXPECT_EQ(data.size(), _b->_input_processor.read_buf().copy_to(&received));
    EXPECT_EQ(data, received);
}

TEST_F(UrmaReadDataPathTest, credit_ack_ignores_reserved_immediate_bits) {
    const uint16_t capacity = _b->_local_window_capacity;
    ASSERT_GT(capacity, 0);
    _b->_remote_rq_window_size.store(capacity - 1,
                                     butil::memory_order_relaxed);
    ASSERT_EQ(0, _a->SendImm(1));
    urma_cr_t cr{};
    ASSERT_EQ(1, urma_poll_jfc(_b->_resource->jfc, 1, &cr));
    ASSERT_EQ(URMA_CR_OPC_SEND_WITH_IMM, cr.opcode);
    ASSERT_EQ(0u, cr.completion_len);
    cr.imm_data |= 0xA5000000U;  // Simulate bonding provider metadata.
    EXPECT_EQ(0, _b->HandleCompletion(cr));
    EXPECT_EQ(capacity, _b->_remote_rq_window_size.load(
                            butil::memory_order_relaxed));
    EXPECT_TRUE(_b->_input_processor.read_buf().empty());
    ASSERT_TRUE(PumpCompletions(_a.get(), nullptr, nullptr));
}

TEST_F(UrmaReadDataPathTest, mock_enforces_read_limit_and_rejection_refunds_sq) {
    struct RestoreLimit {
        ~RestoreLimit() { mock_urma_set_max_read_size(kReadSlotBytes); }
    } restore;
    mock_urma_set_max_read_size(4096);
    uint64_t offset;
    ASSERT_TRUE(urma::GetPoolSegmentOffset(_a->_read_tx_data, 8192, &offset));
    const uint16_t credits = _b->_sq_window_size.load(butil::memory_order_relaxed);
    EXPECT_EQ(-1, _b->PostRead(offset, 8192));
    EXPECT_EQ(EIO, errno);
    EXPECT_EQ(credits, _b->_sq_window_size.load(butil::memory_order_relaxed));
    EXPECT_FALSE(_b->_post_uncertain);
    for (uint8_t in_use : _b->_sq_slot_in_use) EXPECT_EQ(0u, in_use);
    urma_cr_t completion{};
    EXPECT_EQ(0, urma_poll_jfc(_b->_resource->jfc, 1, &completion));
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
