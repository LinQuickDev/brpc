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

// Conformance baseline for the common transport-handshake skeleton (P0).
// A self-contained loopback transport exercises the shared FrameCodec,
// HandshakeSession state machine, terminal-state publication and the
// transport registry, so framework changes are caught before any real
// transport (RDMA/UBSHM/URMA) is involved. See
// docs/cn/transport_handshake_design.md ("conformance self-test").

#include <gtest/gtest.h>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/sys_byteorder.h"
#include <gflags/gflags.h>
#include "brpc/handshake/handshake_io.h"
#include "brpc/handshake/rdma_handshake.h"
#include "brpc/handshake/ubshm_handshake.h"
#include "brpc/handshake/urma_handshake.h"
#include "brpc/handshake/transport_registry.h"
#include "brpc/transport_handshake.h"

namespace brpc {
namespace handshake {
namespace {

// ---------------------------------------------------------------------------
// Test-only blocking HandshakeIO over a raw fd (a socketpair end), with
// PushBack support through a prepend buffer.
// ---------------------------------------------------------------------------
class PipeHandshakeIO : public HandshakeIO {
public:
    explicit PipeHandshakeIO(int fd) : _fd(fd) {}

    int ReadExact(void* data, size_t len) override {
        char* p = static_cast<char*>(data);
        size_t off = 0;
        if (!_pushed.empty()) {
            const size_t n = std::min(_pushed.size(), len);
            memcpy(p, _pushed.data(), n);
            _pushed.erase(0, n);
            off += n;
        }
        while (off < len) {
            const ssize_t nr = read(_fd, p + off, len - off);
            if (nr > 0) {
                off += static_cast<size_t>(nr);
            } else if (nr < 0 && errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }
        return 0;
    }

    int WriteAll(const void* data, size_t len) override {
        const char* p = static_cast<const char*>(data);
        size_t off = 0;
        while (off < len) {
            const ssize_t nw = write(_fd, p + off, len - off);
            if (nw > 0) {
                off += static_cast<size_t>(nw);
            } else if (nw < 0 && errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        }
        return 0;
    }

    int PushBack(const void* data, size_t len) override {
        _pushed.insert(0, static_cast<const char*>(data), len);
        return 0;
    }

private:
    int _fd;
    std::string _pushed;
};

// ---------------------------------------------------------------------------
// Loopback upgrade protocol ("LBOK" v1 / "LBK2" v2), the handshake-layer
// analogue of a null transport.
//
// Hello payload (after magic + u16 total length):
//   u16 hello_ver | u16 impl_ver | u8 flags (bit0 = peer_enabled)
//   | optional opaque bytes (presence => HelloDescriptor::opaque_payload)
// ACK: the shared 4-byte network-order default. v2 additionally declares a
// fixed 8-byte extension frame exchanged after the hellos.
// ---------------------------------------------------------------------------
class LoopbackProtocol : public HandshakeProtocol {
public:
    // Plain enum constants: never odr-used (safe to bind to gtest's
    // const-reference expectations under any C++ standard).
    enum { kV1 = 1, kV2 = 2 };

    explicit LoopbackProtocol(int version) : _version(version) {}

    int ProtocolVersion() const override { return _version; }

    const FrameSpec& HelloFrameSpec() const override {
        if (_version == kV2) {
            static const FrameSpec spec("LBK2", 4, 11, 64,
                                        FrameSpec::U16_TOTAL_LENGTH);
            return spec;
        }
        static const FrameSpec spec("LBOK", 4, 11, 64,
                                    FrameSpec::U16_TOTAL_LENGTH);
        return spec;
    }

    const FrameSpec& AckFrameSpec() const override {
        static const FrameSpec spec(nullptr, 0, 4, 4, FrameSpec::FIXED);
        return spec;
    }

    bool HasExtension() const override { return _version == kV2; }

    const FrameSpec& ExtensionFrameSpec() const override {
        static const FrameSpec spec(nullptr, 0, 8, 8, FrameSpec::FIXED);
        return spec;
    }

    StepResult BuildHello(bool enabled, std::string* payload) override {
        _local = HelloDescriptor();
        _local.wire_version = _version;
        _local.peer_enabled = enabled;
        _local.opaque_payload = _advertised_opaque;
        _local.has_extension = HasExtension();

        payload->clear();
        const uint16_t hello_ver = butil::HostToNet16(_version);
        const uint16_t impl_ver = butil::HostToNet16(1);
        const uint8_t flags = enabled ? 1 : 0;
        payload->append(reinterpret_cast<const char*>(&hello_ver), 2);
        payload->append(reinterpret_cast<const char*>(&impl_ver), 2);
        payload->append(reinterpret_cast<const char*>(&flags), 1);
        payload->append(_advertised_opaque);
        return STEP_OK;
    }

    StepResult ParseHello(const std::string& payload) override {
        if (payload.size() < 5) {
            errno = EPROTO;
            return STEP_ERROR;
        }
        uint16_t hello_ver = 0;
        uint16_t impl_ver = 0;
        uint8_t flags = 0;
        memcpy(&hello_ver, payload.data(), 2);
        memcpy(&impl_ver, payload.data() + 2, 2);
        memcpy(&flags, payload.data() + 4, 1);

        _remote = HelloDescriptor();
        _remote.wire_version = butil::NetToHost16(hello_ver);
        _remote.peer_enabled = (flags & 1) != 0;
        _remote.has_extension = HasExtension();
        if (payload.size() > 5) {
            // Presence-as-capability: trailing bytes are opaque payload.
            _remote.opaque_payload = payload.substr(5);
        }
        return _remote.peer_enabled ? STEP_OK : STEP_FALLBACK;
    }

    StepResult BuildExtension(bool, std::string* payload) override {
        payload->assign("loop-ext", 8);
        return STEP_OK;
    }

    StepResult ParseExtension(const std::string& payload) override {
        if (payload != std::string("loop-ext", 8)) {
            errno = EPROTO;
            return STEP_ERROR;
        }
        return STEP_OK;
    }

    const HelloDescriptor& RemoteHello() const override { return _remote; }
    const HelloDescriptor& LocalHello() const override { return _local; }

    void set_advertised_opaque(std::string opaque) {
        _advertised_opaque = std::move(opaque);
    }

private:
    int _version;
    std::string _advertised_opaque;
    HelloDescriptor _local;
    HelloDescriptor _remote;
};

// ---------------------------------------------------------------------------
// Recording participant with scriptable outcomes.
// ---------------------------------------------------------------------------
struct RecordingTransport : public HandshakeTransport {
    HandshakeProtocol* selected = nullptr;
    int selected_calls = 0;
    int prepare_calls = 0;
    int negotiate_calls = 0;
    int established_calls = 0;
    int fallback_calls = 0;
    int failed_calls = 0;
    int validate_calls = 0;
    StepResult prepare_result = STEP_OK;
    StepResult negotiate_result = STEP_OK;
    StepResult validate_result = STEP_OK;

    void OnProtocolSelected(HandshakeProtocol* protocol) override {
        selected = protocol;
        ++selected_calls;
    }
    StepResult PrepareResources() override {
        ++prepare_calls;
        return prepare_result;
    }
    StepResult NegotiateResources() override {
        ++negotiate_calls;
        return negotiate_result;
    }
    void OnEstablished() override { ++established_calls; }
    void OnFallback() override { ++fallback_calls; }
    void OnFailed() override { ++failed_calls; }
    StepResult ValidateEstablished() override {
        ++validate_calls;
        return validate_result;
    }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static void MakeSocketPair(int fds[2]) {
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    timeval timeout;
    timeout.tv_sec = 10;
    timeout.tv_usec = 0;
    ASSERT_EQ(0, setsockopt(fds[0], SOL_SOCKET, SO_RCVTIMEO,
                            &timeout, sizeof(timeout)));
    ASSERT_EQ(0, setsockopt(fds[1], SOL_SOCKET, SO_RCVTIMEO,
                            &timeout, sizeof(timeout)));
}

// Waits for bytes on `fd', appends them to `buf'. Returns false on timeout.
static bool WaitAndDrain(int fd, butil::IOBuf* buf) {
    pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    if (poll(&pfd, 1, 10000) <= 0) {
        return false;
    }
    char tmp[4096];
    const ssize_t n = read(fd, tmp, sizeof(tmp));
    if (n <= 0) {
        return false;
    }
    buf->append(tmp, static_cast<size_t>(n));
    return true;
}

// Drives a server-side session until it leaves STEP_NEED_MORE.
static StepResult DriveServer(HandshakeSession* session, int fd,
                              butil::IOBuf* buf,
                              const std::vector<HandshakeProtocol*>& protocols,
                              HandshakeTransport* transport,
                              bool fallback_on_not_mine) {
    for (int i = 0; i < 64; ++i) {
        IOBufHandshakeInput input(buf);
        const StepResult result = session->RunServer(
            protocols, &input, transport, fallback_on_not_mine);
        if (result != STEP_NEED_MORE) {
            return result;
        }
        if (!WaitAndDrain(fd, buf)) {
            return STEP_ERROR;
        }
    }
    return STEP_ERROR;
}

static std::string EncodeClientHello(LoopbackProtocol* protocol,
                                     bool enabled) {
    std::string payload;
    EXPECT_EQ(STEP_OK, protocol->BuildHello(enabled, &payload));
    std::string frame;
    EXPECT_EQ(FRAME_OK,
              FrameCodec::Encode(protocol->HelloFrameSpec(), payload, &frame));
    return frame;
}

static std::string EncodeAck(LoopbackProtocol* protocol, bool enabled) {
    std::string payload;
    EXPECT_EQ(STEP_OK, protocol->BuildAck(enabled, &payload));
    std::string frame;
    EXPECT_EQ(FRAME_OK,
              FrameCodec::Encode(protocol->AckFrameSpec(), payload, &frame));
    return frame;
}

// ---------------------------------------------------------------------------
// Frame layer
// ---------------------------------------------------------------------------
TEST(TransportHandshakeTest, FrameCodecFixedEncoding) {
    const FrameSpec spec("AB", 2, 6, 6, FrameSpec::FIXED);
    std::string frame;
    ASSERT_EQ(FRAME_OK, FrameCodec::Encode(spec, "1234", &frame));
    ASSERT_EQ("AB1234", frame);

    butil::IOBuf buf;
    buf.append(frame);
    IOBufHandshakeInput input(&buf);
    std::string payload;
    ASSERT_EQ(FRAME_OK, FrameCodec::ParseBufferedFrame(&input, spec, &payload));
    EXPECT_EQ("1234", payload);
    EXPECT_EQ(0UL, input.Size());
}

TEST(TransportHandshakeTest, FrameCodecU16TotalLengthFragmented) {
    const FrameSpec spec("LBOK", 4, 9, 64, FrameSpec::U16_TOTAL_LENGTH);
    std::string frame;
    ASSERT_EQ(FRAME_OK, FrameCodec::Encode(spec, "xyz", &frame));
    ASSERT_EQ(9UL, frame.size());

    butil::IOBuf buf;
    std::string payload;
    // Feed the frame one slice at a time: magic, length, then body.
    for (size_t cut = 0; cut < frame.size(); ++cut) {
        buf.append(frame.data() + cut, 1);
        IOBufHandshakeInput input(&buf);
        const FrameResult result =
            FrameCodec::ParseBufferedFrame(&input, spec, &payload);
        if (cut + 1 < frame.size()) {
            EXPECT_EQ(FRAME_NEED_MORE, result) << "cut=" << cut;
        } else {
            EXPECT_EQ(FRAME_OK, result);
            EXPECT_EQ("xyz", payload);
            EXPECT_EQ(0UL, input.Size());
        }
    }
}

TEST(TransportHandshakeTest, FrameCodecU32BodyLength) {
    const FrameSpec spec("PB", 2, 6, 1024, FrameSpec::U32_BODY_LENGTH);
    std::string frame;
    ASSERT_EQ(FRAME_OK, FrameCodec::Encode(spec, "0123456789", &frame));
    ASSERT_EQ(16UL, frame.size());

    butil::IOBuf buf;
    buf.append(frame);
    IOBufHandshakeInput input(&buf);
    std::string payload;
    ASSERT_EQ(FRAME_OK, FrameCodec::ParseBufferedFrame(&input, spec, &payload));
    EXPECT_EQ("0123456789", payload);
}

TEST(TransportHandshakeTest, FrameCodecRejectsOversizedDeclaredLength) {
    const FrameSpec spec("LBOK", 4, 9, 64, FrameSpec::U16_TOTAL_LENGTH);
    std::string frame("LBOK", 4);
    const uint16_t huge = butil::HostToNet16(4096);
    frame.append(reinterpret_cast<const char*>(&huge), sizeof(huge));

    butil::IOBuf buf;
    buf.append(frame);
    IOBufHandshakeInput input(&buf);
    std::string payload;
    EXPECT_EQ(FRAME_PROTOCOL_ERROR,
              FrameCodec::ParseBufferedFrame(&input, spec, &payload));
}

TEST(TransportHandshakeTest, FrameCodecNotMinePreservesInput) {
    const FrameSpec spec("LBOK", 4, 9, 64, FrameSpec::U16_TOTAL_LENGTH);
    butil::IOBuf buf;
    buf.append("XXXXgarbage", 11);
    IOBufHandshakeInput input(&buf);
    std::string payload;
    EXPECT_EQ(FRAME_NOT_MINE,
              FrameCodec::ParseBufferedFrame(&input, spec, &payload));
    EXPECT_EQ(11UL, input.Size());
}

TEST(TransportHandshakeTest, FrameCodecReadFramePushBackOnNotMine) {
    int fds[2];
    MakeSocketPair(fds);
    PipeHandshakeIO io(fds[0]);
    const char garbage[] = "XXnot-a-frame";
    ASSERT_EQ(static_cast<ssize_t>(sizeof(garbage) - 1),
              write(fds[1], garbage, sizeof(garbage) - 1));

    const FrameSpec spec("LBOK", 4, 9, 64, FrameSpec::U16_TOTAL_LENGTH);
    std::string payload;
    EXPECT_EQ(FRAME_NOT_MINE,
              FrameCodec::ReadFrame(&io, spec, true, &payload));
    // Pushed-back bytes must be readable again, in order.
    char again[4] = {0};
    ASSERT_EQ(0, io.ReadExact(again, sizeof(again)));
    EXPECT_EQ(0, memcmp(again, "XXno", 4));

    close(fds[0]);
    close(fds[1]);
}

// ---------------------------------------------------------------------------
// Phase contract (C7): values are ordered, terminals live in their own range.
// ---------------------------------------------------------------------------
static_assert(UNINITIALIZED < PREPARING &&
                  PREPARING < HELLO_SEND && HELLO_SEND < HELLO_WAIT &&
                  HELLO_WAIT < NEGOTIATING && NEGOTIATING < ACK_SEND &&
                  ACK_SEND < ACK_WAIT && ACK_WAIT < EXTENSION_SEND &&
                  EXTENSION_SEND < EXTENSION_WAIT &&
                  EXTENSION_WAIT < ESTABLISHED,
              "Phase values must stay ordered");
static_assert(ESTABLISHED == 0x100 && FALLBACK_TCP == 0x200 &&
                  FAILED == 0x300,
              "Terminal states must stay in their dedicated range");

TEST(TransportHandshakeTest, PublishFallbackOrdersCallbackBeforeTerminal) {
    HandshakeSession session;
    session.SetPhase(ACK_WAIT);
    bool tcp_active_published = false;
    session.PublishFallback([&]() {
        // The callback runs BEFORE the terminal state becomes visible.
        EXPECT_NE(FALLBACK_TCP, session.phase(butil::memory_order_acquire));
        tcp_active_published = true;
    });
    EXPECT_TRUE(tcp_active_published);
    EXPECT_EQ(FALLBACK_TCP, session.phase(butil::memory_order_acquire));
}

// ---------------------------------------------------------------------------
// HelloDescriptor
// ---------------------------------------------------------------------------
TEST(TransportHandshakeTest, DescriptorDefaultsAndDependencyFreeSlots) {
    const HelloDescriptor& empty = EmptyHelloDescriptor();
    EXPECT_EQ(SOCKET_MODE_TCP, empty.mode);
    EXPECT_EQ(0, empty.wire_version);
    EXPECT_FALSE(empty.peer_enabled);
    EXPECT_FALSE(empty.has_extension);
    EXPECT_FALSE(empty.has_data_format);
    EXPECT_FALSE(empty.has_ece);
    EXPECT_EQ(0, empty.reject_errno);
    EXPECT_TRUE(empty.reject_msg.empty());
    EXPECT_EQ(0U, empty.rdma.qp_num);
    EXPECT_EQ(0U, empty.ubshm.len);
    EXPECT_EQ(0U, empty.urma.jetty_id);
    EXPECT_TRUE(empty.opaque_payload.empty());
}

// ---------------------------------------------------------------------------
// Transport registry
// ---------------------------------------------------------------------------
// Re-registers the entries the real transports install at static-init time.
// Needed after ResetForTest, which wipes those registrations too.
static void RestoreBuiltinRegistrations() {
    RegisterRdmaHandshake();
    RegisterUrmaHandshake();
#if BRPC_WITH_UBRING
    RegisterUBShmHandshake();
#endif
}

class TransportRegistryTest : public ::testing::Test {
protected:
    void SetUp() override { TransportHandshakeRegistry::ResetForTest(); }
    void TearDown() override {
        TransportHandshakeRegistry::ResetForTest();
        // Leave the process in the same state as program startup: other
        // tests (and other binaries sharing this registry) expect the
        // builtin transports to stay registered.
        RestoreBuiltinRegistrations();
    }
};

static const FrameSpec kLoopbackRecognizeSpecs[] = {
    FrameSpec("LBK2", 4, 11, 64, FrameSpec::U16_TOTAL_LENGTH),
    FrameSpec("LBOK", 4, 11, 64, FrameSpec::U16_TOTAL_LENGTH),
};

static HandshakeProtocol* CreateLoopbackRecognizer() {
    // A NAK-only protocol: recognizes the hello, negotiates fallback.
    class LoopbackRecognizer : public FallbackHandshakeProtocol {
    public:
        int ProtocolVersion() const override { return LoopbackProtocol::kV1; }
        const FrameSpec& HelloFrameSpec() const override {
            return kLoopbackRecognizeSpecs[1];
        }
        const FrameSpec& AckFrameSpec() const override {
            static const FrameSpec spec(nullptr, 0, 4, 4, FrameSpec::FIXED);
            return spec;
        }
        StepResult BuildHello(bool, std::string* payload) override {
            payload->assign(5, '\0');
            return STEP_OK;
        }
    };
    return new LoopbackRecognizer;
}

TEST_F(TransportRegistryTest, RegisterFindAndMagicDispatch) {
    TransportHandshakeEntry entry;
    entry.mode = SOCKET_MODE_RDMA;  // reused purely as a test key
    entry.name = "loopback";
    entry.recognize_specs = kLoopbackRecognizeSpecs;
    entry.recognize_spec_count = 2;
    entry.create_recognizer = &CreateLoopbackRecognizer;
    ASSERT_EQ(0, TransportHandshakeRegistry::Register(entry));

    const TransportHandshakeEntry* found =
        TransportHandshakeRegistry::Find(SOCKET_MODE_RDMA);
    ASSERT_TRUE(found != nullptr);
    EXPECT_STREQ("loopback", found->name);
    EXPECT_EQ(2UL, found->recognize_spec_count);

    // Magic dispatch prefers the first registered spec (v2 before v1).
    EXPECT_EQ(found, TransportHandshakeRegistry::FindByMagic(
                         "LBK2xxxxxxx", 11));
    EXPECT_EQ(found, TransportHandshakeRegistry::FindByMagic(
                         "LBOKxxxxxxx", 11));
    // An incomplete magic is not yet dispatchable.
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::FindByMagic("LBO", 3));
    // A TCP-only peer matches nothing.
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::FindByMagic(
                         "POST / HTTP", 12));

    EXPECT_EQ(1UL, TransportHandshakeRegistry::All().size());
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::Find(SOCKET_MODE_URMA));

    std::unique_ptr<HandshakeProtocol> recognizer(
        found->create_recognizer());
    EXPECT_EQ(LoopbackProtocol::kV1, recognizer->ProtocolVersion());
}

TEST_F(TransportRegistryTest, DuplicateModeIsRejected) {
    // The linked brpc library already runs background threads; the default
    // "fast" death-test style would fork that multi-threaded process and the
    // child can survive the CHECK. "threadsafe" re-executes the test binary
    // in a fresh single-threaded child instead.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    // butil CHECK failures print a stack trace but keep running unless this
    // flag is set. Enable it (via the gflags registry: the flag variable is
    // not exported from the shared library) so the duplicate registration
    // below actually aborts; this also re-runs in the death-test child.
    GFLAGS_NAMESPACE::SetCommandLineOption("crash_on_fatal_log", "true");
    TransportHandshakeEntry entry;
    entry.mode = SOCKET_MODE_UBRING;
    entry.name = "loopback-a";
    entry.recognize_specs = kLoopbackRecognizeSpecs;
    entry.recognize_spec_count = 2;
    ASSERT_EQ(0, TransportHandshakeRegistry::Register(entry));
    entry.name = "loopback-b";
    EXPECT_DEATH(TransportHandshakeRegistry::Register(entry),
                 "duplicate handshake registration");
}

// The real transports self-register at static-init time: RDMA and URMA
// always provide the recognition-only channel (a TCP-only server still NAKs
// their hellos), while UBSHM registers only in UBSHM-enabled builds so
// other builds keep the legacy TRY_OTHERS behavior for "UB" prefixes.
TEST_F(TransportRegistryTest, BuiltinTransportsSelfRegistered) {
    RestoreBuiltinRegistrations();

    const TransportHandshakeEntry* rdma =
        TransportHandshakeRegistry::Find(SOCKET_MODE_RDMA);
    ASSERT_TRUE(rdma != nullptr);
    EXPECT_STREQ("rdma", rdma->name);
    ASSERT_TRUE(rdma->server_adapter != nullptr);
    EXPECT_TRUE(rdma->server_adapter() != nullptr);
#if BRPC_WITH_RDMA
    EXPECT_TRUE(rdma->create_transport != nullptr);
    EXPECT_TRUE(rdma->upgrade_ready != nullptr);
    EXPECT_TRUE(rdma->set_high_speed_available != nullptr);
    EXPECT_TRUE(rdma->start_upgrade_events != nullptr);
    EXPECT_TRUE(rdma->run_client_upgrade != nullptr);
#else
    EXPECT_TRUE(rdma->create_transport == nullptr);
    EXPECT_TRUE(rdma->run_client_upgrade == nullptr);
#endif

    const TransportHandshakeEntry* urma =
        TransportHandshakeRegistry::Find(SOCKET_MODE_URMA);
    ASSERT_TRUE(urma != nullptr);
    EXPECT_STREQ("urma", urma->name);
    ASSERT_TRUE(urma->server_adapter != nullptr);
#if BRPC_WITH_URMA
    EXPECT_TRUE(urma->create_transport != nullptr);
    EXPECT_TRUE(urma->run_client_upgrade != nullptr);
#else
    EXPECT_TRUE(urma->create_transport == nullptr);
    EXPECT_TRUE(urma->run_client_upgrade == nullptr);
#endif

#if BRPC_WITH_UBRING
    const TransportHandshakeEntry* ubshm =
        TransportHandshakeRegistry::Find(SOCKET_MODE_UBRING);
    ASSERT_TRUE(ubshm != nullptr);
    EXPECT_STREQ("ubshm", ubshm->name);
    EXPECT_TRUE(ubshm->create_transport != nullptr);
    EXPECT_EQ(ubshm, TransportHandshakeRegistry::FindByMagic("UB", 2));
#else
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::Find(SOCKET_MODE_UBRING));
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::FindByMagic("UB", 2));
#endif

    // RDMA/URMA magics are always recognized (v2 and v3 spellings).
    EXPECT_EQ(rdma, TransportHandshakeRegistry::FindByMagic("RDMAxxxx", 8));
    EXPECT_EQ(rdma, TransportHandshakeRegistry::FindByMagic("RDM3xxxx", 8));
    EXPECT_EQ(urma, TransportHandshakeRegistry::FindByMagic("URMAxxxx", 8));
    EXPECT_EQ(urma, TransportHandshakeRegistry::FindByMagic("URM3xxxx", 8));
    // An incomplete magic is not dispatchable yet...
    EXPECT_EQ(nullptr, TransportHandshakeRegistry::FindByMagic("RDM", 3));
    // ...but may become dispatchable with more bytes.
    EXPECT_TRUE(TransportHandshakeRegistry::CouldMatchMagic("R", 1));
    EXPECT_TRUE(TransportHandshakeRegistry::CouldMatchMagic("RDM", 3));
    EXPECT_TRUE(TransportHandshakeRegistry::CouldMatchMagic("URM", 3));
    // A prefix that no registered magic starts with never dispatches.
    EXPECT_FALSE(TransportHandshakeRegistry::CouldMatchMagic("POST", 4));
    EXPECT_FALSE(TransportHandshakeRegistry::CouldMatchMagic("XXXX", 4));
}

// ---------------------------------------------------------------------------
// Session state machine over the loopback protocol
// ---------------------------------------------------------------------------
TEST(TransportHandshakeTest, ClientServerUpgradeEndToEnd) {
    int fds[2];
    MakeSocketPair(fds);

    LoopbackProtocol client_protocol(LoopbackProtocol::kV1);
    client_protocol.set_advertised_opaque("client-opaque");
    RecordingTransport client_transport;
    HandshakeSession client_session;
    PipeHandshakeIO client_io(fds[0]);
    client_session.SetIOForTest(&client_io);

    StepResult client_result = STEP_ERROR;
    std::thread client_thread([&]() {
        client_result =
            client_session.RunClient(&client_protocol, &client_transport);
    });

    LoopbackProtocol server_protocol(LoopbackProtocol::kV1);
    server_protocol.set_advertised_opaque("server-opaque");
    std::vector<HandshakeProtocol*> protocols = {&server_protocol};
    RecordingTransport server_transport;
    HandshakeSession server_session;
    PipeHandshakeIO server_io(fds[1]);
    server_session.SetIOForTest(&server_io);
    butil::IOBuf server_buf;

    const StepResult server_result = DriveServer(
        &server_session, fds[1], &server_buf, protocols, &server_transport,
        false);

    client_thread.join();
    close(fds[0]);
    close(fds[1]);

    ASSERT_EQ(STEP_OK, server_result);
    ASSERT_EQ(STEP_OK, client_result);

    EXPECT_EQ(ESTABLISHED,
              server_session.phase(butil::memory_order_acquire));
    EXPECT_EQ(ESTABLISHED,
              client_session.phase(butil::memory_order_acquire));

    // Participant callback choreography. selected_calls is 2 by design: the
    // first RunServer consumed the hello and stopped at ACK_WAIT (notify #1),
    // and the RunServer that later consumed the ACK took the recovery path,
    // which must re-notify per constraint C1 (real integrations rebuild the
    // participant per event; this loop reuses one object so it observes both
    // notifications).
    EXPECT_EQ(2, server_transport.selected_calls);
    EXPECT_EQ(&server_protocol, server_transport.selected);
    EXPECT_EQ(1, server_transport.prepare_calls);
    EXPECT_EQ(1, server_transport.negotiate_calls);
    EXPECT_EQ(1, server_transport.validate_calls);
    EXPECT_EQ(1, server_transport.established_calls);
    EXPECT_EQ(0, server_transport.fallback_calls);
    EXPECT_EQ(0, server_transport.failed_calls);
    EXPECT_EQ(1, client_transport.established_calls);
    EXPECT_EQ(0, client_transport.fallback_calls);

    // HelloDescriptor round trip in both directions.
    EXPECT_EQ(LoopbackProtocol::kV1,
              client_protocol.RemoteHello().wire_version);
    EXPECT_TRUE(client_protocol.RemoteHello().peer_enabled);
    EXPECT_EQ("server-opaque", client_protocol.RemoteHello().opaque_payload);
    EXPECT_EQ(LoopbackProtocol::kV1,
              server_protocol.RemoteHello().wire_version);
    EXPECT_EQ("client-opaque", server_protocol.RemoteHello().opaque_payload);
    EXPECT_EQ("client-opaque", client_protocol.LocalHello().opaque_payload);
}

TEST(TransportHandshakeTest, ExtensionFrameExchangedWhenDeclared) {
    int fds[2];
    MakeSocketPair(fds);

    LoopbackProtocol client_protocol(LoopbackProtocol::kV2);
    RecordingTransport client_transport;
    HandshakeSession client_session;
    PipeHandshakeIO client_io(fds[0]);
    client_session.SetIOForTest(&client_io);

    StepResult client_result = STEP_ERROR;
    std::thread client_thread([&]() {
        client_result =
            client_session.RunClient(&client_protocol, &client_transport);
    });

    LoopbackProtocol server_protocol(LoopbackProtocol::kV2);
    std::vector<HandshakeProtocol*> protocols = {&server_protocol};
    RecordingTransport server_transport;
    HandshakeSession server_session;
    PipeHandshakeIO server_io(fds[1]);
    server_session.SetIOForTest(&server_io);
    butil::IOBuf server_buf;

    const StepResult server_result = DriveServer(
        &server_session, fds[1], &server_buf, protocols, &server_transport,
        false);

    client_thread.join();
    close(fds[0]);
    close(fds[1]);

    ASSERT_EQ(STEP_OK, server_result);
    ASSERT_EQ(STEP_OK, client_result);
    EXPECT_EQ(ESTABLISHED,
              server_session.phase(butil::memory_order_acquire));
    EXPECT_EQ(ESTABLISHED,
              client_session.phase(butil::memory_order_acquire));
    EXPECT_EQ(LoopbackProtocol::kV2,
              client_protocol.RemoteHello().wire_version);
}

// Constraint C1: a later ACK event rebuilds the per-event participant; the
// recovery branch must re-notify OnProtocolSelected or the new participant
// dereferences a dangling protocol pointer.
TEST(TransportHandshakeTest, RecoveryReNotifiesProtocolSelected) {
    LoopbackProtocol protocol(LoopbackProtocol::kV1);
    std::vector<HandshakeProtocol*> protocols = {&protocol};

    const int devnull = open("/dev/null", O_WRONLY);
    ASSERT_GE(devnull, 0);
    HandshakeSession server_session;
    PipeHandshakeIO server_io(devnull);
    server_session.SetIOForTest(&server_io);

    // First event: only the client hello is available.
    butil::IOBuf buf;
    const std::string hello = EncodeClientHello(&protocol, true);
    buf.append(hello);
    RecordingTransport first_transport;
    {
        IOBufHandshakeInput input(&buf);
        EXPECT_EQ(STEP_NEED_MORE,
                  server_session.RunServer(protocols, &input,
                                           &first_transport, false));
    }
    EXPECT_EQ(ACK_WAIT, server_session.phase(butil::memory_order_acquire));
    EXPECT_EQ(1, first_transport.selected_calls);

    // The ACK arrives in a later event with a freshly built participant.
    RecordingTransport second_transport;
    const std::string ack = EncodeAck(&protocol, true);
    buf.append(ack);
    {
        IOBufHandshakeInput input(&buf);
        EXPECT_EQ(STEP_OK,
                  server_session.RunServer(protocols, &input,
                                           &second_transport, false));
    }
    EXPECT_EQ(ESTABLISHED,
              server_session.phase(butil::memory_order_acquire));
    // Without the C1 fix this participant never learns the protocol.
    EXPECT_EQ(&protocol, second_transport.selected);
    EXPECT_EQ(1, second_transport.selected_calls);
    EXPECT_EQ(1, second_transport.validate_calls);
    EXPECT_EQ(1, second_transport.established_calls);

    close(devnull);
}

TEST(TransportHandshakeTest, DisabledHelloFallsBackOnBothSides) {
    int fds[2];
    MakeSocketPair(fds);

    LoopbackProtocol client_protocol(LoopbackProtocol::kV1);
    RecordingTransport client_transport;
    HandshakeSession client_session;
    PipeHandshakeIO client_io(fds[0]);
    client_session.SetIOForTest(&client_io);

    StepResult client_result = STEP_ERROR;
    std::thread client_thread([&]() {
        client_result =
            client_session.RunClient(&client_protocol, &client_transport);
    });

    // The server cannot prepare resources, so it NAKs with a disabled hello.
    LoopbackProtocol server_protocol(LoopbackProtocol::kV1);
    std::vector<HandshakeProtocol*> protocols = {&server_protocol};
    RecordingTransport server_transport;
    server_transport.prepare_result = STEP_FALLBACK;
    HandshakeSession server_session;
    PipeHandshakeIO server_io(fds[1]);
    server_session.SetIOForTest(&server_io);
    butil::IOBuf server_buf;

    const StepResult server_result = DriveServer(
        &server_session, fds[1], &server_buf, protocols, &server_transport,
        false);

    client_thread.join();
    close(fds[0]);
    close(fds[1]);

    EXPECT_EQ(STEP_FALLBACK, server_result);
    EXPECT_EQ(STEP_FALLBACK, client_result);
    EXPECT_EQ(FALLBACK_TCP,
              server_session.phase(butil::memory_order_acquire));
    EXPECT_EQ(FALLBACK_TCP,
              client_session.phase(butil::memory_order_acquire));
    // OnFallback is invoked exactly once per side (idempotency contract C3).
    EXPECT_EQ(1, server_transport.fallback_calls);
    EXPECT_EQ(1, client_transport.fallback_calls);
    EXPECT_EQ(0, server_transport.established_calls);
    EXPECT_EQ(0, client_transport.established_calls);
}

TEST(TransportHandshakeTest, NotMineRespectsFallbackPolicy) {
    LoopbackProtocol protocol(LoopbackProtocol::kV1);
    std::vector<HandshakeProtocol*> protocols = {&protocol};

    const int devnull = open("/dev/null", O_WRONLY);
    ASSERT_GE(devnull, 0);

    // Policy off: the bytes are handed back untouched for other parsers.
    {
        HandshakeSession session;
        PipeHandshakeIO io(devnull);
        session.SetIOForTest(&io);
        butil::IOBuf buf;
        buf.append("GET / HTTP/1.1\r\n\r\n", 18);
        RecordingTransport transport;
        IOBufHandshakeInput input(&buf);
        EXPECT_EQ(STEP_NOT_MINE,
                  session.RunServer(protocols, &input, &transport, false));
        EXPECT_EQ(UNINITIALIZED, session.phase(butil::memory_order_acquire));
        EXPECT_EQ(18UL, input.Size());
        EXPECT_EQ(0, transport.fallback_calls);
    }

    // Policy on: the connection degrades to TCP in place.
    {
        HandshakeSession session;
        PipeHandshakeIO io(devnull);
        session.SetIOForTest(&io);
        butil::IOBuf buf;
        buf.append("GET / HTTP/1.1\r\n\r\n", 18);
        RecordingTransport transport;
        IOBufHandshakeInput input(&buf);
        EXPECT_EQ(STEP_FALLBACK,
                  session.RunServer(protocols, &input, &transport, true));
        EXPECT_EQ(FALLBACK_TCP, session.phase(butil::memory_order_acquire));
        EXPECT_EQ(1, transport.fallback_calls);
    }

    close(devnull);
}

TEST(TransportHandshakeTest, FallbackTerminalBypassesUpgradeCodecs) {
    LoopbackProtocol protocol(LoopbackProtocol::kV1);
    std::vector<HandshakeProtocol*> protocols = {&protocol};
    const int devnull = open("/dev/null", O_WRONLY);
    ASSERT_GE(devnull, 0);
    HandshakeSession session;
    PipeHandshakeIO io(devnull);
    session.SetIOForTest(&io);
    RecordingTransport transport;

    // Publish FALLBACK_TCP first, then deliver upgrade-looking bytes: they
    // are application data and must not re-enter the handshake.
    session.PublishFallback([&]() {});
    butil::IOBuf buf;
    const std::string hello = EncodeClientHello(&protocol, true);
    buf.append(hello);
    IOBufHandshakeInput input(&buf);
    EXPECT_EQ(STEP_NOT_MINE,
              session.RunServer(protocols, &input, &transport, false));
    EXPECT_EQ(FALLBACK_TCP, session.phase(butil::memory_order_acquire));
    EXPECT_EQ(hello.size(), input.Size());
    EXPECT_EQ(0, transport.selected_calls);

    close(devnull);
}

}  // namespace
}  // namespace handshake
}  // namespace brpc
