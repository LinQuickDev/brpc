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

#ifndef BRPC_RDMA_TRANSPORT_H
#define BRPC_RDMA_TRANSPORT_H

#if BRPC_WITH_RDMA
#include "brpc/socket.h"
#include "brpc/channel.h"
#include "brpc/transport.h"
#include "brpc/transport_handshake.h"

namespace brpc {
class RdmaTransport : public Transport {
friend class TransportFactory;
friend class rdma::RdmaEndpoint;
friend class rdma::RdmaConnect;
friend class rdma::RdmaHandshakeServerV2;
friend class rdma::RdmaHandshakeServerV3;
public:
    enum HandshakeState {
        UNINIT = 0x0,
        C_ALLOC_QPCQ = 0x1,
        C_HELLO_SEND = 0x2,
        C_HELLO_WAIT = 0x3,
        C_BRINGUP_QP = 0x4,
        C_ACK_SEND = 0x5,
        S_HELLO_WAIT = 0x11,
        S_ALLOC_QPCQ = 0x12,
        S_BRINGUP_QP = 0x13,
        S_HELLO_SEND = 0x14,
        S_ACK_WAIT = 0x15,
        ESTABLISHED = handshake::ESTABLISHED,
        FALLBACK_TCP = handshake::FALLBACK_TCP,
        FAILED = handshake::FAILED
    };

    void Init(Socket* socket, const SocketOptions& options) override;
    void Release() override;
    int Reset(int32_t expected_nref) override;
    std::shared_ptr<AppConnect> Connect() override;
    int CutFromIOBuf(butil::IOBuf* buf) override;
    ssize_t CutFromIOBufList(butil::IOBuf** buf, size_t ndata) override;
    int WaitEpollOut(butil::atomic<int>* _epollout_butex, bool pollin, const timespec duetime) override;
    void ProcessEvent(bthread_attr_t attr) override;
    void QueueMessage(InputMessageClosure& inputMsg, int* num_bthread_created, bool last_msg) override;
    void Debug(std::ostream &os) override;
    rdma::RdmaEndpoint* GetRdmaEp() {
        CHECK(_rdma_ep != NULL);
        return _rdma_ep;
    }
    int handshake_phase() const {
        return _handshake.phase();
    }
    static int ContextInitOrDie(bool serverOrNot, const void* _options);

    // TCP control-plane callbacks. The endpoint is intentionally absent from
    // these entry points and only participates through resource operations.
    static void OnNewDataFromTcp(Socket* socket);
    static void* ProcessHandshakeAtClient(void* arg);
    static ParseResult ExecuteServerHandshake(butil::IOBuf* source,
                                              Socket* socket);
private:
    static bool OptionsAvailableForRdma(const ChannelOptions* opt);
    static bool OptionsAvailableOverRdma(const ServerOptions* opt);

    // The on/off state of RDMA
    enum RdmaState {
        RDMA_ON,
        RDMA_OFF,
        RDMA_UNKNOWN
    };
    // The RdmaEndpoint
    rdma::RdmaEndpoint* _rdma_ep = NULL;
    // Should use RDMA or not
    RdmaState _rdma_state;
    std::shared_ptr<TcpTransport>  _tcp_transport;
    handshake::SocketHandshakeIO _handshake;
    int _handshake_version = 0;
};
} // namespace brpc
#endif // BRPC_WITH_RDMA
#endif //BRPC_RDMA_TRANSPORT_H
