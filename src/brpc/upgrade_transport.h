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

#ifndef BRPC_UPGRADE_TRANSPORT_H
#define BRPC_UPGRADE_TRANSPORT_H

#include <memory>

#include "brpc/transport.h"
#include "brpc/transport_handshake.h"

namespace brpc {

class TcpTransport;

// A TCP-first transport that may switch its data plane after a successful
// handshake. TCP remains usable before negotiation and after fallback.
class UpgradeTransport : public Transport {
public:
    int CutFromIOBuf(butil::IOBuf* buf) override;
    ssize_t CutFromIOBufList(butil::IOBuf** buf, size_t ndata) override;
    int WaitEpollOut(butil::atomic<int>* epollout_butex,
                     bool pollin, timespec duetime) override;

    int handshake_phase() const { return _handshake.phase(); }
    int handshake_version() const { return _handshake.protocol_version(); }

    static void OnNewDataFromTcp(Socket* socket);

protected:
    UpgradeTransport() = default;
    ~UpgradeTransport() override = default;

    void InitUpgradeTransport(Socket* socket, const SocketOptions& options,
                              const OnEdgeTrigger& default_on_edge);
    void ResetUpgradeTransport();

    void ActivateHighSpeed();
    void FallbackToTcp();
    void FailHandshake();
    void TryReadOnTcp();

    virtual void SetHighSpeedAvailable(bool available) = 0;
    virtual ssize_t CutFromHighSpeedIOBufList(
        butil::IOBuf** buf, size_t ndata) = 0;
    virtual int WaitHighSpeedEpollOut(butil::atomic<int>* epollout_butex,
                                      bool pollin, timespec duetime) = 0;

    // Called only for an accepted socket whose handshake phase is still
    // UNINITIALIZED. Protocols parsed by InputMessenger can leave this as a
    // no-op and install InputMessenger::OnNewMessages on server sockets.
    virtual void StartServerHandshake() {}

    handshake::HandshakeSession _handshake;
    std::shared_ptr<TcpTransport> _tcp_transport;

private:
    void ProcessTcpEvent();
    void CheckUnexpectedTcpData();
};

}  // namespace brpc

#endif  // BRPC_UPGRADE_TRANSPORT_H
