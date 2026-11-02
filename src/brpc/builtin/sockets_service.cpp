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


#include <ostream>
#include <sstream>
#include "brpc/closure_guard.h"        // ClosureGuard
#include "brpc/controller.h"           // Controller
#include "brpc/builtin/common.h"
#include "brpc/builtin/sockets_service.h"
#include "brpc/socket.h"
#if BRPC_WITH_URMA
#include "brpc/urma/urma_observability.h"
#endif

namespace brpc {

void SocketsService::default_method(::google::protobuf::RpcController* cntl_base,
                                     const ::brpc::SocketsRequest*,
                                     ::brpc::SocketsResponse*,
                                     ::google::protobuf::Closure* done) {
    ClosureGuard done_guard(done);
    Controller *cntl = static_cast<Controller*>(cntl_base);
    cntl->http_response().set_content_type("text/plain");
    butil::IOBufBuilder os;
    const std::string& constraint = cntl->http_request().unresolved_path();

#if BRPC_WITH_URMA
    if (constraint == "urma") {
        cntl->http_response().set_content_type("application/json");
        os << "{\"schema_version\":1,\"pool\":";
        urma::DescribePoolJson(os);
        os << ",\"connections\":[";
        bool first = true;
        // Fetch one extra ID to report truncation, rather than silently hide it.
        const auto ids = urma::ListDiagnosticSockets(1025);
        for (size_t i = 0; i < ids.size() && i < 1024; ++i) {
            SocketUniquePtr socket;
            if (Socket::AddressFailedAsWell(ids[i], &socket) < 0) {
                continue;
            }
            std::ostringstream snapshot;
            if (!socket->DescribeUrmaJson(snapshot)) {
                continue;
            }
            if (!first) {
                os << ',';
            }
            first = false;
            os << snapshot.str();
        }
        os << "],\"truncated\":" << (ids.size() > 1024 ? "true" : "false") << '}';
    } else
#endif
        if (constraint.empty()) {
        os << "# Use /sockets/<SocketId>\n"
           << butil::describe_resources<Socket>() << '\n';
    } else {
        char* endptr = nullptr;
        SocketId sid = strtoull(constraint.c_str(), &endptr, 10);
        if (*endptr == '\0' || *endptr == '/') {
#if BRPC_WITH_URMA
            const std::string* format = cntl->http_request().uri().GetQuery("format");
            if (format && *format == "json") {
                SocketUniquePtr socket;
                if (Socket::AddressFailedAsWell(sid, &socket) < 0 ||
                    !socket->DescribeUrmaJson(os)) {
                    cntl->SetFailed(ENOMETHOD, "Socket has no URMA diagnostics");
                } else {
                    cntl->http_response().set_content_type("application/json");
                }
            } else
#endif
            {
                Socket::DebugSocket(os, sid);
            }
        } else {
            cntl->SetFailed(ENOMETHOD, "path=%s is not a SocketId",
                            constraint.c_str());
        }
    }
    os.move_to(cntl->response_attachment());
}

} // namespace brpc
