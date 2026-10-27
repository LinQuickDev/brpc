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

#ifndef BRPC_RDMA_QOS_H
#define BRPC_RDMA_QOS_H

namespace brpc {

// Outbound QP attributes. Their scheduling behavior is defined by the fabric.
struct RdmaQosOptions {
    int service_level = 0;  // SL, range [0, 15].
    int traffic_class = 0;  // Full GRH/IP traffic class byte, range [0, 255].

    bool IsValid() const {
        return service_level >= 0 && service_level <= 15 &&
               traffic_class >= 0 && traffic_class <= 255;
    }
};

}  // namespace brpc

#endif  // BRPC_RDMA_QOS_H
