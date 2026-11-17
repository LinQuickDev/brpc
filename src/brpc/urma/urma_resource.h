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

#ifndef BRPC_URMA_RESOURCE_H
#define BRPC_URMA_RESOURCE_H

#if BRPC_WITH_URMA

#include <cstdint>
#include <memory>

#include "urma_types.h"
#include "butil/macros.h"

namespace brpc {
namespace urma {

struct LocalJettyConfig {
    urma_context_t* context;
    uint16_t sq_depth;
    uint16_t rq_depth;
    uint8_t max_sge;
    uint8_t priority;
    bool use_polling;

    bool operator==(const LocalJettyConfig& other) const;
};

// Owns the local kernel objects shared by the cold and prepared paths.
struct LocalJettyResources {
    static std::unique_ptr<LocalJettyResources> Create(
        const LocalJettyConfig& config);
    ~LocalJettyResources();

    const LocalJettyConfig config;
    urma_jfce_t* jfce{nullptr};
    urma_jfc_t* jfc{nullptr};
    urma_jfr_t* jfr{nullptr};
    urma_jetty_t* jetty{nullptr};

private:
    explicit LocalJettyResources(const LocalJettyConfig& cfg) : config(cfg) {}
    DISALLOW_COPY_AND_ASSIGN(LocalJettyResources);
};

// Remote imports belong to a single handshake and never enter the prepared pool.
struct PeerResources {
    PeerResources() = default;
    ~PeerResources();
    void Reset();

    urma_target_jetty_t* jetty{nullptr};
    urma_target_seg_t* segment{nullptr};

private:
    DISALLOW_COPY_AND_ASSIGN(PeerResources);
};

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_RESOURCE_H
