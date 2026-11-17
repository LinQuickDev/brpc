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

#include "brpc/urma/urma_resource.h"

#if BRPC_WITH_URMA

#include <cerrno>
#include <new>

#include "urma_api.h"
#include "butil/logging.h"

namespace brpc {
namespace urma {

bool LocalJettyConfig::operator==(const LocalJettyConfig& other) const {
    return context == other.context && sq_depth == other.sq_depth &&
           rq_depth == other.rq_depth && max_sge == other.max_sge &&
           priority == other.priority && use_polling == other.use_polling;
}

std::unique_ptr<LocalJettyResources> LocalJettyResources::Create(
        const LocalJettyConfig& config) {
    std::unique_ptr<LocalJettyResources> resource(
        new (std::nothrow) LocalJettyResources(config));
    if (!resource) {
        errno = ENOMEM;
        return nullptr;
    }
    auto failed = [&resource](const char* operation)
            -> std::unique_ptr<LocalJettyResources> {
        const int error = errno ? errno : EIO;
        PLOG(ERROR) << operation;
        resource.reset();
        errno = error;
        return nullptr;
    };

    // The SDK requires every JFC to reference a JFCE. Polling mode does
    // not arm or consume it, but still supplies the required object.
    resource->jfce = urma_create_jfce(config.context);
    if (!resource->jfce) {
        return failed("urma_create_jfce");
    }
    if (!config.use_polling && resource->jfce->fd < 0) {
        errno = ENODEV;
        return failed("Invalid URMA JFCE fd");
    }

    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = static_cast<uint32_t>(config.sq_depth + config.rq_depth);
    jfc_cfg.jfce = resource->jfce;
    resource->jfc = urma_create_jfc(config.context, &jfc_cfg);
    if (!resource->jfc) {
        return failed("urma_create_jfc");
    }

    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = static_cast<uint32_t>(config.rq_depth);
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = resource->jfc;
    resource->jfr = urma_create_jfr(config.context, &jfr_cfg);
    if (!resource->jfr) {
        return failed("urma_create_jfr");
    }

    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(config.sq_depth);
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = config.priority;
    jetty_cfg.jfs_cfg.max_sge = config.max_sge;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = resource->jfc;
    jetty_cfg.shared.jfr = resource->jfr;
    jetty_cfg.shared.jfc = resource->jfc;
    resource->jetty = urma_create_jetty(config.context, &jetty_cfg);
    if (!resource->jetty) {
        return failed("urma_create_jetty");
    }
    return resource;
}

LocalJettyResources::~LocalJettyResources() {
    if (jetty) {
        urma_delete_jetty(jetty);
    }
    if (jfr) {
        urma_delete_jfr(jfr);
    }
    if (jfc) {
        urma_delete_jfc(jfc);
    }
    if (jfce) {
        urma_delete_jfce(jfce);
    }
}

PeerResources::~PeerResources() {
    Reset();
}

void PeerResources::Reset() {
    if (jetty) {
        urma_unimport_jetty(jetty);
        jetty = nullptr;
    }
    if (segment) {
        urma_unimport_seg(segment);
        segment = nullptr;
    }
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
