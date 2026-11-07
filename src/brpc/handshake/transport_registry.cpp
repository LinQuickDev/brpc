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

#include "brpc/handshake/transport_registry.h"

#include <algorithm>
#include <cstring>
#include <deque>
#include <mutex>

#include "butil/logging.h"

namespace brpc {
namespace handshake {

namespace {

// A deque keeps entry addresses stable across Register() calls so Find()
// results never dangle. The mutex serializes registration (static init) and
// test resets; steady-state reads take it too, which is cheap compared to
// the handshake itself.
std::deque<TransportHandshakeEntry>& Entries() {
    static std::deque<TransportHandshakeEntry> entries;
    return entries;
}

std::mutex& EntriesMutex() {
    static std::mutex m;
    return m;
}

}  // namespace

int TransportHandshakeRegistry::Register(
    const TransportHandshakeEntry& entry) {
    CHECK(entry.name != nullptr);
    CHECK(entry.recognize_specs != nullptr ||
          entry.server_adapter != nullptr)
        << "handshake registration for `" << (entry.name ? entry.name : "?")
        << "' provides neither recognition specs nor a server adapter";
    std::lock_guard<std::mutex> lock(EntriesMutex());
    for (const auto& e : Entries()) {
        CHECK(e.mode != entry.mode)
            << "duplicate handshake registration for mode "
            << static_cast<int>(entry.mode) << " (`" << e.name << "' vs `"
            << entry.name << "')";
    }
    Entries().push_back(entry);
    return 0;
}

const TransportHandshakeEntry* TransportHandshakeRegistry::Find(
    SocketMode mode) {
    std::lock_guard<std::mutex> lock(EntriesMutex());
    for (const auto& e : Entries()) {
        if (e.mode == mode) {
            return &e;
        }
    }
    return nullptr;
}

const TransportHandshakeEntry* TransportHandshakeRegistry::FindByMagic(
    const char* data, size_t len) {
    if (data == nullptr || len == 0) {
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(EntriesMutex());
    for (const auto& e : Entries()) {
        for (size_t i = 0; i < e.recognize_spec_count; ++i) {
            const FrameSpec& spec = e.recognize_specs[i];
            if (spec.magic == nullptr || spec.magic_len == 0 ||
                len < spec.magic_len) {
                continue;
            }
            if (memcmp(data, spec.magic, spec.magic_len) == 0) {
                return &e;
            }
        }
    }
    return nullptr;
}

bool TransportHandshakeRegistry::CouldMatchMagic(const char* data,
                                                 size_t len) {
    if (data == nullptr || len == 0) {
        return false;
    }
    std::lock_guard<std::mutex> lock(EntriesMutex());
    for (const auto& e : Entries()) {
        for (size_t i = 0; i < e.recognize_spec_count; ++i) {
            const FrameSpec& spec = e.recognize_specs[i];
            if (spec.magic == nullptr || spec.magic_len == 0) {
                continue;
            }
            const size_t compare_len = std::min(len, spec.magic_len);
            if (memcmp(data, spec.magic, compare_len) == 0) {
                return true;
            }
        }
    }
    return false;
}

std::vector<TransportHandshakeEntry> TransportHandshakeRegistry::All() {
    std::lock_guard<std::mutex> lock(EntriesMutex());
    return std::vector<TransportHandshakeEntry>(Entries().begin(),
                                                Entries().end());
}

void TransportHandshakeRegistry::ResetForTest() {
    std::lock_guard<std::mutex> lock(EntriesMutex());
    Entries().clear();
}

}  // namespace handshake
}  // namespace brpc
