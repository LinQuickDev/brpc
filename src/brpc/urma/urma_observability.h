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

#ifndef BRPC_URMA_OBSERVABILITY_H
#define BRPC_URMA_OBSERVABILITY_H

#if BRPC_WITH_URMA
#include <array>
#include <deque>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "brpc/socket_id.h"
#include "butil/atomicops.h"

namespace brpc {
class Span;
namespace urma {

// Counters count data WRs, not RPCs. Credit-only WRs are excluded.
// clang-format off
#define BRPC_URMA_COUNTERS(V) \
    V(writes) \
    V(bulk_writes) \
    V(bulk_write_bytes) \
    V(send_bytes) \
    V(send_wrs) \
    V(send_completed_bytes) \
    V(send_completed_wrs) \
    V(recv_bytes) \
    V(recv_wrs) \
    V(send_errors) \
    V(recv_post_errors) \
    V(completion_errors) \
    V(sq_blocked) \
    V(remote_credit_blocked) \
    V(sq_wait_us) \
    V(remote_credit_wait_us) \
    V(handshake_success) \
    V(handshake_failed) \
    V(tcp_fallback) \
    V(pool_exhausted) \
    V(malloc_fallback)

enum Counter {
#define BRPC_URMA_ENUM(name) name,
    BRPC_URMA_COUNTERS(BRPC_URMA_ENUM)
#undef BRPC_URMA_ENUM
    COUNTER_COUNT
};
// clang-format on
const char* CounterName(Counter counter);
void AddGlobalCounter(Counter counter, int64_t value = 1);

struct PoolSnapshot {
    uint64_t capacity = 0;
    uint64_t buffer_size = 0;
    uint64_t in_use = 0;
    uint64_t peak_in_use = 0;
};
void SetPoolCapacity(uint64_t count, uint64_t buffer_size);
void PoolAllocated();
void PoolDeallocated();
PoolSnapshot GetPoolSnapshot();
void DescribePoolJson(std::ostream& os);

struct TraceRecord {
    uint64_t trace_id = 0;
    uint64_t span_id = 0;
    uint64_t log_id = 0;
    uint64_t bytes = 0;
    uint64_t wrs = 0;
    uint64_t completed_wrs = 0;
    uint64_t queue_us = 0;
    uint64_t elapsed_us = 0;
    uint64_t sq_wait_us = 0;
    uint64_t remote_credit_wait_us = 0;
    int error = 0;
};

// Storage is bounded per connection, independent of rpcz's disk retention.
class TraceHistory {
   public:
    void Push(const TraceRecord& record);
    std::vector<TraceRecord> Snapshot() const;
    void Clear();

   private:
    mutable std::mutex _mutex;
    std::deque<TraceRecord> _records;
};

// Explicitly carried from Socket::Write through KeepWrite and each data WR.
// A WR may reference multiple traces; a trace may span multiple WRs.
class WriteTrace {
   public:
    WriteTrace(std::shared_ptr<Span> span, SocketId sid, std::shared_ptr<TraceHistory> history);
    void Posted(size_t bytes);
    void Completed();
    void Rejected(size_t bytes);
    void Waited(bool sq, bool remote, uint64_t elapsed_us);
    void PostingDone(int error = 0);

   private:
    void Finish();
    std::shared_ptr<Span> _span;
    const SocketId _sid;
    std::shared_ptr<TraceHistory> _history;
    const int64_t _start_us;
    butil::atomic<uint64_t> _bytes{0}, _wrs{0}, _completed{0};
    butil::atomic<uint64_t> _queue_us{0}, _sq_wait_us{0}, _remote_wait_us{0};
    butil::atomic<bool> _posting_done{false}, _finished{false};
    butil::atomic<int> _error{0};
};

struct TraceContribution {
    std::shared_ptr<WriteTrace> trace;
    size_t bytes;
};

struct ConnectionSnapshot {
    SocketId socket_id = INVALID_SOCKET_ID;
    bool socket_failed = false;
    std::string state;
    std::string reason;
    int error = 0;
    uint64_t generation = 0;
    uint64_t sq_size = 0, rq_size = 0, window_capacity = 0;
    uint64_t sq_remaining = 0, remote_credit_remaining = 0;
    uint64_t handshake_version = 0;
    std::array<uint64_t, COUNTER_COUNT> counters{};
    std::vector<TraceRecord> traces;
};
void DescribeConnectionJson(std::ostream& os, const ConnectionSnapshot& snapshot);

class ConnectionStats {
   public:
    ConnectionStats();
    void Reset();
    void Add(Counter counter, int64_t value = 1);
    void SetReason(const char* static_reason, int error = 0);
    void Snapshot(ConnectionSnapshot* snapshot) const;
    std::shared_ptr<WriteTrace> NewTrace(std::shared_ptr<Span> span, SocketId sid);
    butil::atomic<uint64_t> window_capacity{0};
    butil::atomic<uint64_t> handshake_version{0};

   private:
    std::array<butil::atomic<uint64_t>, COUNTER_COUNT> _counters;
    butil::atomic<uint64_t> _generation{0};
    butil::atomic<const char*> _reason{""};
    butil::atomic<int> _error{0};
    std::shared_ptr<TraceHistory> _history;
};

void RegisterDiagnosticSocket(SocketId sid);
void UnregisterDiagnosticSocket(SocketId sid);
std::vector<SocketId> ListDiagnosticSockets(size_t limit);

}  // namespace urma
}  // namespace brpc
#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_OBSERVABILITY_H
