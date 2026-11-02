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

#include "brpc/urma/urma_observability.h"
#if BRPC_WITH_URMA
#include <gflags/gflags.h>

#include <algorithm>
#include <set>
#include <utility>

#include "brpc/span.h"
#include "butil/third_party/rapidjson/stringbuffer.h"
#include "butil/third_party/rapidjson/writer.h"
#include "butil/time.h"
#include "bvar/passive_status.h"
#include "bvar/reducer.h"

namespace brpc {
namespace urma {

DEFINE_int64(urma_bulk_min_bytes, 65536,
             "Minimum serialized Socket::Write size classified as a bulk write");

DEFINE_int32(urma_trace_sample_every, 0,
             "Trace one in N rpcz spans through URMA; 0 disables transport tracing");

namespace {
const char* const COUNTER_NAMES[] = {
#define BRPC_URMA_NAME(name) #name,
    BRPC_URMA_COUNTERS(BRPC_URMA_NAME)
#undef BRPC_URMA_NAME
};

struct Diagnostics {
    std::array<bvar::Adder<int64_t>, COUNTER_COUNT> counters;
    butil::atomic<uint64_t> capacity{0}, buffer_size{0}, in_use{0}, peak{0};
    std::mutex mutex;
    std::set<SocketId> sockets;
    bvar::PassiveStatus<uint64_t> capacity_var, size_var, used_var, peak_var;
    Diagnostics()
        : capacity_var("urma_pool_capacity", Read, &capacity),
          size_var("urma_pool_buffer_size", Read, &buffer_size),
          used_var("urma_pool_in_use", Read, &in_use),
          peak_var("urma_pool_peak_in_use", Read, &peak) {
        for (size_t i = 0; i < COUNTER_COUNT; ++i) {
            counters[i].expose(std::string("urma_") + COUNTER_NAMES[i]);
        }
    }
    static uint64_t Read(void* arg) {
        return static_cast<butil::atomic<uint64_t>*>(arg)->load(butil::memory_order_relaxed);
    }
};

Diagnostics& Global() {
    // bvar callbacks remain valid during process shutdown.
    static Diagnostics* const diagnostics = new Diagnostics;
    return *diagnostics;
}

using Writer = butil::rapidjson::Writer<butil::rapidjson::StringBuffer>;
void Number(Writer& writer, const char* key, uint64_t value) {
    writer.Key(key);
    writer.AddUint64(value);
}
void Id(Writer& writer, const char* key, uint64_t value) {
    writer.Key(key);
    writer.String(std::to_string(value).c_str());
}
}  // namespace

const char* CounterName(Counter counter) { return COUNTER_NAMES[counter]; }
void AddGlobalCounter(Counter counter, int64_t value) { Global().counters[counter] << value; }

void SetPoolCapacity(uint64_t count, uint64_t size) {
    Global().capacity.store(count, butil::memory_order_relaxed);
    Global().buffer_size.store(size, butil::memory_order_relaxed);
}
void PoolAllocated() {
    auto& diagnostics = Global();
    const uint64_t used = diagnostics.in_use.fetch_add(1, butil::memory_order_relaxed) + 1;
    uint64_t peak = diagnostics.peak.load(butil::memory_order_relaxed);
    while (used > peak &&
           !diagnostics.peak.compare_exchange_weak(peak, used, butil::memory_order_relaxed)) {
    }
}
void PoolDeallocated() { Global().in_use.fetch_sub(1, butil::memory_order_relaxed); }
PoolSnapshot GetPoolSnapshot() {
    auto& diagnostics = Global();
    PoolSnapshot snapshot;
    snapshot.capacity = diagnostics.capacity.load(butil::memory_order_relaxed);
    snapshot.buffer_size = diagnostics.buffer_size.load(butil::memory_order_relaxed);
    snapshot.in_use = diagnostics.in_use.load(butil::memory_order_relaxed);
    snapshot.peak_in_use = diagnostics.peak.load(butil::memory_order_relaxed);
    return snapshot;
}
void DescribePoolJson(std::ostream& os) {
    const auto snapshot = GetPoolSnapshot();
    butil::rapidjson::StringBuffer buffer;
    Writer writer(buffer);
    writer.StartObject();
    Number(writer, "capacity", snapshot.capacity);
    Number(writer, "buffer_size", snapshot.buffer_size);
    Number(writer, "in_use", snapshot.in_use);
    Number(writer, "peak_in_use", snapshot.peak_in_use);
    Number(writer, "free",
           snapshot.capacity > snapshot.in_use ? snapshot.capacity - snapshot.in_use : 0);
    Number(writer, "exhausted", Global().counters[pool_exhausted].get_value());
    Number(writer, "malloc_fallback", Global().counters[malloc_fallback].get_value());
    writer.EndObject();
    os << buffer.GetString();
}

void TraceHistory::Push(const TraceRecord& record) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (_records.size() == 64) {
        _records.pop_front();
    }
    _records.push_back(record);
}
std::vector<TraceRecord> TraceHistory::Snapshot() const {
    std::lock_guard<std::mutex> lock(_mutex);
    return {_records.begin(), _records.end()};
}
void TraceHistory::Clear() {
    std::lock_guard<std::mutex> lock(_mutex);
    _records.clear();
}

WriteTrace::WriteTrace(std::shared_ptr<Span> span, SocketId sid,
                       std::shared_ptr<TraceHistory> history)
    : _span(std::move(span)),
      _sid(sid),
      _history(std::move(history)),
      _start_us(butil::cpuwide_time_us()) {
    _span->Annotate("URMA queued socket=%llu", (unsigned long long)_sid);
}
void WriteTrace::Posted(size_t bytes) {
    _bytes.fetch_add(bytes, butil::memory_order_relaxed);
    if (_wrs.fetch_add(1, butil::memory_order_release) == 0) {
        const uint64_t elapsed = butil::cpuwide_time_us() - _start_us;
        _queue_us.store(elapsed, butil::memory_order_relaxed);
        _span->Annotate("URMA first post socket=%llu queue_us=%llu", (unsigned long long)_sid,
                        (unsigned long long)elapsed);
    }
}
void WriteTrace::Rejected(size_t bytes) {
    _bytes.fetch_sub(bytes, butil::memory_order_relaxed);
    _wrs.fetch_sub(1, butil::memory_order_relaxed);
}
void WriteTrace::Completed() {
    _completed.fetch_add(1, butil::memory_order_acq_rel);
    Finish();
}
void WriteTrace::Waited(bool sq, bool remote, uint64_t elapsed_us) {
    if (sq) {
        _sq_wait_us.fetch_add(elapsed_us, butil::memory_order_relaxed);
    }
    if (remote) {
        _remote_wait_us.fetch_add(elapsed_us, butil::memory_order_relaxed);
    }
}
void WriteTrace::PostingDone(int error) {
    if (error) {
        _error.store(error, butil::memory_order_relaxed);
    }
    _posting_done.store(true, butil::memory_order_release);
    Finish();
}
void WriteTrace::Finish() {
    if (!_posting_done.load(butil::memory_order_acquire)) {
        return;
    }
    const int error = _error.load(butil::memory_order_relaxed);
    if (!error &&
        _completed.load(butil::memory_order_acquire) != _wrs.load(butil::memory_order_acquire)) {
        return;
    }
    if (_finished.exchange(true, butil::memory_order_acq_rel)) {
        return;
    }
    TraceRecord record;
    record.trace_id = _span->trace_id();
    record.span_id = _span->span_id();
    record.log_id = _span->log_id();
    record.bytes = _bytes.load(butil::memory_order_relaxed);
    record.wrs = _wrs.load(butil::memory_order_relaxed);
    record.completed_wrs = _completed.load(butil::memory_order_relaxed);
    record.queue_us = _queue_us.load(butil::memory_order_relaxed);
    record.elapsed_us = butil::cpuwide_time_us() - _start_us;
    record.sq_wait_us = _sq_wait_us.load(butil::memory_order_relaxed);
    record.remote_credit_wait_us = _remote_wait_us.load(butil::memory_order_relaxed);
    record.error = error;
    _history->Push(record);
    _span->Annotate(
        "URMA local send finished socket=%llu bytes=%llu wrs=%llu "
        "completed_wrs=%llu sq_wait_us=%llu remote_credit_wait_us=%llu error=%d",
        (unsigned long long)_sid, (unsigned long long)record.bytes, (unsigned long long)record.wrs,
        (unsigned long long)record.completed_wrs, (unsigned long long)record.sq_wait_us,
        (unsigned long long)record.remote_credit_wait_us, error);
}

ConnectionStats::ConnectionStats() : _history(new TraceHistory) {
    (void)Global();
    Reset();
}
void ConnectionStats::Reset() {
    for (auto& counter : _counters) {
        counter.store(0, butil::memory_order_relaxed);
    }
    window_capacity.store(0, butil::memory_order_relaxed);
    handshake_version.store(0, butil::memory_order_relaxed);
    SetReason("");
    _history->Clear();
    _generation.fetch_add(1, butil::memory_order_relaxed);
}
void ConnectionStats::Add(Counter counter, int64_t value) {
    _counters[counter].fetch_add(value, butil::memory_order_relaxed);
    AddGlobalCounter(counter, value);
}
void ConnectionStats::SetReason(const char* reason, int error) {
    _error.store(error, butil::memory_order_relaxed);
    _reason.store(reason, butil::memory_order_release);
}
void ConnectionStats::Snapshot(ConnectionSnapshot* snapshot) const {
    snapshot->generation = _generation.load(butil::memory_order_relaxed);
    snapshot->reason = _reason.load(butil::memory_order_acquire);
    snapshot->error = _error.load(butil::memory_order_relaxed);
    snapshot->window_capacity = window_capacity.load(butil::memory_order_relaxed);
    snapshot->handshake_version = handshake_version.load(butil::memory_order_relaxed);
    for (size_t i = 0; i < COUNTER_COUNT; ++i) {
        snapshot->counters[i] = _counters[i].load(butil::memory_order_relaxed);
    }
    snapshot->traces = _history->Snapshot();
}
std::shared_ptr<WriteTrace> ConnectionStats::NewTrace(std::shared_ptr<Span> span, SocketId sid) {
    const int every = FLAGS_urma_trace_sample_every;
    if (!span || every <= 0 || span->span_id() % every != 0) {
        return nullptr;
    }
    return std::make_shared<WriteTrace>(std::move(span), sid, _history);
}

void DescribeConnectionJson(std::ostream& os, const ConnectionSnapshot& snapshot) {
    butil::rapidjson::StringBuffer buffer;
    Writer writer(buffer);
    writer.StartObject();
    Id(writer, "socket_id", snapshot.socket_id);
    writer.Key("socket_failed");
    writer.Bool(snapshot.socket_failed);
    writer.Key("state");
    writer.String(snapshot.state.c_str());
    writer.Key("reason");
    writer.String(snapshot.reason.c_str());
    writer.Key("error");
    writer.AddInt(snapshot.error);
    Number(writer, "generation", snapshot.generation);
    Number(writer, "sq_size", snapshot.sq_size);
    Number(writer, "rq_size", snapshot.rq_size);
    Number(writer, "window_capacity", snapshot.window_capacity);
    Number(writer, "sq_remaining", snapshot.sq_remaining);
    Number(writer, "remote_credit_remaining", snapshot.remote_credit_remaining);
    Number(writer, "handshake_version", snapshot.handshake_version);
    const auto submitted_bytes = snapshot.counters[send_bytes];
    const auto completed_bytes = snapshot.counters[send_completed_bytes];
    const auto submitted_wrs = snapshot.counters[send_wrs];
    const auto completed_wrs = snapshot.counters[send_completed_wrs];
    Number(writer, "inflight_bytes",
           submitted_bytes > completed_bytes ? submitted_bytes - completed_bytes : 0);
    Number(writer, "inflight_wrs",
           submitted_wrs > completed_wrs ? submitted_wrs - completed_wrs : 0);
    writer.Key("counters");
    writer.StartObject();
    for (size_t i = 0; i < COUNTER_COUNT; ++i) {
        Number(writer, COUNTER_NAMES[i], snapshot.counters[i]);
    }
    writer.EndObject();
    writer.Key("traces");
    writer.StartArray();
    for (const auto& trace : snapshot.traces) {
        writer.StartObject();
        writer.Key("path");
        writer.String(trace.wrs ? "urma" : "no_urma_wr");
        Id(writer, "trace_id", trace.trace_id);
        Id(writer, "span_id", trace.span_id);
        Id(writer, "log_id", trace.log_id);
        Number(writer, "bytes", trace.bytes);
        Number(writer, "wrs", trace.wrs);
        Number(writer, "completed_wrs", trace.completed_wrs);
        Number(writer, "queue_us", trace.queue_us);
        Number(writer, "elapsed_us", trace.elapsed_us);
        Number(writer, "sq_wait_us", trace.sq_wait_us);
        Number(writer, "remote_credit_wait_us", trace.remote_credit_wait_us);
        writer.Key("error");
        writer.AddInt(trace.error);
        writer.EndObject();
    }
    writer.EndArray();
    writer.EndObject();
    os << buffer.GetString();
}

void RegisterDiagnosticSocket(SocketId sid) {
    auto& diagnostics = Global();
    std::lock_guard<std::mutex> lock(diagnostics.mutex);
    diagnostics.sockets.insert(sid);
}
void UnregisterDiagnosticSocket(SocketId sid) {
    auto& diagnostics = Global();
    std::lock_guard<std::mutex> lock(diagnostics.mutex);
    diagnostics.sockets.erase(sid);
}
std::vector<SocketId> ListDiagnosticSockets(size_t limit) {
    auto& diagnostics = Global();
    std::lock_guard<std::mutex> lock(diagnostics.mutex);
    std::vector<SocketId> result;
    for (auto sid : diagnostics.sockets) {
        if (result.size() == limit) {
            break;
        }
        result.push_back(sid);
    }
    return result;
}

}  // namespace urma
}  // namespace brpc
#endif  // BRPC_WITH_URMA
