# UrmaTransport

UrmaTransport provides Linux RPC over the [URMA](https://atomgit.com/openeuler/umdk)
SDK using reliable-message (`URMA_TM_RM`) Jettys and CTP paths. It supports
`baidu_std` and uses TCP for handshake negotiation.

## Build

### CMake

Install the UMDK headers, provider and `liburma`, then build:

```bash
cmake -S . -B build -DWITH_URMA=ON -DURMA_USE_MOCK=OFF \
    -DCMAKE_INSTALL_PREFIX="$PWD/build/install" -DCMAKE_INSTALL_LIBDIR=lib
cmake --build build --parallel 4
cmake --install build

cmake -S example/urma_performance -B build/urma-perf \
    -DBRPC_INCLUDE_PATH="$PWD/build/install/include" \
    -DBRPC_LIB="$PWD/build/install/lib/libbrpc.a"
cmake --build build/urma-perf --parallel 4
```

`URMA_USE_MOCK`: `AUTO` (default) selects automatically, `ON` uses the mock,
and `OFF` links `liburma`. Missing headers are downloaded at a pinned UMDK
commit; set `DOWNLOAD_URMA_HEADERS=OFF` for offline builds.

### Make

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib --with-urma
make -j4
```

`--with-urma-mock` uses the mock; `--without-urma-mock` links `liburma`.
The default is automatic selection.

### Bazel

```bash
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests
```

Bazel defaults to the mock and pinned UMDK headers; hardware builds link `-lurma`.
For offline builds, set `BRPC_DOWNLOAD_URMA_HEADERS=false` and provide headers
through the toolchain or a local BUILD target.

## Usage

```cpp
// Client
brpc::ChannelOptions options;
options.socket_mode = brpc::SOCKET_MODE_URMA;
options.protocol = "baidu_std";
brpc::Channel channel;
channel.Init("127.0.0.1:8003", &options);

// Server
brpc::ServerOptions server_options;
server_options.socket_mode = brpc::SOCKET_MODE_URMA;
server.Start(port, &server_options);
```

`AdapterTransport` selects TCP or URMA through the
[common handshake](../cn/handshake_common_design.md). Unavailable resources or
declined negotiation fall back to TCP. Malformed handshake frames or errors
after URMA establishment close the connection. TCP servers, including builds
without URMA, reply with a disabled Hello and skip the ACK exchange.
A client import failure after accepting the Hello sends a disabled ACK to
notify the server to fall back.

## Implementation

1. Exchange hello messages over TCP: v2 uses binary `URMA` frames; v3 uses
   protobuf `URM3` frames. Both carry EID, Jetty, receive capacity and segment
   information.
2. Import the peer segment before its Jetty, exchange ACKs, then activate URMA
   completion handling. TCP remains the control connection.
3. Submit SENDs and consume JFC completions through JFCE events (default) or
   polling. Local SQ and peer RQ credits control send progress.
4. CQ callbacks hold a main-Socket reference before accessing the endpoint.
   A mutex synchronizes callbacks with teardown; a generation check isolates
   callbacks from before reset.

Payload SENDs and credit-only SEND_IMM WRs use
`place_order=URMA_STRONG_ORDER` and `comp_order=1`, with JFS out-of-order
completion disabled. Each SEND carries at most 4096 bytes across all SGEs,
including RPC headers and attachments. Larger RPCs span multiple WRs.
Pool blocks default to 8192 bytes.

The buffer pool is registered once and backs IOBuf allocations and send SGEs.
User memory is registered with
`RegisterMemoryForUrma` and deregistered with `DeregisterMemoryForUrma`.

## Configuration

| Flag | Default | Purpose |
| --- | --- | --- |
| `--urma_device` | "" | Device name; empty selects the first device |
| `--urma_use_polling` | false | Poll JFCs instead of using events |
| `--urma_poller_num` | 1 | Poller bthreads per tag |
| `--urma_poller_yield` | false | Yield during polling |
| `--urma_disable_bthread` | false | Process messages inline |
| `--urma_sq_size` | 128 | JFS depth [16, 4096] |
| `--urma_rq_size` | 128 | JFR depth [16, 4096] |
| `--urma_cqe_poll_once` | 32 | Completions per poll |
| `--urma_max_sge` | 0 | SGEs per WR; 0 selects the device limit |
| `--urma_recv_zerocopy` | true | Enable zero-copy receives |
| `--urma_zerocopy_min_size` | 512 | Smaller receives are copied |
| `--urma_buffer_size` | 8192 | Pool block size in bytes |
| `--urma_buffer_count` | 65536 | Pool block count |
| `--urma_bonding_mode` | 0 | 0=standalone, 1=active-backup, 2=balance |
| `--urma_bonding_level` | 0 | 0=IODIE, 1=port |
| `--urma_prepared_jetty_cnt` | 8 | Prepared Jetty+CQ sets, capped by `RLIMIT_NOFILE` |
| `--urma_client_handshake_version` | 2 | 2=binary, 3=protobuf |
| `--urma_trace_stream` | false | Retain the last 32 stream events per connection |

`bonding*` devices configure the provider after context creation, before segments
and queues, using `urma_ubagg.h`. The default is standalone+IODIE.

## Tests and diagnostics

Tests use loopback TCP, the mock provider and the current data path. They cover
bidirectional legacy/common handshakes, final-ACK/first-packet handoff, concurrent
CQ close and reset, across v2/v3, event/polling and copy/zero-copy, with RPC
response and attachment checks.

```bash
cmake -S . -B build-urma-mock -DWITH_URMA=ON -DURMA_USE_MOCK=ON -DBUILD_UNIT_TESTS=ON
cmake --build build-urma-mock --target brpc_urma_unittest --parallel 4
./build-urma-mock/test/brpc_urma_unittest \
  --gtest_repeat=100 --gtest_shuffle
```

Enable `--urma_trace_stream=true` on both peers before connecting to dump the
last 32 records on failure or teardown: `TX` (post attempts), `RX_CQ` (completion
arrival) and `RX_USE` (input delivery), with lengths, offsets, slots, hashes and
16-byte payload prefixes. It defaults to off; keep it off for performance tests
and sensitive traffic.
