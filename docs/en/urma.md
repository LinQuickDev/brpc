# UrmaTransport

UrmaTransport provides Linux RPC over the [URMA](https://atomgit.com/openeuler/umdk)
SDK using reliable-message (`URMA_TM_RM`) Jettys and CTP paths. It supports
`baidu_std` and uses TCP for handshake negotiation.

## Build

### CMake

For hardware use, install the UMDK headers, provider and `liburma`:

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

`URMA_USE_MOCK` accepts `AUTO` (default), `ON` and `OFF`. `AUTO` selects the
mock when `liburma` is absent; `OFF` requires the real library. Missing headers
are downloaded at a pinned UMDK commit unless `DOWNLOAD_URMA_HEADERS=OFF`.

### Make

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib --with-urma
make -j4
```

Add `--with-urma-mock` to force the mock, or `--without-urma-mock` to require
`liburma`. Without either option, selection is automatic.

### Bazel

```bash
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests
```

Bazel defaults to the mock and pinned UMDK headers. Hardware builds require
`-lurma` on the linker search path. For offline headers, set
`BRPC_DOWNLOAD_URMA_HEADERS=false` and declare them through the toolchain or
a local BUILD target.

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
declined negotiation fall back to TCP; malformed handshake frames fail the
connection.
Plain TCP servers, including builds without URMA, can reply with a disabled
Hello. Following the legacy URMA convention, a disabled Hello needs no ACK.
A client import failure after accepting the Hello still sends a disabled ACK.
Errors after URMA is established fail the connection rather than switch data paths.

## Implementation

1. Exchange hello messages over TCP: v2 uses binary `URMA` frames; v3 uses
   protobuf `URM3` frames. Both carry EID, Jetty, receive capacity and segment
   information.
2. Import the peer segment before its Jetty, exchange ACKs, then activate URMA
   completion handling. TCP remains the control connection.
3. Submit SENDs and consume JFC completions through JFCE events (default) or
   polling. Local SQ and peer RQ credits control send progress.

Payload SENDs and credit-only SEND_IMM WRs use
`place_order=URMA_STRONG_ORDER` and `comp_order=1`, with JFS out-of-order
completion disabled. Each SEND carries at most 4096 bytes across all SGEs,
including RPC headers and attachments. Larger RPCs span multiple WRs; the
default 8192-byte pool blocks and advertised receive capacity stay unchanged.

The buffer pool is registered once and backs IOBuf allocations, so sends use
SGEs without per-message registration. User memory can be registered with
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

Devices named `bonding*` are configured after context creation, before segments
and queues. Bonding requires `urma_ubagg.h`; standalone+IODIE is the default.

## Diagnostics

### Handshake interoperability regression

The URMA mock tests connect the retained legacy state machines to the common
handshake over loopback TCP, in both directions, with v2/v3 and event/polling.
They execute baidu_std RPCs and verify response messages and attachments.
Barrier-controlled tests queue the first URMA packet before the final ACK byte,
or while the completed ACK's TCP parser is still active, with copy/zero-copy.
Each scenario verifies exactly one execution of each of its 17 RPCs.

```bash
cmake -S . -B build-urma-mock -DWITH_URMA=ON -DURMA_USE_MOCK=ON -DBUILD_UNIT_TESTS=ON
cmake --build build-urma-mock --target brpc_urma_unittest --parallel 4
./build-urma-mock/test/brpc_urma_unittest \
  --gtest_filter='UrmaMockTest.*legacy*:UrmaMockTest.first_urma_packet*' \
  --gtest_repeat=100 --gtest_shuffle
```

These tests run the actual legacy and common handshake code but share the
current data path and mock provider. Independent old/new binaries on two real
devices still require validation with a matching SDK and hardware environment.
The handoff tests do not cover CQ callback versus resource teardown lifetime races.

### Stream trace

Enable `--urma_trace_stream=true` on both peers before creating connections.
The one-time dump on failure or teardown records `TX` (post attempts), `RX_CQ`
(completion arrival) and `RX_USE` (delivery to the input stream), with lengths,
offsets, slots, hashes and 16-byte payload prefixes.

Trace scanning adds overhead and exposes partial RPC contents. Keep it disabled
for performance measurements and sensitive traffic.
