# UrmaTransport

UrmaTransport 基于 [URMA](https://atomgit.com/openeuler/umdk) SDK，在 Linux 上
使用可靠消息（`URMA_TM_RM`）Jetty 和 CTP 路径传输 RPC。支持 `baidu_std`，
通过 TCP 完成握手协商。

## 编译

### CMake

实机使用需安装 UMDK 头文件、provider 和 `liburma`：

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

`URMA_USE_MOCK` 可选 `AUTO`（默认）、`ON`、`OFF`。`AUTO` 在缺少 `liburma`
时使用 mock，`OFF` 要求真实库。缺少头文件时会下载固定 commit 的 UMDK；
设置 `DOWNLOAD_URMA_HEADERS=OFF` 可关闭下载。

### Make

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib --with-urma
make -j4
```

增加 `--with-urma-mock` 强制使用 mock，或 `--without-urma-mock` 要求
`liburma`；未指定时自动选择。

### Bazel

```bash
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests
```

Bazel 默认使用 mock 和固定版本的 UMDK 头文件。实机构建需确保链接器能找到
`-lurma`。离线使用时，设置 `BRPC_DOWNLOAD_URMA_HEADERS=false`，并通过
工具链或本地 BUILD 目标声明头文件。

## 使用

```cpp
// 客户端
brpc::ChannelOptions options;
options.socket_mode = brpc::SOCKET_MODE_URMA;
options.protocol = "baidu_std";
brpc::Channel channel;
channel.Init("127.0.0.1:8003", &options);

// 服务端
brpc::ServerOptions server_options;
server_options.socket_mode = brpc::SOCKET_MODE_URMA;
server.Start(port, &server_options);
```

`AdapterTransport` 通过[公共握手](handshake_common_design.md)选择 TCP 或 URMA。
资源不可用或协商拒绝时回退 TCP，握手帧畸形时关闭连接。
普通 TCP 服务端（包括未编译 URMA 的版本）可回复禁用 Hello。禁用 Hello
沿用旧 URMA 约定，不发送或等待 ACK；若客户端在接受 Hello 后导入失败，
则发送禁用 ACK 通知服务端回退。已建立 URMA 后的错误会关闭连接，不切换数据通道。

## 实现

1. 通过 TCP 交换 hello：v2 使用二进制 `URMA` 帧，v3 使用 protobuf `URM3`
   帧，携带 EID、Jetty、接收容量和 segment 信息。
2. 先导入对端 segment，再导入 Jetty；交换 ACK 后启用 URMA 完成处理，
   TCP 保留为控制连接。
3. 通过 SEND 发送数据，通过 JFCE 事件（默认）或 JFC 轮询处理完成记录。
   本地 SQ 和对端 RQ 信用控制发送进度。

业务 SEND 和信用 SEND_IMM 均设置 `place_order=URMA_STRONG_ORDER`、
`comp_order=1`，关闭 JFS 完成乱序。每个 SEND 内所有 SGE 的总载荷不超过
4096 字节，包含 RPC 头和附件。大 RPC 拆成多个 WR，默认 8192 字节缓冲块
和握手中公布的接收容量保持不变。

缓冲池一次性注册并用于 IOBuf 分配，发送时直接构造 SGE，无需逐消息注册。
用户内存通过 `RegisterMemoryForUrma` 注册、`DeregisterMemoryForUrma` 注销。

## 配置

| Flag | 默认 | 用途 |
| --- | --- | --- |
| `--urma_device` | "" | 设备名，空值选择首个设备 |
| `--urma_use_polling` | false | 使用轮询而非事件模式 |
| `--urma_poller_num` | 1 | 每个 tag 的轮询 bthread 数 |
| `--urma_poller_yield` | false | 轮询时主动让出 bthread |
| `--urma_disable_bthread` | false | 内联处理消息 |
| `--urma_sq_size` | 128 | JFS 深度 [16, 4096] |
| `--urma_rq_size` | 128 | JFR 深度 [16, 4096] |
| `--urma_cqe_poll_once` | 32 | 每次轮询的完成记录数 |
| `--urma_max_sge` | 0 | 每 WR 的 SGE 上限，0 表示设备上限 |
| `--urma_recv_zerocopy` | true | 启用接收零拷贝 |
| `--urma_zerocopy_min_size` | 512 | 小于此值的接收使用拷贝 |
| `--urma_buffer_size` | 8192 | 缓冲块大小，单位字节 |
| `--urma_buffer_count` | 65536 | 缓冲块数量 |
| `--urma_bonding_mode` | 0 | 0=standalone，1=active-backup，2=balance |
| `--urma_bonding_level` | 0 | 0=IODIE，1=port |
| `--urma_prepared_jetty_cnt` | 8 | 预分配 Jetty+CQ 数，受 `RLIMIT_NOFILE` 限制 |
| `--urma_client_handshake_version` | 2 | 2=二进制，3=protobuf |
| `--urma_trace_stream` | false | 每连接保存最近 32 条流记录 |

设备名以 `bonding` 开头时，在创建 context 后、segment 和队列前配置 provider。
bonding 需要 `urma_ubagg.h`，默认使用 standalone+IODIE。

## 诊断

### 握手互通回归

URMA mock 测试通过 loopback TCP 连接保留的旧握手状态机与公共握手，
覆盖两个方向、v2/v3 和 event/polling，并执行真实 baidu_std RPC、校验响应及附件。
ACK 交接测试用屏障固定首包早于 ACK 最后一个字节、首包到达时 ACK 解析器尚未
退出两种时序，覆盖 copy/zero-copy；每个场景验证 17 次 RPC 恰好执行一次。

```bash
cmake -S . -B build-urma-mock -DWITH_URMA=ON -DURMA_USE_MOCK=ON -DBUILD_UNIT_TESTS=ON
cmake --build build-urma-mock --target brpc_urma_unittest --parallel 4
./build-urma-mock/test/brpc_urma_unittest \
  --gtest_filter='UrmaMockTest.*legacy*:UrmaMockTest.first_urma_packet*' \
  --gtest_repeat=100 --gtest_shuffle
```

这些测试运行实际的新旧握手代码，共用当前 URMA 数据面和 mock provider，
不等同于两台实机上的独立新旧二进制互通；后者仍需使用匹配 SDK 的双机环境验证。

### CQ 生命周期回归

`UrmaMockTest.cq_*` 覆盖 CQ 分发中关闭、回调释放最后一个主 Socket 引用，
以及 reset 后执行旧 carrier 回调，分别运行 event/polling 和 copy/zero-copy。
carrier 持有独立上下文，回调先固定主 Socket 引用，再用互斥锁和代际标记
隔离关闭及重置。这是独立的数据面生命周期修复，不改变握手线协议。

```bash
./build-urma-mock/test/brpc_urma_unittest \
  --gtest_filter='UrmaMockTest.cq_*' --gtest_repeat=100 --gtest_shuffle
```

### 流记录

建连前在两端开启 `--urma_trace_stream=true`。失败或关闭连接时输出一次记录：
`TX` 为提交尝试，`RX_CQ` 为完成到达，`RX_USE` 为交给输入流的数据，
包含长度、偏移、槽位、摘要和载荷前 16 字节。

Trace 会扫描载荷并输出部分 RPC 内容；性能测试和敏感业务流量应关闭此开关。
