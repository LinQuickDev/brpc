# UrmaTransport：基于 URMA 的远程内存 RPC

UrmaTransport 是使用 openEuler
[URMA](https://atomgit.com/openeuler/umdk)（Unified Remote Memory Access）SDK
实现的传输层。它是
[#3217](https://github.com/apache/brpc/discussions/3217) 中提出的路线 B，
与基于 OBMM 的 UBRing 传输（路线 A，
[#3226](https://github.com/apache/brpc/issues/3226)）互补，承担大包/跨节点
高吞吐场景，共同构成路线 C（双后端）。

## 技术背景

URMA 在 UMDK 支持的设备上提供 verbs 风格接口。当前实现创建可靠消息
（`URMA_TM_RM`）Jetty，并使用 CTP 或 RTP 传输路径；通过
`urma_post_jetty_send_wr` 提交发送 WR，通过 `urma_post_jfr_wr` 提交接收
WR。完成事件既可由 JFC 忙轮询获取，也可通过 JFCE 事件 fd 获取。

## 编译配置

### CMake 编译

```bash
# 带 URMA 支持编译 brpc
cmake -B build -DWITH_URMA=ON
make -C build -j$(nproc)

# 编译 urma_performance 示例
cd example/urma_performance
cmake -B build
make -C build -j$(nproc)
```

`WITH_URMA=ON` 使用上游 UMDK 头文件进行编译。CMake 优先使用系统安装的
SDK；找不到头文件时，会参照 Mooncake 的 mock 构建方式下载固定版本的
UMDK commit，可通过 `DOWNLOAD_URMA_HEADERS=OFF` 禁止下载。找到 `liburma` 时使用
真实硬件数据通路，否则链接 brpc 的 mock，使 URMA 代码和测试仍可在无硬件
环境编译。`URMA_USE_MOCK` 默认为 `AUTO`；设为 `ON` 可强制使用 mock，设为
`OFF` 则要求找到 `liburma` 并排除 mock。

### Make 编译

```bash
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma
make -j4

# 强制使用 mock，或强制使用真实 liburma
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --with-urma-mock
sh config_brpc.sh --headers=/usr/include --libs=/usr/lib \
    --with-urma --without-urma-mock
```

默认根据 `liburma` 是否存在自动选择；`--without-urma-mock` 要求配置路径中
存在 `liburma`。

### Bazel 编译

```bash
# 编译带 URMA 支持的 brpc
bazel build --define=BRPC_WITH_URMA=true //:brpc

# 仅展示关闭下载的开关；头文件仍须由工具链或本地 BUILD 目标声明
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_DOWNLOAD_URMA_HEADERS=false //:brpc

# 编译并运行 URMA 单元测试（使用 mock，不需要 URMA 硬件）
bazel test --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=true //test:brpc_unittests

# 使用真实 liburma；要求链接器可找到 -lurma
bazel build --define=BRPC_WITH_URMA=true \
    --define=BRPC_URMA_USE_MOCK=false //:brpc
```

Bazel 会自动获取固定 commit 的 UMDK 头文件，并通过 `@umdk//:urma_headers`
提供给 URMA 目标。设置 `BRPC_DOWNLOAD_URMA_HEADERS=false` 后，Bazel 不再依赖
该外部仓库；实际构建仍需通过工具链或本地 BUILD 目标声明
`urma_api.h`、`urma_types.h` 和 `urma_ubagg.h`。仅通过
`--copt=-I/path` 指向工作区外目录，在默认 Bazel 沙箱下不会把未声明的头文件
带入编译输入。设置
`BRPC_URMA_USE_MOCK=true` 强制加入 mock；设置为 `false` 则排除 mock 并链接
`liburma`。未指定时，URMA 构建默认使用 mock。

## 使用

通过在 channel / server 上设置 `socket_mode` 选择传输层：

```cpp
// 客户端
brpc::ChannelOptions opt;
opt.socket_mode = brpc::SOCKET_MODE_URMA;
opt.protocol = "baidu_std";   // URMA 仅支持 baidu_std
brpc::Channel channel;
channel.Init("127.0.0.1:8003", &opt);

// 服务端
brpc::ServerOptions sopt;
sopt.socket_mode = brpc::SOCKET_MODE_URMA;
server.Start(port, &sopt);
```

若对端不支持 URMA（例如 TCP 客户端连接 URMA 服务端），在 4 字节 magic
握手后透明回退到 TCP，应用代码无需改动。

## 架构

UrmaTransport 沿用与 `RdmaTransport` / `UBShmTransport` 一致的两层设计：

```
UrmaTransport : public Transport         (urma_transport.{h,cpp})
  +-- std::shared_ptr<TcpTransport>      (回退路径)
  +-- urma::UrmaEndpoint*                (URMA 数据路径)
  +-- UrmaState { URMA_ON, URMA_OFF, URMA_UNKNOWN }

urma::UrmaEndpoint : public SocketUser   (urma/urma_endpoint.{h,cpp})
  +-- UrmaResource { jfc, jfce, jfr, jetty, remote_jetty, remote_seg }
  +-- 握手状态机（C/S 对称，在 TCP fd 上驱动）
  +-- 发送路径：urma_post_jetty_send_wr(URMA_OPC_SEND)
  +-- 接收路径：urma_poll_jfc -> HandleCompletion -> InputMessenger
  +-- 双窗口信用流控（_remote_rq_window_size / _sq_window_size）
```

### 建链流程（双平面）

与 RDMA / UBRing 一致，控制面为 TCP，数据面为 URMA：

1. TCP 连接建立。
2. `UrmaConnect::StartConnect` 起客户端握手 bthread。
3. 双方在 TCP fd 上交换 `UrmaHello` 消息（v2 二进制 magic `URMA`，
   v3 protobuf magic `URM3`），携带本地 EID、jetty id、recv buffer 数量、
   以及扁平化的 buffer 池 segment。
4. 双方先调用 `urma_import_seg` **再**调用 `urma_import_jetty`，为远端 EID
   建立传输路径（TP）路由。跳过 `import_seg` 会导致首个 SEND 被硬件以
   `URMA_CR_RNR_RETRY_CNT_EXC_ERR` 拒绝。
5. 4 字节 ACK（`HELLO_ACK_URMA_OK = 0x1`）确认双方均要 URMA。
6. 成功后 TCP fd 仅保留用于 epoll 生命周期和回退，数据走 URMA。

### 内存管理

申请一大段 `mmap` 内存，用 `urma_register_seg` 一次性注册，再切成固定大小
buffer（默认 8KB）。劫持 `butil::iobuf::blockmem_allocate` 使每个 IOBuf
block 都由注册 segment 支撑，发送路径可直接从 IOBuf block refs 构建
`urma_sge_t`，无需逐消息注册（与 RDMA `block_pool` 设计一致）。用户注册
内存通过 `urma::RegisterMemoryForUrma` / `DeregisterMemoryForUrma` 支持。

## 配置

所有 flag 使用 `urma_` 前缀（对标 RDMA 的 `rdma_` 前缀）：

| Flag | 默认 | 用途 |
|------|------|------|
| `--urma_use_polling` | false | 轮询 JFC 而非事件模式 |
| `--urma_poller_num` | 1 | 每 bthread tag 的轮询器数（轮询模式） |
| `--urma_disable_bthread` | false | 内联处理消息（不起 bthread） |
| `--urma_sq_size` | 128 | 本地 JFS 深度 [16, 4096] |
| `--urma_rq_size` | 128 | 本地 JFR 深度 [16, 4096] |
| `--urma_cqe_poll_once` | 32 | 每次 `urma_poll_jfc` 的上限 |
| `--urma_recv_zerocopy` | true | 大于 `--urma_zerocopy_min_size` 的接收零拷贝 |
| `--urma_copy_send` | false | 将主机发送数据复制合并到 64 字节对齐的注册缓冲区 |
| `--urma_zerocopy_min_size` | 512 | 小于此值的接收拷贝 |
| `--urma_device` | "" | URMA 设备名（空=首个） |
| `--urma_tp_type` | auto | RM 路径：auto 优先 CTP，其次 RTP；ctp 或 rtp 显式选择路径 |
| `--urma_max_sge` | 0 | 每 WR SGE 上限（0=设备上限） |
| `--urma_bonding_mode` | 0 | bonding 模式：0=standalone，1=active-backup，2=balance |
| `--urma_bonding_level` | 0 | bonding 层级：0=IODIE，1=port |
| `--urma_prepared_jetty_cnt` | 8 | 预连接 Jetty+CQ 请求数量；会根据 `RLIMIT_NOFILE` 自动限制 |
| `--urma_buffer_size` | 8192 | 池中每个 buffer 大小（字节） |
| `--urma_buffer_count` | 65536 | 池中 buffer 数量 |
| `--urma_poller_yield` | false | 忙轮询循环中主动让出 bthread |
| `--urma_client_handshake_version` | 2 | 客户端握手版本（2=二进制，3=protobuf） |

设备名以 `bonding` 开头时，brpc 会在创建 context 后、创建 segment 和队列
前配置 provider。默认 standalone+IODIE 配置与 UMDK 性能工具保持一致。
bonding 支持需要 provider 扩展头文件 `urma_ubagg.h`。

## 与 UBRing 协同

UrmaTransport 推荐用于**大包和跨节点**高吞吐路径，而 UBRing
（`SOCKET_MODE_UBRING`）对**小包和同机 IPC**最优（亚微秒、零系统调用）。
混合负载可按服务选择传输层：

| 场景 | 建议 `socket_mode` |
|------|---------------------|
| 同机 IPC | `SOCKET_MODE_UBRING` |
| 跨节点小包（< 64KB） | `SOCKET_MODE_UBRING`（UBS-Mem）或 `SOCKET_MODE_URMA` |
| 跨节点大包（>= 64KB） | `SOCKET_MODE_URMA` |
| 传统 RoCE/IB 数据中心 | `SOCKET_MODE_URMA` 或 `SOCKET_MODE_RDMA` |

单连接内按包大小自动分流的方案（路线 C 方案 B）作为后续演进方向。

## 限制

- 仅支持 `baidu_std` 协议（与 RDMA 一致）。SSL、RTMP、NSHEAD、MONGO 在
  `ContextInitOrDie` 阶段拒绝。
- 硬件数据通路需要受支持的 UMDK provider 和 `liburma`。
- 当前实现面向 Linux。

### 传输路径选择

进程初始化时，`--urma_tp_type=auto` 从设备表中选择第一个支持 CTP 的优先级，
没有 CTP 时选择第一个支持 RTP 的优先级，同时支持组合能力位。
v2/v3 握手携带所选路径，Jetty 使用对应优先级。两端需配置相同路径，
不匹配的对端路径会在导入资源前被拒绝。路径在进程生命周期内保持固定。

仅提供 RTP 的设备，两端均使用 `--urma_device=DEVICE --urma_tp_type=rtp`。
优先级是设备表索引，映射的 SL 可以不同，例如 priority 0 映射 SL 2。
brpc 按路径能力选择优先级，设备映射保持原值。初始化日志记录 `tp_type` 和 `priority`。

能力表已报告但不支持显式指定路径时，初始化沿用现有 TCP 回落流程。
未报告任何路径能力的 provider 保留兼容值：auto/ctp 使用 CTP priority 6，
rtp 使用 RTP priority 0。验收 URMA 数据通路时检查协商计数。

## 运行期故障恢复

通过进程级 API 控制新连接的传输选择：

```cpp
#include "brpc/urma/urma_recovery_policy.h"

brpc::urma::SetUrmaRecoveryCooldownMs(5000);
brpc::urma::SetUrmaRecoveryMode(brpc::urma::UrmaRecoveryMode::AUTO);
```

默认 LEGACY 沿用现有行为；AUTO 在重试耗尽、RNR、JFC 错误或 WR 提交失败后，
将客户端对端地址冷却 5 秒，期间新连接使用 TCP。TCP_ONLY 让新连接直接选择 TCP。
服务端通过 v2/v3 hello 拒绝 URMA 协商，随后处理 TCP 请求。

URMA 和 RDMA 分别维护模式、epoch、冷却缓存和指标。同一连接只上报首个运行期故障；
模式切换过滤旧 epoch 的错误。访问错误单独计数，WR flush 不重复归因，
SQ/接收 credit 耗尽保留 EAGAIN 背压，ACK 提交失败向连接错误处理传播。

`GetUrmaRecoveryStatus()` 和 `/vars/urma_recovery_status` 提供故障原因、冷却状态和协商数，
`/vars/urma_recovery_tcp_fallback_admissions` 提供 TCP 准入次数。
模式切换作用于新连接；存量连接使用已协商的传输，冷却到期后重新准入 URMA。

`example/urma_performance` 构建 `urma_recovery_client`，复用 RDMA 示例的恢复客户端源码，
通过 URMA 示例链接依赖，支持仅启用 URMA 的构建。增加 `--transport=urma` 选择 URMA，
使用 `--mode`、`--mode_file` 和 CSV 统计验证双机功能：

```bash
cmake --build build/urma-example --target urma_performance_server urma_recovery_client -j8
./build/urma-example/urma_recovery_client --transport=urma \
    --server=SERVER_IP:8003 --urma_device=URMA_DEVICE \
    --mode=auto --connection_type=short --seconds=15 --interval_ms=200
```

`build/urma-example` 使用本文编译配置生成，链接当前 URMA brpc 静态库。
先分别运行 `auto` 与 `tcp_only`，检查 RPC 成功数、URMA 协商数和 TCP 准入数。
运行中通过 `--mode_file` 指向的文件切换 `auto`、`tcp_only`，使用 short 连接观察新准入。
客户端退出码 0 表示至少一次 RPC 成功；健康基线还需确认 `failure=0`。
功能测试检查模式切换、故障计数及恢复行为，吞吐和 P99 另作性能测试。

### 定向发送失败测试

URMA 示例提供默认关闭的 Linux 注入模块，使用 `LD_PRELOAD` 在指定客户端进程中
模拟一次数据 SEND 提交失败（EIO），其余调用转发给 liburma，信用 ACK 保持原样。

```bash
cmake -S example/urma_performance -B build/urma-example -DBUILD_URMA_FAULT_INJECTOR=ON
cmake --build build/urma-example --target urma_send_failure_injector -j8
python3 example/urma_performance/run_recovery_injection.py \
    --server=SERVER_IP:8003 --device=URMA_DEVICE --tp-type=rtp
```

运行器等待成功的 URMA RPC 后创建独立标记文件，直接访问客户端的 loopback 指标端口，
并校验指标 PID。它保存 CSV、日志及基线、冷却中、恢复后三份状态快照，
检查恰好一次失败、`failures.send_post=1`、冷却期间 TCP 准入及冷却后 URMA 协商恢复。
默认 v3 握手、运行 40 秒、冷却 10 秒；启动等待单独计时。
该模块验证 SEND_POST 错误处理与恢复策略，设备完成错误需单独注入。
构建 `urma_send_failure_injector_test` 后，可通过示例目录的 CTest 执行工具自测。

## 发送访问错误对照

设置 `--urma_copy_send=true` 后，每个数据 WR 使用一个新的、64 字节对齐的注册缓冲区，
该缓冲区由发送槽位持有至完成；消息和附件内容保持一致。此模式增加一次 CPU 拷贝。
可在两端保持设备、队列、消息大小等参数一致，比较原发送路径与复制路径的结果。
启动日志显示 `copy_send`，失败日志显示完成方向及发送槽位的地址、长度和注册段范围。
