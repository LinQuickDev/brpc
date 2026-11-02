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
（`URMA_TM_RM`）Jetty，并使用 CTP 传输路径；通过
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
| `--urma_zerocopy_min_size` | 512 | 小于此值的接收拷贝 |
| `--urma_device` | "" | URMA 设备名（空=首个） |
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


## 可观测性与诊断

URMA 通过已有 `/vars` 和 Prometheus 导出进程级 `urma_*` 指标，通过已有
内置服务入口提供连接快照。设置 `internal_port` 时使用内网端口，沿用现有
内置服务访问控制。

```sh
curl http://127.0.0.1:8003/sockets/urma
curl 'http://127.0.0.1:8003/sockets/123?format=json'
python3 tools/urma_diag.py --server 127.0.0.1:8003 connections
python3 tools/urma_diag.py --server 127.0.0.1:8003 pool
python3 tools/urma_diag.py --server 127.0.0.1:8003 socket 123
python3 tools/urma_diag.py --server 127.0.0.1:8003 --json watch --interval 1 --count 10
python3 tools/urma_diag.py --server 127.0.0.1:8003 trace 123 --trace-id 456
```

CLI 仅依赖 Python 3 标准库。列表最多返回 1024 条连接并输出 `truncated`；
按 SocketId 查询不受列表上限影响。ID 用十进制 JSON 字符串保存，避免 64 位
精度丢失。列表 `schema_version` 为 1。

连接快照包括建链/回退状态和原因、实际协商窗口容量、剩余 SQ/远端 credit、
数据 WR 提交与完成、在途字节、错误及两种资源等待。进程指标带 `urma_` 前缀，
连接快照的 `counters` 使用不带前缀的名称。

* `writes`、`bulk_writes`、`bulk_write_bytes`：进入已建立 URMA 数据路径的完整
  序列化写入。`urma_bulk_min_bytes` 默认 65536 字节，非正值关闭分类。大小包括
  协议头和附件；写入、WR 和业务 RPC 的数量不能混用。
* `send_bytes` / `send_wrs` 与 `send_completed_bytes` / `send_completed_wrs`
  分别表示数据提交和本地完成。提交失败会回滚；本地完成不等于远端业务已处理。
* `sq_blocked` / `remote_credit_blocked` 统计尝试发送时窗口为零的观察次数；
  `sq_wait_us` / `remote_credit_wait_us` 按 butex 等待入口观察到的原因归因。
  两者可能重叠，不能作为互斥阶段相加。
* `urma_pool_capacity`、`urma_pool_buffer_size`、`urma_pool_in_use` 和
  `urma_pool_peak_in_use` 统计注册 BufferPool。IOBuf 引用和线程缓存可能在 RPC
  结束后继续持有 buffer。`urma_pool_exhausted` 和 `urma_malloc_fallback` 统计池耗尽
  及分配回退尝试，后者还包括超大分配；分配回退不等于连接回退 TCP。

开启 rpcz 并设置 `urma_trace_sample_every=N`，按 span ID 每 N 条采样一条；
默认 0 关闭，1 跟踪所有可用 rpcz span，仍受 rpcz 自身采样影响。客户端请求和
baidu_std 服务端响应显式携带 Span 进入写队列，不依赖 KeepWrite 的当前 TLS。
支持一条 RPC 拆成多个 WR、一个 WR 合并多个请求数据，纯 credit WR 不计入。
旁路关联表仅用于采样写入，保持 `Socket::WriteRequest` 为 64 字节。

排队、首次提交和完成/失败事件写入 rpcz 注释。完成可能晚于 rpcz 序列化，
不会为观测延迟 RPC 完成或 rpcz 收集，因此每连接另保留最近 64 条已结束采样，
包含 trace/span/log ID，供 CLI 查询。`path=no_urma_wr` 表示没有提交 URMA 数据 WR，
例如走了 TCP 回退。历史在连接重置或销毁时清除。这些时间描述本地传输进度，
不能直接当作远端处理耗时或纯链路延迟。

快照是各原子字段的并发观察，不保证所有字段来自同一瞬间。Endpoint 重用时
连接计数清零且 `generation` 递增，进程指标累计保留；CLI 不跨 generation 计算速率。
完整指标口径与验证说明见 [英文文档](../en/urma.md#observability-and-diagnostics)。

使用 URMA mock 构建后运行 `brpc_urma_unittest`、
`brpc_urma_observability_unittest` 和 `python3 test/urma_diag_test.py`；同时检查
`WITH_URMA=OFF` 构建。上线前需在真实硬件上比较关闭跟踪及代表性采样率下的
吞吐、CPU 和尾延迟；mock 测试不能代替硬件性能验证。
