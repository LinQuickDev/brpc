# bRPC FlatBuffers 零拷贝集成与远程内存演进方案

> 文档性质：社区 RFC / Feature Proposal 初稿  
> 建议标题：**FlatBuffers Zero-Copy View and Transport-Aware Buffer Integration for bRPC**  
> 目标社区：Apache bRPC  
> 实施原则：先完善 FlatBuffers + `SingleIOBuf`，再扩展 RDMA/URMA；不在一个 PR 中同时引入序列化框架、协议和新传输层。

## 1. 摘要

bRPC 已经合入 `SingleIOBuf`，并正在推进 FlatBuffers 的消息构造和协议接入。因此，本方案不重新发明一套 FlatBuffers RPC，而是补齐以下能力：

1. 客户端直接在 bRPC 管理的连续缓冲区中构造 FlatBuffer，避免 `FlatBufferBuilder -> vector/string -> IOBuf` 的额外复制。
2. 服务端把收到的连续消息作为只读 FlatBuffers View 暴露给业务代码，避免 `IOBuf -> vector/string -> GetRoot()` 的额外复制。
3. 在进入业务方法前完成一次有边界的合法性校验，并把底层 Block 生命周期绑定到请求或异步 Closure。
4. 为现有 bRPC RDMA SEND/RECV 路径提供注册内存分配策略；无法连续分配或消息过大时安全回退。
5. 后续以独立实验特性增加 `RemoteRegion`，让大对象可以通过 RDMA/URMA 单边读取按需访问，而不是塞入普通 RPC 消息。

该方案的核心不是“完全没有序列化”，而是：FlatBuffers 仍需构造线格式，但接收端无需反序列化重建对象，并尽量让构造、传输和访问共享同一块内存。

## 2. 背景与现状

### 2.1 社区已有基础

- bRPC 1.17.0 已引入 `SingleIOBuf`，用于管理单个连续的 `IOBuf::Block`，这是 FlatBuffers 连续内存要求与 bRPC I/O 缓冲区之间的基础桥梁。
- 社区 FlatBuffers 系列工作已经规划为三步：`SingleIOBuf`、FlatBuffers 消息构造 API、FlatBuffers 协议处理。
- 因此新贡献应围绕接收 View、校验、生命周期、内存分配策略、基准测试以及 RDMA 适配展开，而不是另建一套相互竞争的接口。

### 2.2 当前实验发现的问题

现有测试覆盖 Protobuf、FlatBuffers、Cap'n Proto，包含 simple/complex 两类嵌套结构和 64 B～8 MiB payload。

本机 WSL 测试的关键现象：

- 三轮 bRPC localhost TCP 测试共 35,100 条记录，正确性失败为 0。
- complex 8 MiB 的 bRPC 路径 P50：Protobuf 约 6.69 ms，FlatBuffers 约 9.17 ms。
- 同一负载在双进程共享内存路径中，complex 8 MiB P50：Protobuf 约 7.27 ms，FlatBuffers 约 5.52 ms。
- complex 8 MiB 接收端 parse/view P50：Protobuf 约 620 us，FlatBuffers 约 4.4 us。

这说明 FlatBuffers 的只读 View 很快，但现有实验 RPC 路径仍把 FlatBuffer 放进 attachment，并在服务端复制为连续 `vector` 后访问。额外内存复制和大块分配掩盖了免反序列化收益。

上述结论是根据当前实验实现作出的工程推断，不能直接当作 bRPC 主干实现的性能结论；正式贡献必须用主干和社区正在评审的 FlatBuffers 分支重新复现。

## 3. 要解决的问题

### 3.1 功能问题

1. FlatBuffers 要求连续字节区，而普通 `IOBuf` 可能由多个 Block 组成。
2. attachment 只是非结构化字节，并不能提供类型安全的 FlatBuffers RPC 方法签名。
3. 直接 `GetRoot<T>()` 不代表数据合法；网络输入必须校验。
4. View 中的指针依赖底层消息内存，异步服务容易产生悬空引用。
5. RDMA 注册内存、普通堆内存和远程 Region 的所有权与释放方式不同。

### 3.2 性能问题

需要消除或量化以下复制：

```text
业务对象
  -> FlatBufferBuilder 内部缓冲区
  -> string/vector
  -> IOBuf
  -> Socket/RDMA 缓冲区
  -> 接收 IOBuf
  -> string/vector
  -> FlatBuffers View
```

理想的首阶段路径为：

```text
业务对象
  -> SingleIOBuf-backed MessageBuilder
  -> bRPC 协议头 + 同一数据 Block
  -> 接收侧 SingleIOBuf
  -> Verified FlatBuffers View
```

## 4. 范围与非目标

### 4.1 首版范围

- C++ 客户端和服务端。
- `baidu_std` 协议或社区当前 FlatBuffers PR 选定的协议路径。
- TCP localhost、TCP 双机和现有 bRPC RDMA SEND/RECV。
- FlatBuffers schema 生成的 simple/complex RPC。
- 同步和异步服务的内存生命周期测试。
- 64 B～8 MiB 基准与错误输入测试。

### 4.2 首版非目标

- 不替代 Protobuf；Protobuf 继续承担 IDL、控制面或兼容路径。
- 不声称发送端“免序列化”；FlatBuffers 构造本身仍有成本。
- 不在首个 PR 中实现完整 URMA 传输层。
- 不要求任意分段 `IOBuf` 都可直接成为一个 FlatBuffer。
- 不把 FlatBuffers、Cap'n Proto、URMA 和 RDMA 同时塞进一个巨型 PR。

## 5. 用户接口设计

以下接口应尽量复用社区现有 `brpc::flatbuffers::MessageBuilder` 和 `Message`，最终名称以现有 PR 为准。

### 5.1 构造与发送

```cpp
brpc::flatbuffers::BuilderOptions options;
options.initial_capacity = payload_size;
options.protocol_headroom = 64;
options.storage = brpc::flatbuffers::StoragePolicy::kAuto;

brpc::flatbuffers::MessageBuilder builder(options);
auto request = CreateRequest(builder, /* fields */);
builder.Finish(request);

brpc::flatbuffers::Message message = builder.ReleaseMessage();
stub.Exchange(&controller, &message, &response, nullptr);
```

`StoragePolicy::kAuto` 的语义：

- 普通 TCP：使用适合 `SingleIOBuf` 的连续 Block。
- RDMA 已启用且容量满足：优先从注册内存池分配。
- 无法满足时：回退普通内存或现有序列化路径，并暴露统计计数。

### 5.2 接收与访问

```cpp
void Exchange(google::protobuf::RpcController* cntl_base,
              const brpc::flatbuffers::Message* request,
              brpc::flatbuffers::Message* response,
              google::protobuf::Closure* done) override {
    brpc::ClosureGuard done_guard(done);

    auto root = request->GetVerifiedRoot<MyRequest>();
    if (!root.ok()) {
        static_cast<brpc::Controller*>(cntl_base)
            ->SetFailed(EINVAL, "invalid FlatBuffers request");
        return;
    }
    Use(root->payload());
}
```

建议增加的核心抽象：

```cpp
struct VerifyOptions {
    size_t max_message_bytes;
    size_t max_depth;
    size_t max_tables;
};

template <typename T>
StatusOr<VerifiedView<T>> GetVerifiedRoot(
    const VerifyOptions& options = {}) const;
```

`VerifiedView<T>` 同时持有：

- `const T*` 根对象；
- 底层 Block 的只读所有权引用；
- 已验证标记；
- 消息大小和可选 schema/type 标识。

不应向用户返回一个脱离所有权的裸指针。

## 6. 内部实现

### 6.1 发送端

1. `MessageBuilder` 使用现有 Slab/Block allocator 获取一个连续 Block。
2. Block 前部预留 bRPC 协议头空间，FlatBuffers 从后续位置构造。
3. `Finish()` 后冻结可写状态。
4. `ReleaseMessage()` 转移 Block 引用，不复制 payload。
5. 协议打包器只追加/引用该 Block，不调用 `to_string()` 或中间 `vector`。

必须增加调试断言或计数，确认消息打包期间没有发生 payload 字节复制。

### 6.2 接收端

1. 协议解析器识别消息类型、长度和可选 schema 标识。
2. 若 payload 已在单个连续 Block 中，直接建立 `Message`。
3. 若 payload 分段：
   - 小消息可合并到一个连续 Block；
   - 大消息默认回退并记录 `flatbuffers_receive_coalesce_bytes`；
   - 不允许把不连续内存伪装成连续 FlatBuffer。
4. 使用 `flatbuffers::Verifier` 做一次有上限校验。
5. 业务方法得到 `VerifiedView<T>`；请求完成或异步回调释放前，Block 必须存活。

### 6.3 生命周期状态

```text
Writable Builder
      |
    Finish
      v
Frozen Message ---- send/in-flight ----> Received Message
                                           |
                                         Verify
                                           v
                                      Verified View
                                           |
                                  RPC/Closure 完成后释放
```

约束：

- Frozen 后不可修改。
- View 不可跨越其 Block owner 生命周期。
- 异步保存 View 时必须显式保留 owner，而不是只保存 `const T*`。
- 同一个未声明线程安全的 builder 不得并发写。

### 6.4 协议元数据

首版建议只加入最少元数据：

```text
encoding      = flatbuffers
schema/type   = stable type id（可选）
payload_size  = N
flags         = verified / compressed / remote-region
```

不要把 C++ RTTI 名字写入线协议。类型 ID 应稳定、跨编译器，并支持版本演进。压缩与零拷贝天然冲突：启用压缩时应明确退化为解压到新缓冲区。

## 7. RDMA 与 URMA 演进

### 7.1 现有 RDMA SEND/RECV

bRPC RDMA 已经围绕 `IOBuf::Block` 和注册内存池实现零拷贝能力。FlatBuffers 可先复用该能力，但存在一个关键限制：FlatBuffer 需要一整块连续内存，而现有 RDMA 接收池常用固定大小 Block；8 MiB 消息不一定能由单个现有 Block 承载。

建议新增内部策略，而非立刻修改公开 API：

```cpp
enum class RegisteredAllocationResult {
    kRegisteredContiguous,
    kNormalContiguous,
    kSegmentedFallback,
    kRejectedTooLarge,
};
```

并提供：

- 小/中消息注册连续块池；
- 大消息按需注册或大块池，带容量上限；
- 注册失败、内存压力或超限时回退；
- 指标记录实际走到的路径。

### 7.2 后续 RemoteRegion / URMA

当 payload 很大且业务只访问少量字段时，把整个 8 MiB FlatBuffer主动发送到服务端仍不理想。后续可引入独立的远程区域描述符：

```cpp
struct RemoteRegionDescriptor {
    uint64_t region_id;
    uint64_t remote_address;
    uint64_t length;
    uint32_t access_key;
    uint32_t provider_id;   // RDMA / URMA
    uint64_t lease_id;
};
```

控制面通过普通 bRPC 传递 descriptor，数据面由 provider 执行 RDMA/URMA Read。接收端可按需拉取 FlatBuffer 的索引或数据页，并通过 lease 保证远端内存仍有效。

这一阶段需要另行解决：

- FlatBuffers 偏移访问跨远程页时的读取和缓存；
- lease、撤销、超时和断连清理；
- rkey/token 的认证与越界检查；
- 分页读取与预取策略；
- TCP fallback；
- URMA 设备能力探测和 provider 插件化。

因此 RemoteRegion 应是后续 RFC，而不是 FlatBuffers 首次集成的合入条件。

## 8. 安全与健壮性

必须包含以下保护：

- 网络输入默认验证，不能只调用 `GetRoot()`。
- 最大消息大小、最大嵌套深度和对象数量限制。
- 长度加法、偏移和对齐的溢出检查。
- schema/type 不匹配时明确失败。
- fuzz：截断、随机偏移、超大 vector、非法 vtable。
- Block 只读冻结，防止验证后修改（TOCTOU）。
- RDMA/URMA descriptor 必须校验权限、长度、租约和连接身份。
- 记录 fallback，避免“看起来是零拷贝，实际发生了合并复制”。

## 9. 可观测性

建议加入以下 bvar 或等价指标：

- `flatbuffers_requests_total`
- `flatbuffers_verify_failures_total`
- `flatbuffers_builder_reallocations_total`
- `flatbuffers_send_copy_bytes`
- `flatbuffers_receive_coalesce_bytes`
- `flatbuffers_contiguous_fast_path_total`
- `flatbuffers_fallback_total{reason}`
- `flatbuffers_registered_block_total`
- `flatbuffers_registered_allocation_failures_total`
- `flatbuffers_remote_read_bytes`（后续）

只有把复制字节数作为一等指标，基准结果才能说明是真正的零拷贝，而不是仅仅 API 名称如此。

## 10. 测试与验收

### 10.1 正确性矩阵

| 维度 | 取值 |
|---|---|
| Schema | simple、complex nested |
| Payload | 64 B～8 MiB，2 的幂 |
| Format | Protobuf、FlatBuffers；Cap'n Proto 仅作 benchmark 对照 |
| Transport | localhost TCP、双机 TCP、现有 RDMA |
| Invocation | sync、async |
| Buffer path | contiguous、segmented fallback、allocation failure |

每个组合检查：字段值、checksum、encoded bytes、错误码和生命周期。

### 10.2 性能指标

分别报告，禁止只给一个模糊的“端到端”：

- build/serialize latency；
- protocol pack latency；
- copied bytes；
- RPC round-trip latency；
- verify latency；
- first-field、sparse、full-scan access latency；
- QPS、CPU cycles、allocations、峰值内存；
- P50/P95/P99，而不只平均值。

建议首版验收目标：

1. 所有正确性组合零失败。
2. 连续快路径中不出现 payload 大小级别的 `IOBuf -> vector/string` 复制。
3. complex 8 MiB FlatBuffers RPC 相比当前 attachment 实验至少降低 20% 的客户端构造至服务端访问总耗时；最终阈值以社区 CI/测试机复测为准。
4. complex 8 MiB 服务端 view 初始化保持在微秒级，且不包含全量复制。
5. Protobuf 和普通 attachment 基准无显著回退。
6. ASan、UBSan、TSan（适用用例）及 fuzz 测试通过。

## 11. 社区贡献拆分

### PR 0：RFC 与可复现基准

- 先在 Issue/RFC 中对齐当前 #3196/#3197 的状态和接口。
- 提交 simple/complex、64 B～8 MiB benchmark。
- 增加 copied-bytes、allocation 和 verification 指标。
- 明确现有 attachment 基准不是 FlatBuffers 原生集成结果。

### PR 1：API 加固与接收 View

- 在现有 `Message` 上增加有界 verifier API。
- 定义 owner-carrying `VerifiedView<T>`。
- 补充 null root、错误 schema、截断数据和异步生命周期测试。
- 修复社区评审已发现的空字段和 descriptor 生命周期问题。

### PR 2：连续快路径

- `MessageBuilder -> SingleIOBuf -> protocol` 无中间 payload 复制。
- 接收端连续 Block 直接建立 Message/View。
- 分段数据合并与明确 fallback 指标。
- TCP benchmark 和回归测试。

### PR 3：现有 RDMA 注册内存适配

- transport-aware 内部分配器。
- 注册连续 Block 池、容量上限和失败回退。
- RDMA 双机测试；没有 RDMA 设备的 CI 使用 mock allocator。

### PR 4：实验性 RemoteRegion provider

- RDMA provider 和 URMA provider 统一接口。
- descriptor、lease、权限和远程读状态机。
- 仅在独立构建开关下启用，成熟后再讨论公共 API 稳定性。

## 12. 建议目录布局

```text
src/brpc/flatbuffers/
  message.h/.cpp
  message_builder.h/.cpp
  verified_view.h
  verifier_options.h
  block_allocator.h/.cpp

test/flatbuffers/
  message_builder_test.cpp
  verified_view_test.cpp
  malformed_message_test.cpp
  async_lifetime_test.cpp
  protocol_roundtrip_test.cpp

example/flatbuffers_c++/
  echo.fbs
  client.cpp
  server.cpp

test/benchmark/
  flatbuffers_rpc_benchmark.cpp
```

实际路径应服从 #3196/#3197 已采用的目录，避免在它们合入前制造平行实现。

## 13. 向社区提交时的说明模板

> bRPC 已有 SingleIOBuf，并正在加入 FlatBuffers message/protocol support。本提案希望在现有实现上补充 verified zero-copy receive view、明确的 buffer lifetime、copy/fallback observability，以及现有 RDMA registered-block integration。我们的初步 benchmark 显示，FlatBuffers 在 8 MiB complex 消息上的 view 初始化只需微秒级，但 attachment 路径中的整块复制会掩盖这一优势。计划先提交可复现 benchmark 和 API/lifetime tests，再分别提交 TCP contiguous fast path、RDMA registered allocator，最后以实验 RFC 讨论 URMA RemoteRegion。

## 14. 推荐的近期行动

1. 把本地 bRPC 切到最新主干，在独立分支检查 `SingleIOBuf` 实际 API。
2. 拉取或基于 #3196/#3197 分支构建，不从零复制一套 FlatBuffers service API。
3. 将现有 benchmark 改成社区 MessageBuilder/Message API，删除服务端 `IOBuf -> vector`。
4. 增加 copied-bytes 与 allocation 计数后重新跑 TCP 三轮。
5. 整理最小复现、结果表和 flame graph，先发 Discussion/Issue 征求维护者意见。
6. 获得接口方向确认后，从 PR 1 开始提交小而独立的改动。

## 15. 结论

该特性可行，但合适的社区贡献不是笼统的“给 bRPC 加 FlatBuffers”，因为基础工作已经存在。最有价值且可合入的方向是：让现有 FlatBuffers 消息真正贯通 `SingleIOBuf`、协议层和接收端只读 View；用验证、生命周期和可观测性保证它可安全用于生产；然后复用 bRPC RDMA 注册内存，最后再把 URMA/RDMA 单边远程内存作为独立演进层。

这一路线既能直接解释并改善当前 benchmark 中暴露的复制瓶颈，也能为后续“Over URMA/RDMA 通用免反序列化方案”提供稳定的消息对象和内存所有权基础。
