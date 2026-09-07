# bRPC FlatBuffers 免反序列化方案预研评审

> 文档性质：技术预研与方向评审，不是最终实现说明。  
> 当前目标：先以 FlatBuffers over TCP 验证安全的免反序列化路径，再为 RDMA/URMA 提取通用数据区域抽象。

## 1. 背景与问题

bRPC 当前主要围绕 Protobuf 组织 RPC 消息。对于大型、嵌套或只需访问少数字段的数据，接收端完整反序列化可能带来额外的 CPU、内存分配和内存带宽成本。

```text
传统路径：C++ 对象 → 序列化 → 网络 → 反序列化 → 新对象 → 业务访问
目标路径：编码缓冲区 → 网络 → 安全校验 → 类型化视图 → 按需访问
```

希望验证的问题是：

> bRPC 能否在保证输入安全和生命周期正确的前提下，让业务直接访问编码缓冲区，避免完整对象重建，并尽量减少额外复制？

本方案不是替换 Protobuf，而是为大对象和按需访问场景增加一项可选能力。

## 2. 术语和承诺边界

- **免反序列化**：不把整条消息重新构造成另一套 C++/Protobuf 对象，而是在编码缓冲区上读取字段。
- **零拷贝**：数据在链路中没有从一块内存复制到另一块内存。

二者并不等价：

```text
免反序列化 ≠ 全链路零拷贝
```

V1 建议只承诺：

- 提供 FlatBuffers 免反序列化访问；
- 连续单块缓冲区满足条件时提供零拷贝快路径；
- 分片缓冲区无法直接访问时，安全降级为合并复制；
- 不承诺网络和操作系统链路完全没有复制。

## 3. 为什么先选择 FlatBuffers

| 方案 | 接收端典型行为 | 主要特点 |
|---|---|---|
| Protobuf | 解析并构造消息对象 | 生态成熟、兼容性好 |
| FlatBuffers | 在连续编码缓冲区上建立视图 | 适合按需访问和免对象重建 |
| Cap’n Proto | 在其消息布局上建立 reader | 同样偏向原位访问，布局与生态不同 |

FlatBuffers 适合验证：

1. 编码缓冲区如何进入 bRPC；
2. bRPC 如何持有和释放底层数据；
3. 接收端如何校验并建立类型化视图；
4. 业务如何在安全生命周期内访问字段。

这不意味着 FlatBuffers 在所有消息大小和访问模式下都一定比 Protobuf 快。

## 4. 已完成的前期验证

测试覆盖：

- Protobuf、FlatBuffers、Cap’n Proto；
- simple/complex 两类嵌套结构；
- 64B～8MiB payload；
- checksum 正确性校验；
- 多轮重复运行。

| 测试层次 | 目的 | 主要测量项 |
|---|---|---|
| 单进程 | 分离格式自身成本 | 序列化、解析/视图、部分访问、完整访问、编码大小 |
| 双进程共享内存 | 分离生产端与消费端 | producer 序列化、区域发布、consumer 解析/视图和访问 |
| 完整 bRPC TCP RPC | 验证真实 RPC 链路 | 客户端序列化、RPC 往返、服务端解析/视图和访问 |

当前可以得出的结论：

- 三种格式均完成上述范围的正确性测试；
- FlatBuffers 和 Cap’n Proto 可避免传统的完整对象重建；
- FlatBuffers 的潜在优势主要在接收端视图建立和按需访问；
- 小消息中，RPC 固定开销可能掩盖格式差异；
- 大消息中，复制、缓冲区连续性和完整扫描成本更重要；
- 正式评估不能只使用一个端到端耗时，还需要 P50/P95/P99、吞吐量、CPU、内存和复制字节数。

当前数据属于预研基线，不能解释为 FlatBuffers 在所有场景下已经证明更快。

## 5. RMMap 论文带来的启发

本方案借鉴的是“数据区域与访问视图分离”的思想，而不是直接照搬论文实现：

1. 不在收到数据后无条件复制并重建完整对象；
2. 用 region 描述地址、长度、权限、所有权和生命周期；
3. 在 region 上建立经过验证的类型化视图；
4. 根据访问模式直接访问或按需获取数据；
5. 让本地缓冲区、RDMA 注册内存和 URMA 区域尽量共享上层语义。

```text
FlatBuffers：第一种类型化格式
TCP IOBuf：第一种 region 数据来源
RDMA/URMA：后续 region 的远端传输与访问实现
```

格式与传输需要解耦：FlatBuffers 不应直接依赖 RDMA，RDMA/URMA region 也不应只服务于 FlatBuffers。

## 6. 为什么调研 PR #3196 和 #3197

Apache bRPC 社区此前已经尝试过接入 FlatBuffers。

### PR #3196：消息构造与 IOBuf 集成

主要包含 `MessageBuilder`、消息包装、`ReleaseMessage()`、`ParseFbFromIOBUF()`、`SerializeFbToIOBUF()` 及 FlatBuffers buffer 与 IOBuf 的衔接。

它回答的是：

> FlatBuffers 消息如何在 bRPC 内存体系中构造、持有、发送和访问？

### PR #3197：完整 RPC protocol

主要涉及协议注册、Channel、Controller、Server、请求打包、响应解析、service/method descriptor、客户端 stub 和服务端分发。

它回答的是：

> FlatBuffers 消息如何进入完整的 bRPC 请求/响应链路？

```text
#3196：消息怎样构造、持有和访问
                  ↓
#3197：消息怎样通过完整 RPC 链路
```

调研意义：

- 避免重复实现已经探索过的机制；
- 找到非 Protobuf 格式接入 bRPC 的真实修改边界；
- 验证连续缓冲区上的免反序列化路径可行；
- 发现输入验证、生命周期、分片和类型安全风险。

它们不能直接作为最终实现，因为其落后当前主线、缺少完整测试和安全约束，部分接口还假定 method descriptor 为 Protobuf 类型。

## 7. 建议的总体设计

```text
┌───────────────────────────────────────────────────────┐
│                    业务层                             │
│             VerifiedView<T> / 字段访问               │
└─────────────────────────┬─────────────────────────────┘
                          │ 持有生命周期
┌─────────────────────────▼─────────────────────────────┐
│            SerializedRegion / OwnedRegion            │
│  地址、长度、连续性、所有权、权限、释放策略、注册信息 │
└─────────────────────────┬─────────────────────────────┘
                          │ 由适配器提供
┌─────────────────────────▼─────────────────────────────┐
│                   Transport Adapter                   │
│      TCP IOBuf │ 共享内存 │ RDMA region │ URMA region │
└───────────────────────────────────────────────────────┘
```

### SerializedRegion

描述地址、长度、连续/分片状态、读写属性、所有权、引用计数和释放策略；未来可扩展 RDMA/URMA 注册句柄和访问权限。

### VerifiedView<T>

持有或引用 region，先使用 `flatbuffers::Verifier` 校验不可信输入，成功后才允许 `GetRoot<T>()`。

必须满足：

```text
View 有效期 ≤ Region 有效期
```

### Transport Adapter

把 TCP IOBuf、共享内存、RDMA 或 URMA 数据转化为统一 region，使格式不绑定具体传输。

## 8. 当前原型状态

当前原型分支：`prototype/flatbuffers-builder-port`。

已经完成：

- 移植 PR #3196 的 builder/message 基础，并形成独立原型提交；
- 将 PR #3197 的 protocol 改动应用到当前主线；
- 解决旧代码与当前主线的冲突；
- bRPC 静态库成功编译；
- FlatBuffers 关键符号进入 `libbrpc.a`；
- FlatBuffers 示例客户端和服务端成功构建。

尚未完成：

- 示例 RPC 的完整运行和字段级一致性验证；
- 服务端和客户端强制 `Verifier`；
- 类型安全的非 Protobuf method descriptor 接口；
- view 生命周期和异步持有规则；
- 分片 IOBuf 的复制降级测试；
- 异常报文、并发、超时、取消和重试测试；
- 正式性能基准；
- RDMA/URMA 设备验证。

当前结论：

> 已经得到可编译的 FlatBuffers builder/protocol 原型，用于证明接入路径可行；尚未形成可以提交给社区的正式功能实现。

## 9. 已确认的关键限制

### IOBuf 连续性

当前 `SingleIOBuf::assign()` 路径表明：

- 消息完整位于一个连续 IOBuf block 时，可以直接引用；
- 消息跨多个 block 时，为满足 FlatBuffers 连续布局要求，可能需要合并复制。

### 输入安全

`GetRoot()` 不等于完整校验。正式实现必须在业务访问前执行 `Verifier`，并限制消息大小、嵌套深度、vector/string 边界。

### 类型安全

当前原型存在 descriptor 类型的临时兼容处理。正式设计需要独立的类型安全回调或带明确类型标签的通用描述符，不能保留不安全转换。

### 生命周期

FlatBuffers 对象是指向底层字节的视图。跨 RPC 回调持有必须显式取得共享所有权或复制数据。

## 10. 分阶段实施建议

### V1：FlatBuffers over TCP

- FlatBuffers 作为可选依赖；
- builder/message/view API；
- 类型安全的 method descriptor 与调用路径；
- 接收端强制 `Verifier`；
- 明确 region/view 生命周期；
- 单 block 零拷贝快路径；
- 多 block 安全合并降级；
- 正确性、安全性和性能测试。

V1 可在本地 WSL 完成，不需要 RDMA 设备。

### V2：通用 SerializedRegion

- 从 FlatBuffers 专用实现提取通用区域抽象；
- 支持连续和分片数据描述；
- 提供复制次数、复制字节数和降级原因观测；
- 让不同格式共享所有权和传输接口。

### V3：RDMA/URMA

- 注册内存、region handle、远端地址和访问权限；
- region lease、撤销、超时和断连处理；
- 按需读取策略；
- 并发和重试安全；
- 在具有 RDMA/URMA 设备的原生 Linux 环境测试。

## 11. 正式测试门槛建议

- **正确性**：simple/complex、嵌套对象、空字段、vector/string、64B～8MiB、请求响应字段级校验；
- **安全性**：截断消息、非法 offset/长度、超大消息、深层嵌套、校验失败阻断、fuzz；
- **生命周期**：同步/异步、超时、取消、重试、并发、引用与释放次数；
- **性能**：序列化、parse/view、部分/完整访问、P50/P95/P99、吞吐量、CPU、内存、复制次数和字节数；
- **缓冲区布局**：单 block 与多 block 分别测试。

## 12. 希望本次会议确认的事项

1. 是否认可“FlatBuffers over TCP → 通用 Region → RDMA/URMA”的路线？
2. V1 是否只承诺免反序列化和条件式零拷贝快路径？
3. FlatBuffers 应作为独立 protocol，还是 attachment 上的类型化 view？
4. 是否接受 FlatBuffers 为可选依赖，默认不影响现有用户？
5. 接收端是否必须执行 `Verifier`？
6. V1 是否允许多分片输入合并复制？
7. view 能否跨 RPC 回调持有，应采用何种所有权接口？
8. 历史 PR 中哪些机制保留，哪些接口重写？
9. 哪些测试和性能指标作为合入门槛？
10. RDMA/URMA 是否只进入当前接口约束，推迟到后续版本实现？

## 13. 建议结论

暂不把当前历史 PR 移植代码作为正式功能提交。建议先确认 V1 边界，完成类型安全接口、强制校验、生命周期规则及 TCP 测试，再根据证据提取通用 region，最后进入 RDMA/URMA 验证。

> 先把 FlatBuffers over TCP 做成安全、可测试、可维护的免反序列化能力；证据充分后，再把数据区域抽象推广到 RDMA/URMA。

## 14. 相关链接

- 内部方案 PR：<https://github.com/LinQuickDev/brpc/pull/44>
- Apache bRPC：<https://github.com/apache/brpc>
- 历史 builder PR：<https://github.com/apache/brpc/pull/3196>
- 历史 protocol PR：<https://github.com/apache/brpc/pull/3197>
- FlatBuffers：<https://github.com/google/flatbuffers>
- Cap’n Proto：<https://github.com/capnproto/capnproto>

