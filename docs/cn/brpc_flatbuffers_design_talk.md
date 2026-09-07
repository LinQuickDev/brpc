# bRPC FlatBuffers 免反序列化与面向 RDMA/URMA 的通用数据视图方案

## 1. 本次会议希望解决什么

本次会议不是评审一份已经完成的最终代码，也不是立即决定完整的 RDMA/URMA 实现，而是希望确认第一阶段方案的方向和边界：

1. 是否值得在 bRPC 中支持 FlatBuffers 免反序列化访问；
2. 第一阶段是否应先完成 FlatBuffers over TCP；
3. FlatBuffers 应以独立 RPC protocol 还是 attachment 数据视图的形式接入；
4. 怎样保证数据校验、内存所有权和访问生命周期安全；
5. 怎样为后续 RDMA/URMA 的远端内存访问留下通用扩展接口。

一句话概括方案：

> 为 bRPC 增加一种以 FlatBuffers 为首个落地对象、面向 TCP/RDMA/URMA 演进的免反序列化数据访问能力，减少大对象 RPC 中的编解码和内存复制开销。

这里优先使用“免反序列化访问”这一表述，而不直接承诺“全链路零拷贝”。是否发生复制还取决于 IOBuf 分片、网络接收布局和缓冲区生命周期。

---

## 2. 背景与问题

bRPC 当前主要围绕 Protobuf 构建 RPC 接口。传统处理流程通常是：

```text
业务对象
  → Protobuf 序列化
  → 网络传输
  → Protobuf 反序列化
  → 构造接收端对象
  → 业务访问
```

在普通小请求中，这套机制成熟且易用。但在 AI Infra、张量、大型嵌套结构体和大对象传输场景中，可能产生以下成本：

- 发送端遍历对象并编码；
- 接收端重新解析并构造对象；
- 网络缓冲区、连续缓冲区和业务对象之间发生内存复制；
- 大对象占用较多 CPU 时间和内存带宽；
- 业务只访问少数字段时，完整反序列化可能做了大量无效工作。

因此要研究的问题是：

> 当收到的数据本身已经具有可直接访问的布局时，bRPC 能否安全地向业务提供一个数据视图，而不是立即把整条消息还原成另一套 C++ 对象？

本方案不是替换 Protobuf。Protobuf 继续服务于兼容性和通用 RPC 场景；FlatBuffers 作为可选能力，服务于对大对象、按需访问和内存复制成本敏感的场景。

---

## 3. 为什么先选择 FlatBuffers

FlatBuffers 将表、向量和字符串组织在一个可随机访问的编码缓冲区中。接收端在完成边界和结构校验后，可以直接通过生成的访问器读取字段，不必把整个消息重新构造成普通对象。

三种已测试方案的定位如下：

| 方案 | 接收端典型行为 | 主要特点 |
|---|---|---|
| Protobuf | 解析字节并构造消息对象 | 生态成熟、兼容性好 |
| FlatBuffers | 在连续编码缓冲区上建立视图 | 适合免反序列化和按需访问 |
| Cap’n Proto | 在其消息布局上建立 reader | 同样偏向原位访问，但布局及生态不同 |

选择 FlatBuffers 作为第一个实现，不表示它在所有场景中一定最快。它适合验证三个关键问题：

- bRPC 怎样持有编码缓冲区；
- 数据视图的生命周期怎样与缓冲区绑定；
- 业务怎样在不构造完整对象的情况下安全访问字段。

---

## 4. 前期完成的测试

测试统一覆盖：

- Protobuf、FlatBuffers、Cap’n Proto；
- simple 和 complex 两类结构；
- 64B 至 8MiB 的 payload；
- checksum 正确性校验；
- 多轮重复运行。

### 4.1 单进程序列化框架基准

在同一进程中分别记录：

- 序列化时间；
- 编码后大小；
- 数据复制时间；
- 反序列化或视图初始化时间；
- 部分字段访问时间；
- 完整访问时间。

这组测试主要用于分离“格式本身的成本”和“网络传输成本”。

### 4.2 独立生产者与消费者测试

使用两个独立进程和共享内存区域：

- producer 只负责构造、序列化并发布数据；
- consumer 只负责解析或建立视图、访问数据并校验；
- 分开记录生产端和消费端的耗时。

这组测试用于模拟发送端与接收端分离的情形。共享内存不是 RDMA，但可以先验证数据区域、发布状态和视图生命周期等机制。

### 4.3 完整 bRPC TCP RPC 测试

客户端构造三种格式的数据，通过 bRPC 发给服务端。服务端解析或建立视图、访问字段、计算 checksum，并通过 RPC 返回结果。

这组测试用于验证格式进入真实 RPC 链路后，序列化成本、RPC 往返成本、服务端解析或视图建立成本和访问成本之间的关系。

### 4.4 当前测试得到的启发

- 三种格式均能覆盖 simple/complex 和 64B～8MiB 数据，并通过正确性校验；
- FlatBuffers 和 Cap’n Proto 的接收端可以避免传统的完整对象重建；
- FlatBuffers 的主要潜在收益位于接收端视图建立和按需访问，而不是保证所有端到端场景都更快；
- 小消息中，RPC 固定开销可能掩盖序列化差异；
- 大消息中，内存复制、缓冲区连续性和完整扫描成本变得更加重要；
- 因此正式测试必须同时报告序列化、解析或视图初始化、访问、复制和 RPC 往返时间，不能只报告一个总耗时。

---

## 5. RMMap 论文对方案的启发

论文带来的核心启发不是直接照搬其全部实现，而是把“数据所在区域”和“业务访问方式”分开：

1. 不要在收到数据后无条件复制并重建完整对象；
2. 用数据区域抽象保存地址、长度、权限、所有权和生命周期；
3. 在区域之上建立经过验证的类型化视图；
4. 根据业务访问行为直接访问或按需获取数据；
5. 让本地缓冲区、RDMA 注册内存和 URMA 区域尽量共享上层访问模型。

对应到 bRPC：

```text
FlatBuffers 是第一种格式
TCP IOBuf 是第一种本地数据区域来源
RDMA/URMA 是后续的数据区域传输与访问方式
```

因此，FlatBuffers 不应直接依赖 RDMA；RDMA/URMA 的区域抽象也不应只服务于 FlatBuffers。

---

## 6. 为什么调研 Apache bRPC PR #3196 和 #3197

Apache bRPC 社区此前已经尝试过接入 FlatBuffers。调研这两个历史 PR 的目的不是直接复制旧代码，而是确认社区曾经怎样拆分问题、哪些机制可以复用，以及哪些设计在当前主线上已经不适用。

### 6.1 PR #3196：FlatBuffers 消息构造和内存管理

该 PR 主要引入：

- FlatBuffers `MessageBuilder`；
- FlatBuffers 消息包装对象；
- `ReleaseMessage()`；
- `ParseFbFromIOBUF()`；
- `SerializeFbToIOBUF()`；
- FlatBuffers buffer 与 `butil::IOBuf` 的衔接。

它回答的是：

> FlatBuffers 消息怎样在 bRPC 的内存体系中构造、持有、发送和访问？

这是消息构造和数据面的基础。

### 6.2 PR #3197：完整 RPC 协议接入

该 PR 主要涉及：

- 新增 FlatBuffers RPC protocol；
- 协议注册；
- Channel、Controller 和 Server 调用路径；
- 请求打包和响应解析；
- FlatBuffers service/method descriptor；
- 客户端 stub 与服务端 service 分发。

它回答的是：

> FlatBuffers 消息怎样进入 bRPC 完整的请求和响应链路？

两个 PR 的关系是：

```text
PR #3196：消息怎样构造、保存和访问
                    ↓
PR #3197：消息怎样进入完整的 bRPC RPC 链路
```

### 6.3 调研的实际意义

- 避免重复实现 builder、IOBuf 适配和协议注册等机制；
- 找到非 Protobuf 格式接入 bRPC 时真正需要修改的边界；
- 验证连续缓冲区上的免反序列化路径在工程上可行；
- 发现 bRPC 某些接口仍强依赖 Protobuf descriptor；
- 提前发现输入验证、生命周期、分片和构建依赖等风险；
- 为是否基于旧实现继续演进提供代码依据。

一句话总结：

> #3196 和 #3197 帮助我们确认“能不能做”和“要改哪里”；当前工作要进一步解决“怎样安全、可维护、可验证地做”。

---

## 7. 当前原型进展

当前在 `prototype/flatbuffers-builder-port` 分支进行原型验证。

已经完成：

- 将 PR #3196 的 builder/message 机制移植到当前主线；
- builder 部分已经形成独立原型提交；
- 将 PR #3197 的 protocol 改动应用到当前主线；
- 解决旧 PR 与当前主线之间的冲突；
- 统一 FlatBuffers 相关命名空间；
- 重新生成与当前 FlatBuffers 版本兼容的示例代码；
- bRPC 静态库成功编译；
- `MessageBuilder::ReleaseMessage()`、`ParseFbFromIOBUF()` 和 `SerializeFbToIOBUF()` 已进入 `libbrpc.a`；
- FlatBuffers 示例客户端和服务端已经成功构建。

尚未完成：

- protocol 移植代码尚未整理成正式提交；
- 示例 RPC 运行验证尚未完成；
- 还没有完整的请求和响应数据一致性测试；
- 服务端尚未形成强制 `Verifier` 的安全路径；
- 当前存在原型性质的 descriptor 类型兼容处理；
- 尚未补齐单元测试、异常报文测试和性能基准；
- 尚未在 RDMA/URMA 设备上测试。

因此当前状态应描述为：

> FlatBuffers builder 和 protocol 的可编译原型已经打通，但尚未达到可以向社区提交正式功能代码的程度。

---

## 8. 已确认的技术边界

### 8.1 免反序列化不等于全链路零拷贝

FlatBuffers 可以让业务直接读取编码缓冲区，但不保证数据从网卡到业务访问之间完全不发生复制。

当前 `SingleIOBuf::assign()` 的行为表明：

- 数据位于单个连续 IOBuf block 中时，可以直接引用该区域；
- 数据跨多个 IOBuf block 时，为满足 FlatBuffers 连续布局要求，可能需要申请连续内存并合并复制。

所以准确的承诺应该是：

> 提供免反序列化访问，并在连续缓冲区条件满足时提供零拷贝快路径；分片情况下允许安全降级为合并复制。

### 8.2 数据安全验证

`flatbuffers::GetRoot()` 本身不等于完成不可信输入校验。网络输入应先通过 `flatbuffers::Verifier`，验证成功后才能暴露类型化视图。

还需要限制：

- 最大消息长度；
- 嵌套深度；
- vector 和 string 的边界；
- 截断和畸形报文；
- 校验失败后的错误传播。

### 8.3 生命周期

FlatBuffers 对象只是指向底层字节的视图。如果 IOBuf、共享区域或 RDMA region 被提前释放，视图会悬空。

因此 API 必须保证：

```text
View 的有效期 ≤ Region/Buffer 的有效期
```

业务若要在 RPC 回调结束后继续持有数据，必须显式取得共享所有权或复制数据。

### 8.4 类型安全

当前 bRPC 的部分协议接口假定 method descriptor 是 Protobuf 类型。原型中为了验证链路存在临时类型兼容处理，但正式方案不能依赖不安全的强制类型转换。

正式实现应采用：

- 独立的 FlatBuffers pack/dispatch 回调；或
- 明确带类型标签的通用方法描述符；或
- 更上层的统一 RPC method abstraction。

---

## 9. 建议的核心抽象

总体路径：

```text
业务数据
   ↓
FlatBuffers Builder
   ↓
Encoded Buffer / IOBuf
   ↓
bRPC Transport（TCP → RDMA/URMA）
   ↓
Owned Serialized Region
   ↓
Verified Typed View
   ↓
业务按需访问字段
```

### 9.1 SerializedRegion：数据区域

负责描述：

- 数据地址与长度；
- 连续或分片状态；
- 只读或可写属性；
- 内存所有权；
- 引用计数和生命周期；
- 释放回调；
- 后续可扩展的 RDMA/URMA 注册信息和访问句柄。

### 9.2 VerifiedView：验证后的类型化视图

负责：

- 持有或引用 `SerializedRegion`；
- 执行格式校验；
- 在验证成功后提供 `GetRoot<T>()`；
- 保证访问期间底层区域有效；
- 禁止未经验证的不可信网络数据直接暴露给业务。

### 9.3 Transport Adapter：传输适配层

负责把不同来源的数据转化为统一区域：

- TCP `IOBuf`；
- 共享内存；
- RDMA 注册内存；
- URMA 内存区域。

格式与传输解耦后，FlatBuffers 可以运行在 TCP、RDMA 或 URMA 上；其他适合原位访问的格式也可以复用相同的区域和生命周期机制。

---

## 10. 分阶段实施建议

### V1：FlatBuffers over TCP

目标是先做出安全、可测试、可维护的 FlatBuffers RPC 能力：

- FlatBuffers 作为可选依赖，默认不影响现有用户；
- 提供 builder/message/view API；
- 使用类型安全的 FlatBuffers method descriptor；
- 接收端默认或强制执行 `Verifier`；
- 明确 view 与 IOBuf 的生命周期关系；
- 单 block 时走直接引用快路径；
- 多 block 时允许合并复制并记录降级；
- 增加正确性、安全性和性能测试。

### V2：提取通用 SerializedRegion

- 从 FlatBuffers 专用实现中提取传输无关的数据区域；
- 支持连续与分片区域描述；
- 增加复制次数、复制字节数和降级原因观测；
- 让不同序列化格式共享区域所有权和传输接口；
- 评估 scatter/gather 与连续视图之间的边界。

### V3：接入 RDMA/URMA

- 注册内存与 region handle；
- 远端地址、访问权限和 rkey 等元数据；
- region lease 和撤销机制；
- 按需读取或直接映射策略；
- 超时、断连、重试和并发安全；
- 本地、TCP、RDMA 和 URMA 路径的统一语义。

V1 不需要 RDMA 环境；V3 才需要在具有 RDMA/URMA 设备的原生 Linux 服务器上验证。

---

## 11. 正式实现需要补齐的测试

### 正确性测试

- simple/complex 结构；
- 空字段、可选字段、字符串、vector 和嵌套对象；
- 64B～8MiB；
- 请求和响应双向校验；
- checksum 与字段级一致性。

### 安全性测试

- 截断 buffer；
- 错误 root offset；
- 非法 vector/string 长度；
- 超大消息；
- 校验失败后不得调用业务方法；
- fuzz 测试。

### 生命周期测试

- RPC 回调期间访问；
- 回调结束后的非法访问防护；
- 异步回调；
- 超时、取消和重试；
- 并发请求；
- buffer 释放次数和引用计数。

### 性能测试

- 序列化时间；
- parse/view 初始化时间；
- 部分访问与完整访问时间；
- RPC latency 的 P50/P95/P99；
- throughput；
- CPU 使用率；
- 内存峰值；
- 复制次数与复制字节数；
- 单 block 与多 block 的差异。

---

## 12. 当前主要风险

1. 历史 PR 与当前主线差距较大，不能直接作为正式实现；
2. bRPC 现有接口存在 Protobuf 类型假设；
3. FlatBuffers 需要连续数据，IOBuf 分片可能触发复制；
4. 未经 `Verifier` 的网络数据存在安全风险；
5. view 生命周期处理不当会产生悬空指针；
6. FlatBuffers 版本和生成代码需要明确兼容策略；
7. 引入依赖不能影响默认 bRPC 构建；
8. TCP 原型结果不能直接等价于 RDMA/URMA 效果。

---

## 13. 希望会议形成的决策

建议围绕以下问题逐项确认：

1. 是否接受“先 FlatBuffers over TCP，再抽象 RDMA/URMA”的实施顺序？
2. V1 是否只承诺免反序列化和条件式零拷贝快路径？
3. FlatBuffers 应作为独立 bRPC protocol，还是作为 attachment 上的类型化视图？
4. 是否接受 FlatBuffers 为可选编译依赖？
5. 接收端是否必须默认启用 `Verifier`？
6. 多分片输入第一版是否允许合并复制？
7. view 是否允许跨 RPC 回调持有；如果允许，采用什么所有权接口？
8. 当前历史 PR 原型中哪些部分可以复用，哪些部分必须重写？
9. 哪些 benchmark 和安全测试应作为合入门槛？
10. RDMA/URMA 是否纳入当前接口设计，但推迟到后续版本实现？

---

## 14. 建议的现场串讲话术

### 开场

> 今天想讨论的是 bRPC 在大对象 RPC 场景中的免反序列化能力。我们并不是要替换 Protobuf，也不是今天就实现完整 RDMA/URMA，而是希望先以 FlatBuffers over TCP 验证一个安全的数据区域和类型化视图模型。

### 介绍问题

> 当网络数据已经具有可直接访问的布局时，如果接收端仍然把整条消息重新构造成一套对象，会额外消耗 CPU 和内存带宽。对于大对象或只访问少数字段的业务，这部分成本可能是不必要的。

### 介绍已有验证

> 前期分别完成了单进程、共享内存双进程和完整 bRPC TCP 三层 benchmark，覆盖 Protobuf、FlatBuffers、Cap’n Proto，包含 simple/complex 结构和 64B 到 8MiB。测试说明免反序列化视图具备价值，但收益依赖消息大小和访问方式，不能简单概括为 FlatBuffers 在所有场景都更快。

### 介绍历史 PR

> 社区以前的 PR #3196 解决消息构造和 IOBuf 集成，PR #3197 解决完整 RPC 协议接入。它们证明了方向可行，也帮助我们定位修改边界；但旧代码缺少完整校验、生命周期约束和测试，并且部分接口仍建立在 Protobuf 类型假设上，所以不能直接作为最终方案。

### 介绍方案

> 正式方案希望拆成数据区域、验证视图和传输适配三层。FlatBuffers 只负责格式和访问器，IOBuf 或 RDMA/URMA 负责承载数据，region 负责所有权和生命周期。这样 FlatBuffers 与传输方式不会相互绑定。

### 说明零拷贝边界

> 当前分析确认，单个连续 IOBuf block 可以直接引用；跨 block 时 FlatBuffers 由于连续布局要求可能需要合并复制。因此第一阶段应该承诺免反序列化，并将零拷贝定义为满足连续性条件时的快路径。

### 结束

> 今天希望先确认 V1 的技术边界和 API 方向。如果认可，我们会把当前可编译原型整理成类型安全、默认校验、可选编译并具有完整测试的 FlatBuffers over TCP 实现，然后再提取通用 region，最后进入 RDMA/URMA 验证。

---

## 15. 可能被问到的问题

### Q1：FlatBuffers 就是零拷贝吗？

不是。FlatBuffers 可以免去传统反序列化对象重建，但网络接收、IOBuf 分片合并、发送系统调用和设备传输仍可能发生复制。

### Q2：为什么不直接做 RDMA/URMA？

TCP 环境更容易调试，适合先验证 API、校验、所有权和生命周期。直接进入 RDMA/URMA 会把格式、协议、设备、内存注册和并发问题混在一起。

### Q3：为什么不只使用 attachment？

attachment 是成本较低的实验路径，但无法自然提供类型化 service/method、自动校验和一致的调用接口。是否采用 attachment 或独立 protocol，正是本次评审需要决定的问题。

### Q4：为什么还要保留 Protobuf？

Protobuf 生态成熟、兼容性强，对小消息和普通 RPC 很合适。本特性是可选的补充能力，不应破坏现有协议和用户代码。

### Q5：历史 PR 能直接合并吗？

不能。它们落后当前主线，缺少测试和安全约束，并暴露出 descriptor 类型安全问题。它们更适合作为原型基础和设计证据。

### Q6：什么时候需要 RDMA/URMA 服务器？

V1 FlatBuffers over TCP 在本地 WSL 即可完成。进入 V3 的注册内存、远端区域访问和真实传输性能测试时，必须使用具有 RDMA/URMA 设备的原生 Linux 环境。

---

## 16. 当前暂停点

- 工作分支：`prototype/flatbuffers-builder-port`；
- builder 移植已经形成独立提交；
- protocol 移植已经解决冲突并完成 bRPC 静态库编译；
- protocol 改动仍处于暂存状态，尚未形成正式提交；
- FlatBuffers 示例已经构建，但尚未完成运行和数据一致性验证；
- 暂停现场已保存为 Git 状态和 staged/unstaged diff；
- 当前没有 FlatBuffers 客户端或服务端进程运行。

在评审明确方案边界前，不建议把当前 protocol 原型作为正式功能代码提交。

---

## 17. 相关材料

- 内部设计初稿：<https://github.com/LinQuickDev/brpc/pull/44>
- Apache bRPC：<https://github.com/apache/brpc>
- 历史 FlatBuffers builder PR：<https://github.com/apache/brpc/pull/3196>
- 历史 FlatBuffers protocol PR：<https://github.com/apache/brpc/pull/3197>
- FlatBuffers：<https://github.com/google/flatbuffers>
- Cap’n Proto：<https://github.com/capnproto/capnproto>

