# Protobuf / FlatBuffers / Cap'n Proto 测试流程与结果汇总

## 1. 测试目标

本项目比较以下三种序列化方案：

- Protobuf 3.21.12
- FlatBuffers 25.12.19
- Cap'n Proto 1.5.0

测试目标是为 bRPC 社区设计一套面向 TCP、RDMA/URMA 的通用免反序列化/低复制数据传输方案，并回答以下问题：

1. 三种格式单独进行序列化时的成本有什么区别？
2. Protobuf 反序列化与 FlatBuffers/Cap'n Proto 建立只读视图的成本有什么区别？
3. 将生产者和消费者分成两个进程后，结果是否仍然成立？
4. 接入完整 bRPC TCP 请求—响应流程后，额外的数据复制会带来什么影响？
5. 哪种格式更适合作为 bRPC 免反序列化特性的第一阶段实现？

## 2. 测试环境

| 项目 | 环境 |
|---|---|
| 操作系统 | Ubuntu 24.04 on WSL2 |
| 内核 | 6.18.33.2-microsoft-standard-WSL2 |
| 编译器 | GCC 13.3.0 |
| Protobuf | 3.21.12 |
| FlatBuffers | 25.12.19 |
| Cap'n Proto | 1.5.0 |
| bRPC commit | `6a1c6bfb496f56b77494de89146eb27c6c9ef0dd` |
| bRPC branch | `pr-22-compile-fix` |

当前全部测试均在本地 WSL2 中完成。尚未使用真实 RDMA/URMA 设备，也尚未得到跨物理机器网络结果。

## 3. 测试数据模型

### 3.1 Simple 模型

Simple 模型包含：

- Header：request ID、时间戳、版本、来源；
- SimplePayload：一个字节数组。

该模型用于观察以连续大块 Payload 为主、元数据较少的场景。

### 3.2 Complex 模型

Complex 模型包含：

- Header；
- 16 个 Record；
- 每个 Record 包含 ID、名称、Metrics、samples、两个 Tag 和一部分 Payload。

该模型用于观察多层嵌套结构、字符串、数组和重复字段较多的场景。

### 3.3 Payload 范围

测试覆盖 18 个 Payload：

```text
64B, 128B, 256B, 512B,
1KiB, 2KiB, 4KiB, 8KiB,
16KiB, 32KiB, 64KiB,
128KiB, 256KiB, 512KiB,
1MiB, 2MiB, 4MiB, 8MiB
```

所有方案使用相同的原始字节序列和 checksum 算法。正式结果中 checksum 失败数均为 0。

## 4. 已完成的测试层次

| 测试 | 进程模型 | 数据传递方式 | 主要目的 | 状态 |
|---|---|---|---|---|
| 库级测试 | 单进程 | 进程内缓冲区 | 分离测量编码、复制、解析/建视图和访问 | 已完成，共 6 轮 |
| IPC 测试 | 两个独立进程 | POSIX 共享内存单槽位 | 测量生产者和消费者分离后的成本 | 已完成，共 3 轮、35,100 条、0 失败 |
| bRPC 测试 | 客户端 + 服务端 | localhost TCP | 测量完整 RPC 请求—响应路径 | 已完成，共 3 轮、35,100 条、0 失败 |

## 5. 单进程库级测试

### 5.1 测试流程

```text
构造对象
→ 序列化
→ memcpy 到消费者缓冲区
→ Protobuf 反序列化，或 FlatBuffers/Cap'n Proto 建立视图
→ 部分字段访问
→ 完整数据访问
→ checksum 校验
```

CSV 分别记录：

- `serialize_ns`
- `copy_ns`
- `deserialize_or_view_ns`
- `partial_access_ns`
- `full_access_ns`
- `end_to_end_ns`
- `encoded_bytes`
- `checksum`
- `success`

因此，该测试既能单独比较序列化和反序列化，也能比较完整本地数据处理流水线。

### 5.2 主要结果

- Protobuf 对复杂小消息的编码结果最紧凑。
- FlatBuffers 和 Cap'n Proto 可以在编码缓冲区上建立视图，不需要构造完整反序列化对象。
- FlatBuffers 的连续缓冲区和运行稳定性更适合作为工程集成起点。
- Cap'n Proto 建立 Reader 很快，但部分大消息和复杂对象测试中的波动较大。
- Payload 增大后，复制和完整内存扫描逐渐成为主要成本。

## 6. 双进程共享内存测试

### 6.1 测试流程

```text
Serializer/Producer 进程
  构造 simple/complex 对象
  → 序列化
  → memcpy 到 POSIX 共享内存
  → 将槽位状态设为 READY

Deserializer/Consumer 进程
  等待 READY
  → Protobuf ParseFromArray
     或 FlatBuffers Verifier + GetRoot
     或 Cap'n Proto FlatArrayMessageReader
  → 完整访问 Payload
  → checksum 校验
  → 将槽位状态设为 CONSUMED
```

两个进程地址空间彼此独立，使用一个共享内存槽位进行严格的生产—消费 ping-pong。

CSV 分别记录：

- `serialize_ns`：生产者序列化时间；
- `publish_ns`：复制到共享内存的时间；
- `consumer_parse_or_view_ns`：消费者解析或建视图时间；
- `consumer_access_ns`：消费者完整访问时间；
- `end_to_end_ns`：发布后至消费者处理完成的时间。

用于比较的完整流水线时间为：

```text
serialize_ns + publish_ns + end_to_end_ns
```

每轮第一条记录包含人工/进程启动等待，在汇总统计中予以剔除。

### 6.2 完整流水线 P50

| 模型 / Payload | Protobuf | FlatBuffers | Cap'n Proto |
|---|---:|---:|---:|
| Simple 64B | 0.56 μs | 0.52 μs | 0.54 μs |
| Simple 4KiB | 1.56 μs | 1.35 μs | 1.88 μs |
| Simple 1MiB | 0.81 ms | 0.73 ms | 0.73 ms |
| Simple 8MiB | 9.35 ms | 8.28 ms | 8.05 ms |
| Complex 64B | 13.21 μs | 4.43 μs | 1.97 μs |
| Complex 4KiB | 15.49 μs | 6.78 μs | 3.55 μs |
| Complex 1MiB | 0.66 ms | 0.43 ms | 0.74 ms |
| Complex 8MiB | 7.27 ms | 5.52 ms | 7.95 ms |

### 6.3 单独解析/建视图 P50（8MiB Complex）

| 格式 | 解析/建视图时间 |
|---|---:|
| Protobuf | 620 μs |
| FlatBuffers | 4.4 μs |
| Cap'n Proto | 3.1 μs |

该结果是免反序列化方向最关键的本地证据：FlatBuffers 和 Cap'n Proto 建立视图的成本比 Protobuf 构造完整对象低约两个数量级。

### 6.4 单独序列化 P50（8MiB Complex）

| 格式 | 序列化时间 |
|---|---:|
| FlatBuffers | 4.01 ms |
| Protobuf | 5.33 ms |
| Cap'n Proto | 6.61 ms |

Complex 中大消息场景下，FlatBuffers 的序列化和完整流水线性能最好。

### 6.5 IPC 测试结论

- Simple 小消息差别很小。
- Complex 小消息中 Cap'n Proto 最快，FlatBuffers 次之，Protobuf 构造和解析成本最高。
- Complex 中大消息中 FlatBuffers 整体表现最好。
- 对全部 Payload 进行完整扫描时，三种格式都无法避免真实的内存读取成本。
- 当前生产者仍先编码到临时缓冲区，再复制到共享内存；尚未实现直接在共享/注册内存中原地构建。

## 7. 完整 bRPC localhost TCP 测试

### 7.1 测试数据路径

Protobuf：

```text
客户端构造原生 Protobuf RPC message
→ bRPC 内部编码
→ localhost TCP
→ bRPC 自动解析
→ 服务方法访问数据并校验
→ 返回小型 BenchmarkResponse
```

FlatBuffers/Cap'n Proto：

```text
客户端编码连续缓冲区
→ 复制进 bRPC request_attachment/IOBuf
→ localhost TCP
→ 服务端从 IOBuf 复制到连续 vector
→ 建立视图/Reader
→ 访问数据并校验
→ 返回小型 BenchmarkResponse
```

这是完整的 RPC 请求—响应测试，但属于“大请求 + 小响应”，服务端没有将完整 Payload 原样返回。

### 7.2 客户端完整流水线 P50

| 模型 / Payload | Protobuf native | FlatBuffers attachment | Cap'n Proto attachment |
|---|---:|---:|---:|
| Simple 64B | 69.0 μs | 68.3 μs | 69.1 μs |
| Simple 64KiB | 100.4 μs | 130.1 μs | 133.1 μs |
| Simple 1MiB | 0.87 ms | 1.08 ms | 1.28 ms |
| Simple 8MiB | 9.12 ms | 11.18 ms | 14.10 ms |
| Complex 64B | 87.4 μs | 76.4 μs | 75.4 μs |
| Complex 64KiB | 126.4 μs | 133.4 μs | 149.6 μs |
| Complex 1MiB | 0.61 ms | 0.93 ms | 1.08 ms |
| Complex 8MiB | 6.69 ms | 9.17 ms | 10.86 ms |

客户端完整流水线按以下口径计算：

```text
serialize_ns + rpc_roundtrip_ns
```

需要注意：Protobuf 的实际线性编码和服务端解析由 bRPC 内部完成，部分成本包含在 `rpc_roundtrip_ns` 中。

### 7.3 8MiB Complex 估算吞吐量

| 格式 | 吞吐量 |
|---|---:|
| Protobuf native | 约 1195 MiB/s |
| FlatBuffers attachment | 约 872 MiB/s |
| Cap'n Proto attachment | 约 737 MiB/s |

### 7.4 bRPC 测试结论

- 64B Simple 场景三者约为 68～69 μs，主要由 RPC 固定开销主导。
- 当前 bRPC 路径下，大消息 Protobuf native 最快，FlatBuffers attachment 次之，Cap'n Proto attachment 最慢。
- 这不能直接证明 Protobuf 格式本身在大消息上更优，因为三种格式使用了不同的 bRPC 数据路径。
- FlatBuffers/Cap'n Proto attachment 路径多出了客户端写入 IOBuf和服务端复制出 IOBuf 的成本。
- 当前 CSV 中 `server_access_ns` 实际更接近 attachment 复制、建视图和访问组成的服务端总处理时间，不应解读成纯字段访问时间。

## 8. IPC 与 bRPC 结果的关键对照

以 8MiB Complex 为例：

| 格式 | 共享内存双进程 | bRPC localhost TCP |
|---|---:|---:|
| Protobuf | 7.27 ms | 6.69 ms |
| FlatBuffers | 5.52 ms | 9.17 ms |
| Cap'n Proto | 7.95 ms | 10.86 ms |

FlatBuffers 在共享内存路径中比 Protobuf 快约 24%，但在当前 bRPC attachment 路径中比 Protobuf 慢约 37%。

这表明当前的主要问题不是 FlatBuffers 无法带来收益，而是 attachment 路径中的额外复制掩盖了免反序列化收益。

## 9. 对 bRPC 免序列化特性的启发

仅仅把 FlatBuffers 或 Cap'n Proto 编码结果放入传统 attachment 并不够。建议为 bRPC 设计统一的可寻址数据区域抽象，例如：

```text
RemoteRegion / ZeroCopyAttachment / SerializedView
```

理想路径：

```text
发送端在可发送/注册内存中构建编码结果
→ TCP、RDMA 或 URMA 传输
→ 接收端直接持有接收内存区域
→ FlatBuffers/Cap'n Proto 在该区域建立只读视图
→ 按需访问字段
```

第一阶段建议优先适配 FlatBuffers，原因包括：

- Complex 中大消息的共享内存流水线性能最好；
- 单一连续缓冲区更容易映射到 IOBuf、RDMA 和 URMA 注册内存；
- 建视图成本很低；
- 运行结果总体比 Cap'n Proto 稳定；
- 对现有 bRPC attachment 接口的改造复杂度相对较低。

Cap'n Proto 可作为第二阶段适配对象，需要进一步处理 word 对齐、segment、遍历限制和大消息稳定性。

## 10. 当前尚未完成的测试

以下内容尚未测试：

- 两台物理机器之间的普通 TCP；
- 大请求 + 大响应的 Payload Echo；
- 多客户端并发和吞吐量饱和；
- CPU 绑核、NUMA 和内存亲和性控制；
- 多槽位共享内存流水线；
- 发送端直接在目标共享/注册内存中原地构建；
- 真实 RDMA 数据路径；
- 真实 URMA 数据路径；
- 最终 bRPC RemoteRegion/ZeroCopyAttachment 特性的 A/B 对照。

## 11. 总结

当前已经完成：

1. 单进程中可分项统计的序列化和反序列化/建视图测试；
2. 单个序列化生产者进程与单个反序列化消费者进程的共享内存测试；
3. 客户端与服务端之间完整的 bRPC localhost TCP 请求—响应测试。

全部正式测试均覆盖 Protobuf、FlatBuffers、Cap'n Proto、Simple/Complex 和 64B～8MiB，正确性检查全部通过。

当前最重要的实验结论是：

> FlatBuffers/Cap'n Proto 的建视图确实远快于 Protobuf 反序列化，但如果 bRPC attachment 仍需要额外内存复制，这一优势可能被完全抵消。社区特性应同时解决反序列化和缓冲区复制问题，而不是只替换编码格式。

本文档中的结果是 WSL2 本地基线，不能替代真实 RDMA/URMA 环境中的最终实验。
