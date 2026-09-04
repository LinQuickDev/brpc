# Protobuf / FlatBuffers / Cap'n Proto 单独序列化测试总结

> 重新生成日期：2026-09-03  
> 数据来源：`ipc-run1.csv`、`ipc-run2.csv`、`ipc-run3.csv`，共 35,100 条记录，失败 0 条。  
> 统计口径：仅使用 `serialize_ns`；不包含发布复制、反序列化、数据访问、IPC 等待和 RPC。

## 1. 测试目的

本测试只比较三种方案从统一业务数据生成最终可传输编码缓冲区的成本：

- Protobuf 3.21.12
- FlatBuffers 25.12.19
- Cap'n Proto 1.5.0

本文不讨论反序列化、共享内存发布、网络传输或 RPC。

## 2. 单独序列化的定义

本项目将单独序列化定义为：

```text
准备好的原始 Payload
→ 开始计时
→ 构造对应格式的 Simple/Complex 消息
→ 填充 Header、Record、Metrics、Tag 和 Payload
→ 生成最终编码缓冲区
→ 停止计时
```

计时结果记录在 `serialize_ns`。它包括对象/Builder 创建、字段填充、内存分配、Payload 写入以及生成最终 wire-format 缓冲区。

不包括：

- 将编码结果复制到另一块缓冲区；
- `publish_ns` 共享内存发布；
- Protobuf `ParseFromArray()`；
- FlatBuffers `Verifier`、`GetRoot()`；
- Cap'n Proto `FlatArrayMessageReader`；
- 部分或完整字段访问；
- IPC 等待、TCP、bRPC、RDMA 或 URMA。

## 3. 三种格式的计时边界

### 3.1 Protobuf

```text
创建 SimpleMessage/ComplexMessage
→ 填充所有字段
→ SerializeToString()
→ 得到 std::string 编码结果
```

### 3.2 FlatBuffers

```text
创建 FlatBufferBuilder
→ 创建 String、Vector 和 Table
→ Finish()
→ 得到 Builder 中的连续编码缓冲区
```

FlatBuffers 没有与 Protobuf 完全相同的“先构造普通对象，再单独编码”阶段；Builder 构造过程本身就是最终内存布局生成过程。

### 3.3 Cap'n Proto

```text
创建 MallocMessageBuilder
→ 初始化结构体和列表
→ 填充所有字段
→ messageToFlatArray()
→ 得到连续 word 数组
```

## 4. 测试数据

模型：

- Simple：Header + 单个连续字节数组；
- Complex：Header + 16 个嵌套 Record，每个 Record 包含名称、Metrics、samples、Tag 和一部分 Payload。

Payload 覆盖：

```text
64B、128B、256B、512B、1KiB、2KiB、4KiB、8KiB、
16KiB、32KiB、64KiB、128KiB、256KiB、512KiB、
1MiB、2MiB、4MiB、8MiB
```

正式 IPC 数据共运行三轮。以下结果取三轮合并后的中位数 P50；每轮第一条进程启动等待记录不参与汇总。

## 5. 代表性结果

### 5.1 Simple 模型

| Payload | Protobuf | FlatBuffers | Cap'n Proto |
|---|---:|---:|---:|
| 64B | 0.176 μs | 0.110 μs | 0.161 μs |
| 4KiB | 0.324 μs | 0.172 μs | 0.233 μs |
| 64KiB | 18.80 μs | 20.77 μs | 17.91 μs |
| 1MiB | 0.640 ms | 0.592 ms | 0.590 ms |
| 8MiB | 7.26 ms | 6.86 ms | 6.78 ms |

Simple 模型中三者差距总体有限。小消息中 FlatBuffers 最快；8MiB 时 FlatBuffers 和 Cap'n Proto 接近，均略快于 Protobuf。

### 5.2 Complex 模型

| Payload | Protobuf | FlatBuffers | Cap'n Proto |
|---|---:|---:|---:|
| 64B | 8.32 μs | 3.04 μs | 1.25 μs |
| 4KiB | 9.15 μs | 3.21 μs | 1.30 μs |
| 64KiB | 15.94 μs | 16.42 μs | 30.28 μs |
| 1MiB | 0.475 ms | 0.294 ms | 0.601 ms |
| 8MiB | 5.33 ms | 4.01 ms | 6.61 ms |

Complex 小消息中 Cap'n Proto 最快，原因是固定嵌套结构的 Builder 构造成本较低；随着 Payload 增大，Cap'n Proto 的连续化成本上升。Complex 1MiB 和 8MiB 中 FlatBuffers 最快。

## 6. 主要结论

1. 没有一种格式在所有模型和 Payload 下始终最快。
2. Simple 小消息：FlatBuffers 略优，但绝对差异只有几十到几百纳秒。
3. Complex 小消息：Cap'n Proto 明显领先，FlatBuffers 次之，Protobuf 最慢。
4. Complex 中大消息：FlatBuffers 最有优势；8MiB 比 Protobuf 快约 25%，比 Cap'n Proto 快约 39%。
5. 大消息序列化时间主要由 Payload 写入、内存分配和最终缓冲区生成决定。
6. Protobuf 对复杂小消息通常编码更紧凑，但紧凑程度和序列化耗时是不同指标。

## 7. 公平性说明

`serialize_ns` 是“从统一原始数据得到可传输缓冲区”的业务口径，不是只测一个库函数的微基准。这一口径适合比较真实发送端成本，但应注意：

- Protobuf 构造普通消息对象后再次执行编码；
- FlatBuffers 直接通过 Builder 构造最终布局；
- Cap'n Proto 通过 Builder 构造消息后又执行 `messageToFlatArray()` 连续化。

三种库的编程模型不同，无法完全拆成语义相同的内部步骤。

## 8. 当前限制与下一步

当前生产端仍然执行：

```text
生成临时编码缓冲区
→ 后续再复制到共享内存或 bRPC IOBuf
```

因此测试已经隔离出序列化时间，但尚未测量“直接在目标共享内存或 RDMA/URMA 注册内存中原地构建”。下一步应为 FlatBuffers 提供目标内存分配器，比较：

```text
临时缓冲区构建 + memcpy
vs.
直接在可发送/注册内存中构建
```

该测试结果来自 WSL2 本地 CPU 和内存，不能直接视为远端 RDMA/URMA 性能结果。
