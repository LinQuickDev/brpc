# Protobuf / FlatBuffers / Cap'n Proto 单独反序列化测试总结

> 重新生成日期：2026-09-03  
> 数据来源：`ipc-run1.csv`、`ipc-run2.csv`、`ipc-run3.csv`，共 35,100 条记录，失败 0 条。  
> 统计口径：仅使用 `consumer_parse_or_view_ns`；不包含序列化、发布复制、字段访问、IPC 等待和 RPC。

## 1. 测试目的

本测试只比较消费者已经获得完整编码缓冲区后，将其转换成可读取消息所需的成本：

- Protobuf：完整反序列化并构造 C++ 对象；
- FlatBuffers：验证缓冲区并取得根对象视图；
- Cap'n Proto：建立 Reader 并取得根对象视图。

本文将该指标统一称为 `parse_or_view_ns`。它不包含生产端序列化、跨进程发布、RPC 或完整 Payload 扫描。

## 2. 单独反序列化的定义

计时起点是消费者已经持有完整、可访问的编码缓冲区，计时终点是得到可供字段访问的消息对象或只读视图：

```text
已有编码缓冲区
→ 开始计时
→ 解析或建立视图
→ 得到根消息
→ 停止计时
```

不包括：

- 发送端对象构造和序列化；
- 编码缓冲区生成；
- memcpy 到共享内存；
- 生产者/消费者等待；
- 部分字段读取和完整 Payload 扫描；
- TCP、bRPC、RDMA 或 URMA。

## 3. 三种格式的计时边界

### 3.1 Protobuf

```text
创建空的 SimpleMessage/ComplexMessage
→ ParseFromArray(encoded_buffer)
→ 得到完整 C++ 对象树
```

Protobuf 必须遍历 wire format、分配嵌套对象和字符串/数组，并把字段填充到新对象中。

### 3.2 FlatBuffers

```text
创建 Verifier
→ VerifyBuffer<SimpleMessage/ComplexMessage>()
→ GetRoot()
→ 得到指向原缓冲区的只读视图
```

FlatBuffers 不创建完整对象副本，但当前测试把完整缓冲区验证计入 `parse_or_view_ns`。

### 3.3 Cap'n Proto

```text
创建 FlatArrayMessageReader
→ getRoot<SimpleMessage/ComplexMessage>()
→ 得到指向原缓冲区的 Reader
```

Cap'n Proto 数据必须满足 word 对齐要求，并设置足够的 traversal limit。当前计时不包含对整个消息进行与 FlatBuffers Verifier 完全等价的全量验证，因此二者的安全检查口径并不完全相同。

## 4. 测试流程

双进程测试采用：

```text
Producer 将编码消息发布到 POSIX 共享内存
→ 槽位状态变成 READY
→ Consumer 直接在共享区域执行解析/建视图
→ 停止 parse/view 计时
→ 另行测量完整访问
→ checksum 校验
```

本文只使用 CSV 中的 `consumer_parse_or_view_ns`，不把 `consumer_access_ns` 加入反序列化结果。

模型和 Payload 与序列化测试一致：Simple/Complex，64B～8MiB。正式测试运行三轮且所有 checksum 正确。

## 5. 代表性解析/建视图 P50

### 5.1 Simple 模型

| Payload | Protobuf 解析 | FlatBuffers 验证+建视图 | Cap'n Proto 建 Reader |
|---|---:|---:|---:|
| 64B | 0.122 μs | 0.082 μs | 0.071 μs |
| 4KiB | 0.427 μs | 0.082 μs | 0.071 μs |
| 64KiB | 3.24 μs | 0.080 μs | 0.090 μs |
| 1MiB | 30.10 μs | 0.085 μs | 0.246 μs |
| 8MiB | 603 μs | 0.511 μs | 3.37 μs |

### 5.2 Complex 模型

| Payload | Protobuf 解析 | FlatBuffers 验证+建视图 | Cap'n Proto 建 Reader |
|---|---:|---:|---:|
| 64B | 3.39 μs | 0.992 μs | 0.070 μs |
| 4KiB | 4.24 μs | 1.71 μs | 0.070 μs |
| 64KiB | 8.72 μs | 1.20 μs | 0.096 μs |
| 1MiB | 37.64 μs | 1.09 μs | 0.235 μs |
| 8MiB | 620 μs | 4.44 μs | 3.12 μs |

## 6. 主要结论

1. Protobuf 解析时间随 Payload 增大而明显增长，因为它需要扫描编码数据并构造完整对象。
2. FlatBuffers 和 Cap'n Proto 主要建立指向原始缓冲区的视图，建视图成本显著更低。
3. 8MiB Complex 中，Protobuf 约为 620 μs，FlatBuffers 约为 4.4 μs，Cap'n Proto 约为 3.1 μs；后两者比 Protobuf 低约两个数量级。
4. FlatBuffers 的 Complex 建视图数据包含 Verifier，因此比只建立 Reader 的 Cap'n Proto 更高。
5. “建视图很快”不等于“完整处理消息不需要时间”。如果业务读取全部 8MiB Payload，内存扫描成本仍然存在。

## 7. 反序列化与数据访问必须分开

消费者阶段分为：

```text
parse_or_view_ns
→ 将缓冲区变成可读消息或视图

consumer_access_ns
→ 实际遍历字段和 Payload，计算 checksum
```

免反序列化主要优化第一部分。对于只读取少数字段的业务，FlatBuffers/Cap'n Proto 可以避免解析和复制未访问字段，收益可能很大；对于必须完整扫描大 Payload 的业务，访问内存的成本无法通过格式本身消除。

## 8. 与 bRPC 测试的关系

在当前 bRPC 测试中：

- Protobuf 由 bRPC 在调用服务方法前自动解析，无法在服务方法中单独计时；
- FlatBuffers/Cap'n Proto 需要先从 bRPC IOBuf 复制到连续 vector，再建立视图；
- 额外复制会掩盖免反序列化收益。

共享内存测试能够单独观察解析/建视图成本，因此更清楚地证明免反序列化的潜力；bRPC 测试则衡量现有系统中的真实完整路径。

## 9. 对特性设计的意义

要让本测试中的低建视图成本在 bRPC、RDMA/URMA 中真正发挥作用，接收端必须能直接访问传输完成后的内存区域：

```text
传输完成
→ 接收端获得 RemoteRegion/ZeroCopyAttachment
→ 不复制到新的连续 vector
→ 直接验证并建立 FlatBuffers/Cap'n Proto 视图
→ 按需访问字段
```

FlatBuffers 适合作为第一阶段：它采用单一连续缓冲区、建视图成本低、验证模型清晰，且比 Cap'n Proto 更容易接入 IOBuf 和注册内存。Cap'n Proto 可在第二阶段处理对齐、segment 和 traversal limit 等问题。

## 10. 当前限制

- 测试位于 WSL2 本地共享内存，不代表跨机器或 RDMA/URMA 延迟；
- 使用单槽 ping-pong 和忙等待，没有测试并发与流水线饱和；
- 没有进行 CPU 绑核和 NUMA 控制；
- FlatBuffers 与 Cap'n Proto 的验证强度不完全一致；
- 本文的反序列化结果不包含字段访问时间，这是有意的指标隔离。

当前结论是：FlatBuffers/Cap'n Proto 的建视图成本确实远低于 Protobuf 完整解析；但最终社区方案还必须同时消除接收路径复制，才能在完整 RPC 中兑现这部分收益。
