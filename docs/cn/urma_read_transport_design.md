# UrmaTransport 单边 READ 数据面设计

## 1. 目标与首版范围

首版采用大小包混合路径：小于 4096 字节的待发送 IOBuf 继续使用现有 SEND/RECV；达到或超过 4096 字节的 IOBuf 用 SEND/RECV 发送 descriptor/credit，以 READ 搬运 payload。目标环境须成功完成一条大于 4096 字节的 READ WR。首版验收关注路径正确和限制解除，不要求吞吐超过当前实现。

保留 TCP 建链、现有 pool segment 注册/导入和 `InputMessenger` 接口；双方部署同一新版协议，暂不做能力协商。按每个 transport `WriteRequest` 中的编码后字节数选择路径，不解码 RPC body。选定 READ 路径的请求在部分发送时保持该路径直到发送完毕；每个方向最多一个未确认的大包 offer。小 SEND 仍按现有窗口批量发送。两端仍可同时发送数据，不将整个连接限制成半双工。

首版复用现有约 8 KiB 的注册 IOBuf block；接收端使用固定注册缓冲，READ 完成后复制到输入 IOBuf 再复用。小包保留现有零拷贝 SEND 路径；多 READ 并发、零拷贝 READ 交付、64 KiB 以上 staging 和 WRITE 数据面均为后续优化。

本文是设计评审结果，READ 路径尚未实现，也未在真实设备上验证。

## 2. 代码与技术材料核对

本次核对基于 brpc `36c2d12e` 和本机 Mooncake `a2f25168`，定位如下。

| 代码位置 | 已确认事实及设计影响 |
|---|---|
| [urma_endpoint.cpp](../../src/brpc/urma/urma_endpoint.cpp)，`CutFromIOBufList()` | 当前以 `URMA_OPC_SEND` 发送；按 peer receive buffer 和 SGE 数切分，未发现固定按 4096 字节切分的逻辑。4 KiB 作为用户提供的目标环境约束，不能归因于此函数已有硬编码。 |
| [urma_helper.cpp](../../src/brpc/urma/urma_helper.cpp)，`PoolAllocate()`、`GlobalUrmaInitializeOrDie()` | 默认 block 总大小为 8192 字节，有效 payload 为 `8192 - sizeof(IOBuf::Block)`；超大申请或 pool 耗尽会退回普通 allocator。IOBuf 总长不受这个单 block 容量限制。 |
| `UrmaEndpoint::ImportPeer()` | 已导入对端 pool segment；注册与导入配置包含 READ 权限，可复用作远端 source。用户单独注册的 segment 当前未随握手发布。 |
| `UrmaIOBuf::cut_into_sglist()` | 当前边构造 SGE 边消费输入；新路径必须明确 post 失败时输入字节、lease 和 credit 的提交/回滚关系。 |
| [urma_transport.cpp](../../src/brpc/urma_transport.cpp)，`CutFromIOBufList()`、`WaitEpollOut()` | `URMA_ON` 后走 URMA 数据通路，不能因某个 buffer 未注册而临时把这部分字节发往 TCP。背压通过 `IsWritable()` 和 socket wakeup 对接。 |
| `UrmaEndpoint::HandleCompletion()`、`Reset()`、`DeallocateResources()` | 当前 SEND 完成即释放 `_sbuf`；READ source 需延长到对端确认。现有 CQ 停止和析构顺序需增加 DMA 生命周期处理。 |

Mooncake 的 `mooncake-transfer-engine/src/transport/kunpeng_transport/urma/urma_endpoint.cpp::submitPostSend()` 展示了：READ 使用远端 `rw.src`、本地 `rw.dst`，WRITE 则相反；WR 经 `urma_post_jetty_send_wr()` 提交，以 `user_ctx` 找到 Slice。上层 `ub_transport.cpp` 按 `slice_size` 分块，`include/config.h` 默认 65536。它提供了大于 4 KiB 的分块设计参考，不能代替目标硬件的成功传输证据，也不提供 brpc 的字节流和缓冲释放协议。

[openEuler URMA API Guide](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html) 的 `urma_rw_wr_t` 说明 READ 远端 source 只能有一个 SGE；设备能力包含 `max_read_size`；`urma_cr_t::opcode` 标注仅用于接收 completion。因此，READ 的本地 completion 应由 WR context 分派，不能要求其 opcode 等于某个接收 opcode。实际编译/运行时仍以部署的 UMDK/provider 版本为准。

## 3. 首版数据路径

```text
两端建链并导入 segment
  -> 预留注册 RX slot、TX bounce slot、控制 SEND/RECV buffer
  -> 各发送一次 READ_READY，发布一个接收额度及可接收 chunk 上限

小包（该 WriteRequest 编码长度 < 4096）：
S: 继续现有 SEND，payload 直接进入对端注册 receive buffer
R: 现有 receive 路径将数据交给 InputMessenger，并按现有机制回收 RQ credit

大包（该 WriteRequest 编码长度 >= 4096）：
S: 按字节流顺序准备 source，持有 lease，预扣对端 RQ credit
  -> SEND_IMM DATA_OFFER(seq, stream_offset, segment_offset, length)
R: 收到并复制 descriptor，重投 receive buffer
  -> READ(remote source -> 固定本地 RX slot)
R: READ 成功 -> 将 length 字节复制进输入 IOBuf -> RX slot 可复用
  -> SEND_IMM READ_DONE(seq, next_credit=1)
  -> 按既有流程通知 InputMessenger 新增字节
S: 收到 READ_DONE -> 释放 source lease、恢复一个大包数据 credit
  -> 等待 DATA_OFFER 本地 SEND completion 后，允许后续流数据
```

小包继续占用 JFR receive WR，保留现有 credit ACK 和 zero-copy 行为。大包 READ 不占用 payload 的 JFR receive WR；DATA_OFFER/READ_DONE 是带短 payload 的 SEND_IMM，仍消耗控制消息对端 JFR。控制帧通过 imm_data 中的保留 control tag 与普通 SEND payload 及既有纯 credit ACK 区分，不将控制内容送入 RPC 字节流。

## 4. 控制消息与字节流序号

首版固定编码小型控制消息，单条不超过 128 字节，并保证小于部署环境的 SEND 上限；不要直接发送带有编译器 padding 的 C++ struct。公共头部含 magic、固定协议版本、type、编码长度，整数采用网络字节序。

| 消息 | 字段与含义 |
|---|---|
| `READ_READY` | 本端连接 epoch、`max_chunk_length`、初始 credit=1；仅在注册缓冲及控制 RQ 已就绪后发送一次。 |
| `DATA_OFFER` | 双端 epoch、`seq`、`stream_offset`、`remote_segment_offset`、`chunk_length`；发布下一段连续字节。 |
| `READ_DONE` | 双端 epoch、所确认的 `seq`、`next_credit=1`；同时确认 source 不再被读取，以及下一段数据的目标 slot 已可用。 |

控制帧通过有 payload 的 `URMA_OPC_SEND_IMM` 发送，使用保留的 imm_data 高位/tag 和 type 标记。现有空 payload credit ACK 保持原义，imm_data 仍表示返还的 RQ 数；接收端必须先区分控制 tag 与 ACK，不能把控制 tag 当成 credit 数累加。控制帧本身消耗一个普通远端 RQ credit，接收端按现有 `SendAck(1)` 归还。普通小包仍为 `URMA_OPC_SEND`，由 completion opcode 区分。若 provider 对有 payload 的 SEND_IMM 有额外限制，先在目标环境验证；不支持则需改用专用控制 Jetty/明确的 framing，不能猜测魔数识别普通 payload。

两端在 READ_READY 交换各自本次连接唯一的 epoch，后续消息校验 epoch 对；无需新增 TCP 握手能力协商。`max_chunk_length` 是真实接收资源及 READ 长度上限的公布，不能复用控制 receive buffer 的长度。收到 peer READY 且本端 READY 已被 provider 接受后，才可发布 offer。

启动时不能假定 READY 与 OFFER 的接收 CQE 保序：若先处理到一个 OFFER，先复制控制字段、重投 receive WR 并暂存这一条 offer，待 peer READY 到达后验证 epoch 并启动 READ。首版只需一个启动暂存项，不需要通用重排队列；额外提前到达的 offer 视为协议错误。READY 永不等待数据 credit。

`CutFromIOBufList()` 接收的是字节流片段，可能跨 RPC，也可能只有一条 RPC 的部分字节。删除原方案的 `message_total_length`、`chunk_count` 和 RPC message sequence。每个方向独立维护递增 `seq` 与字节偏移，接收端要求等于当前期望值；`(epoch 对, 方向, seq)` 同时作为 lease 标识，不再单独分配 lease_id。

校验 `length > 0`、`length <= max_chunk_length`、`offset <= seg_len`、`length <= seg_len - offset`，并检查 VA 加法、流偏移递增溢出。重复 READY、重复 DONE、缺序/重复 offer、非法 credit 或 epoch 不匹配按协议错误处理，不重复交付或重复加 credit。首版不做传输层重发。

READ_DONE 的两个条件仍是独立条件：**READ 已成功结束、RX slot 已重新可用**。首版复制接收数据后两者均成立，故合并在一条消息中。以后零拷贝交付若使 slot 尚不可用，必须拆分 lease 确认与 credit 补充，不能直接沿用 `next_credit=1`。

路径选择以 `WriteRequest` 的编码后 IOBuf 大小为准，包含 brpc 协议头；不等于应用层 protobuf/body 大小。`CutFromIOBufList()` 可能一次收到多个 WriteRequest，因此逐项分类，不能以整个 list 的总长度选路。相同 IOBuf 若因窗口只消费了一部分，必须保存首次路径选择直到该 IOBuf 清空，不能让“大包的最后几千字节”意外切回 SEND。若要严格按应用 RPC body 大小选择路径，需要在协议序列化层额外传递 size/type 元数据，不属于本首版。

## 5. IOBuf、注册内存与超过 4 KiB 的 WR

### 5.1 长度约束

接收端公布：

```text
max_chunk_length = min(本地固定 RX slot 的有效容量,
                       本地设备/provider 的有效 READ WR 上限,
                       SGE length 字段上限)
实际 chunk_length <= min(peer.max_chunk_length, 本次连续 source 长度)
```

READ 上限结合设备 `max_read_size` 和当前 provider/传输模式的有效限制确定；未知或返回零的能力值不能解释成无限大。只在部署文档确认适用时加入其它设备上限，不能把 SEND 的 4096 限制直接套给 READ。

默认 8192 字节 block 的有效 payload 大于 4096，足以验证单条 READ 跨过 4 KiB。若实际配置使有效容量不大于 4096，则该配置只能验证流程，不能通过“解除 4 KiB WR 限制”的验收。小包阈值只决定路径；大 IOBuf 超过单 slot 容量时继续多次 READ，IOBuf 和 RPC 总长没有因此被限制为一个 block。

### 5.2 Source 与 bounce slot

若输入头部存在满足本次长度的连续 pool-backed 范围，可直接保留其 IOBuf 引用作为 source lease，整个范围都必须属于已发布的注册 segment。源字节在 lease 期间保持不变。

为覆盖普通 malloc IOBuf、外部用户数据、启动前生成的 IOBuf 和碎片化输入，每个连接预留一个 pool-backed TX bounce slot。需要时把输入前缀复制/合并进去，再发布其 offset。首版对未发布的用户注册 segment 也采用这一方式。slot 有效容量约为一个默认 block，不增加新的大块注册池。

为避免输入天然由若干小于 4 KiB 的片段组成而始终产生小 READ，当前已有输入足够时，可把小片段合并到 bounce slot，直到本次 chunk 上限；不得为凑满 block 等待未来 RPC 数据。满长度连续 source 仍直接引用。

必须提供“只从注册 pool 取 slot”的 helper，或在分配后检查完整范围；不能把普通 allocator 的回退地址当作 DMA 地址。固定 RX/TX/控制 slot 在发布 READY 前分配完成，失败则建链失败或在切入 URMA 前按原策略回退。URMA 已启用后的资源/协议错误使连接失败，不在该连接混发 TCP payload。

### 5.3 固定 RX slot 与复制交付

首版每端一个固定 RX slot。成功 READ 后，仅把 context 记录的有效 `length` 复制进 `_input_processor.read_buf()`；复制成功后才允许复用 slot、返回 credit。输入 IOBuf 只供 CPU/解析器访问，允许使用普通内存；不能继续要求它的所有 block 都是注册内存。复制失败按连接错误处理。

这样一条大于接收窗口的 RPC 能持续进入解析器，不会因解析器持有不完整消息的 block 而耗尽固定 READ slot。普通输入缓冲仍有应用积压和内存耗尽风险，沿用 brpc 消息大小与错误处理规则；READ credit 只约束在途 DMA，不代表应用消费确认。

本阶段不新增修改全局 `SetDefaultBlockSize()` 的行为，也不新增 64 KiB staging 池。接收复制和 window=1 是首版为简化生命周期作出的取舍，不能据此预期吞吐提升。

### 5.4 首版选 READ 的原因

READ 与 WRITE 都可以承担大包数据面；两端对称使用 READ 即可覆盖 request 和 response：各自发布已注册 source，peer 对该 source 发起 READ。当前建链已交换并导入 pool segment，首版无需再为每个 WRITE target 发布 remote destination 地址。

WRITE 路径要求接收端先发布 destination slot 的 segment/offset/长度和可写额度，发送端完成 WRITE 后再通知接收端何时可见/何时可复用，并处理对端提前断连后的本地 target 回收。它可以减少 READ 发起侧对 peer source 生命周期的依赖，但增加目标 slot 发布、写完成通知和访问权限验证。首版优先用 READ 打通闭环；完成性能对比后，再以相同小包 SEND 路径和明确的 WRITE slot 协议评估替代 READ 的收益，不把 READ/WRITE 混在同一个首版大包路径中。

## 6. 窗口、控制面进展与背压

首版使用严格 stop-and-wait：每方向的数据 credit 仅为 0 或 1。发送端收到一次 READY 获得初始 credit，post offer 前消费，收到匹配的 DONE 后恢复。发送 source lease 未确认时不允许第二个 offer；接收端最多一个 READ 在途。READ、控制 SEND 和 JFC 仍分别记账。

小 SEND 与其纯 SEND_IMM credit ACK 保持现有窗口实现。大 READ 的数据 credit 独立于 receive-buffer credit：READY 给一个大包额度；发送端发 DATA_OFFER 时消费；READ_DONE 在目标 slot 可复用、source 不再被 DMA 访问后恢复。DATA_OFFER/READ_DONE 控制帧自身也消耗现有 RQ credit，并沿用 ACK 机制返还。READ_DONE 发送优先级高于新的数据，但不能越过 SQ/RQ 约束。

固定 READ window=1 限制控制面状态：

1. 每端每连接只发送一次 READY；下一条 OFFER 依赖前一条 DONE。因此每方向最多一个 DATA_OFFER/READ 在途及一条因本地 SEND completion 延迟而待复用的控制 buffer。
2. READY/OFFER/DONE 使用已有 JFR pool 和 RQ credit。接收控制消息后先复制字段、重投 receive WR，再进行 READ 或更新状态；重投失败关闭连接。控制帧同样通过 `SendAck(1)` 返还 RQ credit，纯 ACK 本身不再 ACK，避免确认循环。
3. 数据面 SEND、控制面 SEND_IMM 都从原 SQ/JFC 资源池分配 completion；READY/OFFER/DONE 控制 buffer 必须等本地 SEND completion 后复用。小包继续受原 `IsWritable()`/remote RQ credit 约束。
4. JFC 深度覆盖实际允许的 SQ 与已投递 RQ 完成数量，poller 在无业务数据时也必须处理控制 completion。READ_DONE 被 SQ/RQ 暂时阻塞时保持待发状态，在对应 credit/completion 到达后重试，不得依赖下一次应用写入才推进。

小包 SEND 可能与 READ 控制帧交错，但每个 WriteRequest 先选定且保持路径。所有影响字节流的 SEND/SEND_IMM 需在同一 Jetty 上保持接收 CQE 顺序：对目标 URMA_TM_RM/provider 显式启用并验证 `comp_order`（及其要求的 ordered completion 配置）。收到大包 offer 时校验 `stream_offset`；若前序小 SEND completion 尚未交付，暂存这一条 offer，待偏移到达后执行 READ。小包接收后按其 completion_len 推进字节偏移；READ completion 成功并复制后推进其长度。若部署 provider 不能保证混合 opcode 的 completion 顺序，需给小包也加显式 offset 并做有界重排，不能依赖未验证的 CQ 顺序。

`IsWritable()` 同时检查当前 WriteRequest 的路径条件、peer READY/READ credit、source/bounce 可用、SQ 与对应 completion context。未能消费任何输入时返回 `EAGAIN`；已成功接收部分输入时返回实际字节数。小 SEND completion 和 READ_DONE 等状态变化都要唤醒写侧；READ、credit ACK 与待发控制消息的进展不能依赖本端继续收到新业务写入。

## 7. 提交、完成和排序

### 7.1 Post 前后的事务边界

在 post 前构造 source lease、稳定的 control/READ context、SGE 并预扣资源；采用串行状态机或明确的锁/引用保护，使 poller 能安全处理早于 post 返回的 completion。endpoint 状态及输入 buffer 的更新需序列化，调用 InputMessenger/用户回调时不持有提交状态锁，避免重入死锁。

准备 offer 时先引用或复制输入前缀，不立即破坏调用者 IOBuf。provider 确认接受后才消费对应字节并返回成功字节数；明确未接受时撤销预留，输入保持未消费。post 错误仍按第 8 节关闭连接，是否重试 RPC 由 brpc 上层策略决定。context 与源引用不能因为收到快速 DONE 而在提交线程仍访问时被销毁。

首版每次只 post 一个 WR，避免引入 WR 链部分成功处理。只有依据 API 返回值与 `bad_wr` 明确未提交，才可撤销资源；状态不明则关闭连接并进入安全回收，禁止重复发布已可能被接收的 offer。READ post 失败同理，首版不增加 NACK/重发协议。

### 7.2 Completion 分派与内存寿命

```text
user_ctx -> 稳定 operation context { kind, local_epoch, seq, length, slot }
kind = CONTROL_SEND | CONTROL_RECV | READ
```

本地 SEND/READ completion 通过 context 的 kind 分派并检查 status；接收 completion 再检查有效的 recv opcode/控制格式。不能把 READ completion 当作普通 SEND 去清理 `_sbuf`，也不能要求本地 READ CR 具有有效 recv opcode。READ 有效长度由已验证的 WR context 保存，CR 长度字段仅按部署 SDK 定义校验，不能照搬 receive 分支的零长度判断。

| 资源 | 最早安全释放/复用条件 |
|---|---|
| OFFER/READY/DONE 控制发送 buffer | 相应本地 SEND completion；且提交线程已不再访问 |
| source IOBuf 引用或 TX bounce 内容 | 匹配的 READ_DONE；且无提交线程访问。OFFER 的本地 SEND completion 不足以释放 source |
| RX slot | 成功 READ completion 后，数据已完整复制到输入 IOBuf |
| operation context | 对应本地 completion 已处理、提交线程已退出其访问；异常路径须先满足第 8 节静止条件 |

READ_DONE 可早于 OFFER 本地 SEND completion。两个事件分别记账；前者释放 source/恢复数据 credit，后者释放控制 SEND buffer；两者条件满足后才能发布下一 OFFER。

### 7.3 流顺序

READ window=1 下无需通用 READ completion 重排队列。接收端用字节偏移衔接原始小 SEND 与 READ 段；READ 成功并复制后递增期望值，只将 payload 字节计入 `DispatchReceivedBytes()`，控制帧贡献零字节。不等待整条 RPC 到齐才交付。混合 SEND/SEND_IMM 的 CQ 顺序以第 6 节明确配置和目标环境验证为前提。

## 8. 失败、断连与回收

post 失败、completion error、控制协议错误、复制失败均使连接失败；失败/不确定完整度的数据不得交付。正常路径的资源释放及异常路径的 DMA 保护都属于首版必需内容，不能等性能阶段再补。

关闭时先阻止新 post 和新 credit，将资源移交 draining 状态，并同步在途提交线程/poller；保持足够的资源引用，由唯一 drain owner 完成后续清理。具体 reset/flush/drain/unimport/delete 顺序须满足部署 provider 的依赖和 API 约束，不能沿用当前“停 poller、直接析构后清空 IOBuf”作为 READ 安全证明。

必须分别处理两个方向：

- 本端 READ 正在写本地 RX slot：只有确认该 DMA 已完成/取消且相关 completion 生命周期结束后，才能释放目标 buffer/context、解除 imported segment 的依赖。
- 对端可能仍在 READ 本端 source：本端没有对应 READ CQE；删除本端 Jetty、断开 TCP 或排空本地 CQ 均不能单独证明远端已停止读共享注册 segment。收到 DONE 的 lease 可正常回收；其余 lease 需要 provider 的远端访问终止保证，或转入跨 endpoint 生命周期的隔离表。

epoch 只能识别陈旧消息，不能阻止访问旧 VA 的 DMA。隔离表必须继续持有 source 引用、注册内存及必要资源，禁止超时后无条件回池；在连接准入时为可能隔离的资源预留预算，达到字节/对象上限后拒绝新连接，已有不安全资源仍继续持有。若部署 provider 暂无可证明的终止机制，首版可保守隔离并暴露计数，但不能声称异常断连已完成无泄漏回收，也不能通过压力测试后直接取消隔离。

## 9. 实施范围与阶段

### 阶段 A：完成最小闭环（本次方案主目标）

- `urma_helper.{h,cpp}`：注册池专用 slot 获取、完整范围/offset 校验、有效 READ 上限；禁止 DMA slot 默默落入普通 malloc。
- `urma_endpoint.h/.cpp`：短包沿用 SEND/RECV；增加阈值与稳定的 WriteRequest 路径选择、READY/OFFER/DONE 编解码、imm control tag 解码、单 source lease、TX bounce、固定 RX slot、typed completion context。
- `UrmaEndpoint::CutFromIOBufList/HandleCompletion/IsWritable`：避免跨路径合并 SGE，短 SEND WR 和控制帧有界，READ CQ 分派、字节偏移衔接、DONE/credit 与写侧唤醒。
- 建链后增加 READY 就绪条件；`UrmaTransport`/`InputMessenger` 维持既有字节流接口，不增加 RPC 边界解析。
- `Reset/DeallocateResources/UrmaResource`：先完成 in-flight 生命周期、drain/隔离与失败唤醒，再允许 endpoint 复用。
- `mock_urma.cpp`：增加 READ 方向、注册范围校验、本地 READ completion；确保 READ 数据不消费远端 JFR。

### 阶段 B：按测量结果扩展

完成阶段 A 的硬件验收后，优先比较多 READ 窗口及通知批量化；引入独立控制 RQ credit、有界重排队列，再考虑零拷贝接收与替换缓冲。只有单 block WR 数仍是瓶颈时才增加大块 staging/多尺寸池。大块 source 需要更大本地目标或设备支持的本地多 SGE；本地多 SGE 不能合并远端不连续 source。

## 10. 验证与验收

### 首版必需的确定性验证

- 覆盖 4095/4096 字节选路边界；验证小于阈值走现有 SEND，达到阈值走 READ，READ 路径的部分发送不会切回 SEND。
- 在 `CutFromIOBufList()` 中混合多个小/大 WriteRequest；验证不跨路径合并 SGE，bytes 顺序正确，只有当前 WriteRequest 的模式会持续到它清空。
- 分别喂入单 block、多 block、非注册外部内存及许多小片段；验证输出字节完全一致、失败时输入消费正确。
- 验证普通 SEND、control-tag SEND_IMM、有 payload credit ACK 三者不会互相误判；控制帧 credit 回收与 legacy pure ACK 的 `imm_data` 数量语义独立。
- 一条 RPC 大于 slot 容量很多倍，跨多次 `CutFromIOBufList()`；两条 RPC 合并在一次调用；验证流序号与协议解析不依赖 RPC 边界。
- 同时双向传输，模拟 OFFER 接收 CQE 先于 READY、READY/OFFER/DONE 的本地 completion 延迟及 CQ 交错；验证启动暂存和 ordered mixed SEND/SEND_IMM completion 下字节偏移连续，无需新业务写入即可继续进展。
- 强制 DONE 早于 OFFER SEND completion、completion 早于 post 返回，验证 source、控制 buffer 与 context 分别释放且仅释放一次。
- 未提交的 post 失败、READ completion error、receive 重投失败、复制失败、坏 offset/length、重复消息、旧 epoch；检查不重复交付/加 credit。
- offer 后、READ 在途、READ 完成但 DONE 未到三种时点断连；分别验证本地目标保护和对端 source 隔离，不能只检查无 crash。
- 输入解析器长期持有不完整大 RPC、普通 pool 耗尽时，固定 RX/TX slot 仍受保护；确实无法分配输入内存时可正确失败和唤醒。

### 目标环境验收

记录 UMDK/provider 版本、设备、传输模式及能力查询值，验证 READ 方向、权限、完成可见性与生命周期。必须满足：

1. 4095 字节 WriteRequest 走现有 SEND；4096 字节 WriteRequest 走 descriptor+READ。另以 4097 字节或接近默认 block 有效容量的数据验证单条 `URMA_OPC_READ` 完成且数据一致。
2. 小包沿用当前 RQ/SQ ack/window 语义；控制 SEND_IMM 与空 credit ACK 可区分，控制帧长度始终不超过 128 字节。
3. 64 KiB、1 MiB 等大 WriteRequest 能正确传输；这些用例不要求一次 READ 完成，不得把 IOBuf/RPC 总长当作单 WR 长度证据。
4. 正常运行后 lease、SQ/READ outstanding 和 credit 恢复；断连时没有提前复用 DMA 内存，隔离计数符合设计。
5. 单向/双向 RPC、polling/event 模式均持续进展，并能正常关闭与重连。

增加按小 SEND/大 READ 分类的 payload 字节数与 WR 数、READ 长度分布、控制 SEND 字节数/WR 数、in-flight lease、credit 与隔离字节计数，便于证明选路及目标达成。吞吐、CPU、延迟按大小分桶与当前 SEND 基线比较；短包路径保留当前成本，大包路径增加 descriptor/DONE 和接收复制，解除 4 KiB 限制并不自动减少 WR 数或提高性能。

## 11. 本轮评审调整

| 原方案问题 | 本轮修正 |
|---|---|
| RPC message/chunk 计数要求传输层知道消息边界 | 改为每方向字节流 `seq + stream_offset`。 |
| 首版同时引入并发 READ、重排、独立 credit 通知等 | 收敛到 window=1，READY 初始化、DONE 携带下一 credit；并发扩展单列。 |
| 原方案所有大小都走 READ，未保留小包低延迟路径 | 每个 WriteRequest 小于 4096 字节沿用现有 SEND/RECV；大包选 READ 并在部分发送时保持选择。 |
| 零拷贝目标交给解析器后，再申请替换 buffer 的进展条件未落实 | 首版固定 RX slot 复制交付，显式保证大 RPC 可继续读取。 |
| 非 pool IOBuf 直接报错或尝试运行时 TCP fallback | 预留同一注册池的 TX bounce slot，覆盖常见输入和小片段合并。 |
| “突破 4 KiB”缺少可核对条件 | 要求真实单条 READ 长度超过 4096；区分用户环境限制、IOBuf block 和 READ 能力。 |
| 所有 completion 都检查 opcode，SEND 与 source 回收关系不清 | 本地完成按 context 分派；明确 SEND buffer 与 source lease 的独立寿命。 |
| 控制帧与 legacy imm credit ACK 容易混淆 | 给 control-tag SEND_IMM 单独解码；控制 payload 走现有 RQ credit 回收，pure ACK 保持原义。 |
| 错误回收被推迟到性能阶段，local drain 被当成 remote source 安全条件 | 纳入首版；单独处理远端 READ 尚可能访问本端 source 的隔离要求。 |

方案按大小保留两条数据路径：小 WriteRequest 直用 SEND/RECV，大 WriteRequest 经 SEND 控制帧触发 READ。首版聚焦于可验证的路径选取、混合流排序和单 READ 闭环；64 KiB 单 WR、零拷贝及性能调参不作为前置条件。
