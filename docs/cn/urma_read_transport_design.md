# UrmaTransport 大块单 READ 数据面设计

更新日期：2026-10-17。代码基线：`urma-transport-rw-dev` / `4f20af82`。
本文替代此前“复用约 8KiB IOBuf block 验证 READ 流程”的设计，作为下一轮实现依据。
当前工作区已按本文实现大块单 READ 路径及相关控制/资源保护；实现和构建状态见第 10 节。
本文不代表硬件性能或 provider 生命周期验收结果。

## 1. 目标与范围

首版采用 **独立分配的大块注册缓冲区 + 单 OFFER + 单条 READ + 单 DONE**。
目标是解除 READ 长度与普通 IOBuf block 容量的绑定，减少 2MiB/4MiB
传输的分块次数和串行 OFFER/DONE 周转，尽量复用已有 transport 状态机。

- 小于 4096 字节的待发送 WriteRequest IOBuf 沿用 SEND/RECV；达到或超过
  4096 字节走 READ，部分发送后保留原路径，直至该 IOBuf 消费完毕。
- 每端预留一个 TX source slot、一个 RX destination slot；每个方向最多
  一个未确认 OFFER、一条在途 READ。两端可同时发送，各方向独立推进。
- 一个 OFFER 描述本次完整连续区间，对应一条 READ；“整块”不等于整条 RPC。
  超过本次长度上限的字节流，等 DONE 后继续下一轮 OFFER。
- 保留 TCP 建链、一个 pool segment 的注册/导入、现有 56 字节控制帧、
  seq/stream offset、InputMessenger 和 socket 写侧背压接口。
- 首版保留发送整理拷贝和接收追加拷贝；不引入多 slot、并发 READ、批量
  descriptor、WRITE、零拷贝 READ 交付、RPC 序列化层改造或新 Jetty。
- 两端部署相同实现，暂不增加版本能力协商；已有 READY 中的长度公布继续使用。

用户报告的 2MB/4MB 高时延来自社区基线 `36c2d12e`，不是当前 READ 版本。
“4KiB SEND 限制”是目标环境提供的约束，社区发送函数未硬编码此值。
本方案不将操作次数减少直接换算为实测提速倍数。

## 2. 当前实现与变更对应关系

| 位置 | 当前逻辑 | 本轮设计 |
|---|---|---|
| `urma_helper.cpp::InitPool()` | mmap 一个区域、注册一个 segment，按普通 block 划分 | 同一 mapping 中增加大块子区，仍只注册一个 segment |
| `PoolAllocate/PoolDeallocate` | 普通 IOBuf 分配器，超大申请可能 fallback | 普通分配策略保持；增加专用大 slot helper，严格隔离两类空闲表 |
| `MakeLocalParsedHello/ImportPeer` | 交换并导入整个 pool segment | 保持报文结构；seg_len 自动覆盖新增子区 |
| `AllocateResources()` | TX/RX 由 IOBuf stream 分配；READ 上限取普通 block 与设备上限最小值 | TX/RX 从大块子区分配；接收上限取 RX 容量与设备有效上限 |
| `CutFromIOBufList()` | copy_to TX，发 OFFER，等待 DONE 后继续 | 保留此流程；发送长度改由 TX 容量、peer 上限和剩余字节数决定 |
| `PostRead()` | 一对 SGE、一条 READ、context kind=3 | 保留，校验大块完整范围，WR 长度不超过本端 READ 上限 |
| `HandleCompletion()` | READ 完成后复制到输入 IOBuf、发送 DONE | 保留，补齐复制失败处理和控制消息背压 |
| `Reset/DeallocateResources/GlobalRelease` | 在途 IOBuf 隔离，随后销毁资源 | 纳入大 slot 所有权、隔离预算及 provider 静止条件 |

对应代码：[endpoint](../../src/brpc/urma/urma_endpoint.cpp)、
[endpoint 状态](../../src/brpc/urma/urma_endpoint.h)、
[helper](../../src/brpc/urma/urma_helper.cpp)、
[helper 接口](../../src/brpc/urma/urma_helper.h)。

## 3. 长度、参数与内存预算

### 3.1 首版长度规则

定义 `C_tx/C_rx` 为本端大 slot 的有效容量；配置首版令二者相同。
本端 READ 能力仅约束本端发起的 READ，不能误用于限制对端发起的读取。

```text
本端 READY.max_chunk = min(C_rx, 本端设备/provider 有效 max_read_size,
                           SGE/控制帧 length 字段上限)
发送 OFFER.length = min(当前 WriteRequest 剩余字节数,
                        C_tx, peer.READY.max_chunk)
接收验证：0 < OFFER.length <= 本端 READY.max_chunk
```

`_read_max_chunk` 保留为本端公布的单 READ 接收上限；
`_read_peer_max_chunk` 保留为对端公布的上限。新增本地 slot 容量字段，
发送长度不再取 `min(_read_max_chunk, _read_peer_max_chunk)`。
这样两端设备能力不同时，不会把本端 READ 发起上限错误施加到本端 TX source。

能力为零或未知时沿用明确失败处理，不能解释为无限大。目标环境应允许
大于 4096 字节的单条 READ；只能读到 4096 字节的设备不能通过本方案性能目标验收。
若有效上限仅 256KiB，即使 slot 为 1MiB，也只能每 OFFER 读 256KiB；
首版按多轮 OFFER 传输，不在一个 OFFER 内拆成多条 READ。

IOBuf 总长度可大于一个 block；普通 block 的容量和 READ 的硬件上限是两回事。
不会修改全局 `SetDefaultBlockSize()` 或放大 `urma_buffer_size` 来实现大包路径。

### 3.2 配置（本轮已实现）

| 配置 | 建议起点 | 语义 |
|---|---:|---|
| `urma_read_slot_size` | 1MiB | 单 slot 有效容量；初始化时固定，页对齐 stride 单独计算 |
| `urma_read_pool_size` | 64MiB | 大块子区总预算，包含空闲、连接占用和隔离 slot；部署时按连接数调整 |

采用单一 slot 规格、一个互斥锁和空闲索引表即可，不增加通用内存分配框架。
连接建立前原子取得一对 slot，或在第二个分配失败时完整回滚。两块都准备好
才允许公布 READY；连接内一直持有它们，不按每个 RPC 反复分配。

默认示例最多容纳 32 个连接端点的大 slot，不包含普通 pool 和控制缓冲。
进程总注册内存为“原普通池 + 大块子区 + 对齐余量”。配置时必须检查乘法、
加法、对齐溢出以及 segment/provider 注册限制，注册失败明确报错。
已启用 URMA 的连接不得因池耗尽临时混发 TCP，也不得偷偷换成未注册 malloc 地址。
建链阶段能否回退 TCP，继续服从现有连接策略。

对于连接少、目标 payload 为 4MiB 的场景，可评估 4MiB 加协议头余量的 slot；
每端每连接 TX+RX 将占用 8MiB 以上，需要同步提高预算或降低连接数。
协议头、attachment 和元数据大小不固定，任何固定余量都不能保证每条 RPC
恰好一条 READ；超出时正常继续下一轮。没有必要为凑满 slot 等待后续写入。

## 4. 大块注册缓冲区布局与所有权

“独立”指独立分配、容量和回收，不要求增加第二个 MR：

```text
同一 mmap / 同一 g_pool_seg / 同一 hello.seg_len
+--------------------------------+---------------------------+
| 普通 IOBuf block 子区            | READ 大块子区              |
| 原分片 free_lists 与 in_use      | 固定大小 slot + 独立空闲表 |
+--------------------------------+---------------------------+
```

在 `InitPool()` 注册前确定两个子区边界，普通 pool 原有 block 数量保持不变。
必须显式区分普通区大小、READ 区起点/大小与整个注册区大小。

- `GetPoolSegFor` 和 `GetPoolSegmentOffset` 仍识别整个注册区；每个 DMA
  地址检查完整 `[offset, offset + length)` 范围，不能只验证首地址。
- 普通 `PoolDeallocate` 仅接受普通区有效 block。落入 READ 子区的地址
  误传入此函数时应拒绝/诊断，既不能放入普通空闲表，也不能交给原 allocator free。
- 新 helper 返回 slot 地址、有效容量及其所有权。可用 `append_user_data`
  与专用 deleter 包装进现有 `_read_tx_buf/_read_rx_buf`，使正常清理和隔离
  继续持有 IOBuf 引用；检查包装失败并归还刚取得的 slot。
- 大 slot 的页对齐 stride 与有效容量分开；IOBuf 用户数据包装的元数据
  不占 DMA 区间，不再扣普通 `IOBuf::Block` 头。禁止用普通 stream 分配大 slot。
- 发布 OFFER 后 TX 内容保持不变，直至匹配 DONE；RX 内容在 READ 完成
  且已复制交付之前保持专属。首版统一使用 TX 整理拷贝，不增加直接引用 source 分支。
- READ arena 与其 allocator 元数据必须活过所有引用和 DMA；GlobalRelease
  不能在隔离引用仍可能被访问时先 unregister/munmap。

同一 segment 的方案复用当前地址发布方式；它仍然是对整个注册段授权，
不提供每个 slot 的硬件访问隔离，也不扩大为新的跨信任域协议。

## 5. 控制协议与正常流程

继续使用当前 56 字节编码和 `URMA_READ_IMM_TAG`：

| 消息 | 当前字段的使用方式 |
|---|---|
| READY | sender_epoch 非零；target_epoch 固定为 0；max_chunk 公布本端单 READ 接收上限；收到后授予一个 data credit |
| OFFER | epoch 对、seq、stream_offset、segment_offset、length；max_chunk 字段为 0 |
| DONE | epoch 对、原 seq/stream_offset/length；segment_offset 为 0；max_chunk 字段为 1，表示归还一个 data credit |

字段名保持现状，避免另加 wire 字段；接收端按 type 解释末尾字段，不混淆
READY 的长度和 DONE 的 credit。控制 SEND_IMM 消耗已有远端 RQ credit；
READ 不消耗远端 receive WR；纯 credit ACK 保持原语义。

聚合设备开启 MSN 时，`SEND_IMM` 的应用立即数只保留低 21 位。因此控制
标记放在 bit 16～20（`0x001A0000`，掩码 `0x001F0000`），消息类型放在
低 16 位；纯 credit ACK 的值小于 4096，与控制标记不相交。旧标记
`0xA5000000` 会被该设备截去，导致 READY 的 `imm=1`、56 字节控制帧被
误判为纯 credit ACK。接收端先以 `0x001FFFFF` 提取应用立即数，再分流
控制消息和 credit ACK。该 wire 编码变更要求连接两端一起升级。

```text
A：TX/RX/控制资源就绪 -> READY                B：同样完成 READY
A：copy_to(TX, length) -> OFFER ------------> B：验证、保存 descriptor、投递 READ
A：TX 保持不变                 <----------- B：READ(remote TX -> local RX)
                                               READ CQE 成功
                                               复制到临时 IOBuf，再整体追加输入流
A：收到 DONE，归还 data credit <------------ B：DONE（可因资源不足短暂 pending）
A：下一 OFFER 或小 SEND                         按现有入口分发 payload
```

输入路径继续按 transport WriteRequest 的编码字节数选路，不解码 RPC body。
不跨多个 WriteRequest 主动拼成一个 OFFER，复用当前逐项处理逻辑。
检查 `copy_to` 实际长度；OFFER 确认提交后才 `pop_front`，未提交不得消费输入。

接收复制使用临时 IOBuf：全部复制成功再转移到 `read_buf()`，转移操作也须
按其 API 检查失败，完成前不触发分发，避免部分追加失败后交付残缺数据。
完整追加成功才推进 seq/offset 并建立
pending DONE；控制发送 EAGAIN 不撤销已收到的数据，也不能重复追加。
控制 receive 对新增业务字节的贡献为 0；仅普通 SEND 和成功 READ 交付返回 payload 长度。

## 6. 对现有状态机的最小补充

### 6.1 保留单在途结构

复用 `_read_tx_offer_pending`、`_read_rx_active`、seq/offset/length 和
现有 typed WR context；不增加子 READ 数组或通用重排队列。
实现时增加少量状态明确以下区别，不能仅依赖一个 active 布尔值：

| 方向 | 阶段 | 必须持有的资源 |
|---|---|---|
| TX | 准备/正在 post OFFER | TX slot、提交引用、预留 credit、控制 buffer |
| TX | OFFER 已接受，等待 DONE | TX source lease；控制 buffer 独立等 SEND CQE |
| TX | DONE 已到但提交尚未退出 | 保留提交引用，禁止下一 writer 过早覆盖 TX |
| RX | 已收 OFFER，等待 SQ | 固定 descriptor、RX slot；尚未有 READ DMA |
| RX | READ 在途 | RX slot、READ context、imported segment |
| RX | 已复制交付，DONE 待发送 | 不重复交付；保留完整 DONE 字段直到被接受 |

OFFER 的 SEND CQE 只释放控制 buffer/SQ slot，DONE 才结束 source lease。
DONE 可能先于本地 OFFER SEND CQE。两者独立记账；首版为简化单 OFFER
槽位管理，下一 OFFER 等当前 OFFER 的 SEND CQE、DONE 以及提交退出均满足后再发布。
不能用 SEND 完成代替远端 READ 已结束的确认。

### 6.2 READY、排序和 credit 进展

1. READY 使用固定 target_epoch=0。READY 被 provider 接受后才公布本端发送
   就绪；与 peer READY、业务 writer 使用一致同步，避免先发 OFFER 后发 READY。
   completion 可能早于 post 返回，状态和引用需在 post 前准备。
2. 保留一个启动/前序 SEND 等待项，容纳尚不能验证的 OFFER；待 peer READY
   和前序字节 offset 就绪后执行。额外未授权 OFFER 为协议错误。
3. 混合 SEND/SEND_IMM 的接收顺序仍依赖目标 provider 的 ordered completion
   能力与现有 `comp_order/place_order` 配置。不能为追求并发直接取消排序标志。
   若目标设备不满足该前提，必须补小包显式序号/有界重排，超出本轮最小改动范围。
4. 每方向 data credit 仍为 0/1，独立于 SQ/RQ credit。统一保护所有提交来源
   的资源预占，涵盖 writer、READY、DONE、READ 与 ACK；`ReserveSqSlot()`
   保护位图并不足以避免窗口先检查再扣减的竞争。
5. 从现有普通 SQ/RQ 可用额度划出控制进展余量，至少覆盖双向阻塞时的 READ
   和 DONE；现有 `RESERVED_WR_NUM` 是 pure ACK 预算，不能直接挪用。
   额度需按 READY/DONE 最大重叠及本地完成延迟核算，并校验最小队列深度。
6. READ/DONE/READY 资源暂缺属于 pending，不等同连接错误。保持有界状态，
   在 SQ CQE、RQ credit 到达、READY 到达时调用轻量 progress；不等待新业务
   写入、不忙轮询、不另建进度线程。progress 有唯一执行者/重入保护。
7. 接收控制帧后及时重投 RQ 并归还 credit。检查现有 `SendAck` 批量阈值：
   低流量下不足阈值的尾部 credit 也必须能刷新，避免 data credit 已归还却
   没有 RQ credit 发下一控制帧。只发送实际已重投的数量，pure ACK 不再 ACK。
8. `IsWritable`、业务提交和唤醒使用相同条件；资源恢复时唤醒 writer。
   InputMessenger/用户回调不得在提交状态锁内执行，pending 控制推进不依赖业务回调返回。

控制 buffer 必须是保证连续的注册小块。当前 `append(frame, 56)` 后取
`fetch1()` 提交一个 56 字节 SGE 不保证连续，应改为专用连续 slot 或经验证
的完整连续分配，持有至对应 SEND CQE。不要为每条控制消息占一个 MiB slot。

### 6.3 提交失败与断连

- post 前预留资源并建立稳定 context；以部署 API 的返回值和 `bad_wr`
  区分明确未接受、已接受和状态不明。只对明确未接受者回滚；不确定时关闭
  连接并保留可能被 DMA 使用的内存，不盲目重发或复用。
- READ 错误 CQE、非法长度/epoch/seq、复制失败均关闭连接，不发成功 DONE。
  校验 stream offset 与 VA 加法溢出、完整 segment 范围、重复 DONE/credit。
- 关闭先禁止新 post，再同步提交者和 poller，按 provider 依赖顺序 drain、
  解除导入和销毁资源。必须分别证明本端 READ 不再写 RX、对端 READ 不再读 TX。
  本端 CQ 空了不能单独证明对端 source lease 已结束。
- 无法证明 DMA 静止的 slot 继续隔离，不按超时直接释放。隔离 slot 占用
  固定 arena 预算，不从其他未注册内存补齐；耗尽则拒绝新 READ 连接资源。
  隔离对象/控制 buffer 也要有预算和准入限制，不能维持无限增长的全局 vector。
- 仅隔离 IOBuf 不足以证明 Jetty、CQ 和 imported segment 可立即销毁。
  provider 的 drain/撤销远端访问保证未核实时，这仍是上线阻塞项，而非已解决事项。

## 7. 必修项与变更边界

大块优化和以下既有问题修复一并验收；两者可拆成独立提交，便于审阅：

| 必修项 | 当前实现风险 | 最小处理方向 |
|---|---|---|
| 控制帧连续性 | 单 SGE 可能跨 IOBuf block 读错数据 | 连续注册控制 buffer，独立生命周期 |
| READY 时序 | target_epoch 编码与检查不一致，过早 ready | 固定规则、同步状态、启动暂存 |
| SQ/RQ 资源预占 | 多提交者 check/sub 竞争；EAGAIN 导致断连 | 原子预占或统一串行提交、有界 pending 和进展余量 |
| 提交与失败事务 | 输入/credit 提前消费或错误退款 | accepted 边界、稳定 context、可证明的回滚 |
| 复制与统计 | append 失败仍 DONE；控制字节算入业务 | 临时 IOBuf 完整交付，控制返回 0 |
| 断连回收 | 无界隔离、资源销毁缺少静止依据 | 固定预算、引用保护、provider 生命周期验证 |

主要源码改动限制在 `urma_helper.h/.cpp`、`urma_endpoint.h/.cpp`，
补充 `test/brpc_urma_unittest.cpp`。如 event 模式回调内联阻塞 CQ 进展，
单列适配 `urma_transport.cpp` 的最后消息异步分发，保留显式 inline 运行模式语义。
不要为满足“四个文件”而省略正确性必需改动。

当前已移植的 CQ 按批分发和 pool 线程分片保留。fengweiqi 的 SEND 链式批量
post、polling budget、writer wake 合并可作为独立优化，不是大块 READ 的前置依赖。
不整体引入其 bulk WRITE_IMM、controller/序列化/握手改造。

## 8. 收益预期与验收计划

### 8.1 可静态核对的操作次数

以下按待传输字节流恰好为 2MiB/4MiB、slot 与设备允许表中长度计算，
不含 RPC 头、READY、pure ACK、错误路径；不跨 WriteRequest 合并时分别取整。

| 单 READ 有效长度 | 2MiB 的 OFFER/READ/DONE 轮数 | 4MiB 的轮数 |
|---|---:|---:|
| 当前默认 8160 字节 | 258 | 515 |
| 256KiB | 8 | 16 |
| 1MiB | 2 | 4 |
| 4MiB | 1 | 1 |

每轮为两个控制 SEND 和一个 READ，两端合计 3 个 post WR、5 个 CQE
（两个 SEND 各有发送/接收完成，READ 有本地完成）。仍然每方向 stop-and-wait，
收发整理复制总字节量约为 payload 的两倍；降低轮数后，复制、NUMA、解析
与回调调度可能成为主要开销。单条大 READ 不保证比设备最优流水线更快。

### 8.2 验证清单（执行情况见第 10 节）

- 单测：普通区/READ 区边界、对齐和溢出、slot 对分配回滚、包装失败、
  自定义 deleter、池耗尽及隔离不复用；普通 deallocator 不误收大 slot。
- 协议：4095/4096/4097 字节、slot 上限前后、超长 IOBuf、非注册源、
  碎片源、多个 WriteRequest、两端 READ 上限不同、RPC 跨多轮。
- 时序：READY/OFFER 交错、DONE 早于 OFFER SEND CQE、CQE 早于 post 返回、
  双向流、SQ/RQ 耗尽、低流量尾部 ACK、无新业务写入时 pending 仍能推进。
- 故障：明确拒绝/接受不明的 post、错误 CQE、复制失败、非法控制字段、
  不同时点断连与重连；验证数据不重复交付、credit 不重复返还、DMA 内存不提前复用。
- 设备：记录 UMDK/provider、模式、max_read_size 和排序能力；证明单条
  READ 大于 4096 字节并数据一致，分别验证 256KiB/1MiB/4MiB（能力允许时）。
- 构建：实现后同步到 brpc-dev，使用最多 `-j4` 构建并运行针对性测试；
  mock 测试/编译成功不能替代设备生命周期和数据传输验证。
- 性能：相同 Release、硬件/NUMA、连接数和并发，比较社区基线、当前小块
  READ 与修复后的大块单 READ；已有优化 SEND 版本可增设对照，无需先完成全部移植。
  覆盖 event/polling、单/双向大包、QD=1 和业务并发，记录平均/P50/P99、
  吞吐、CPU、实际 WR 长度/数量、OFFER→DONE 时间、credit 等待及隔离内存。

## 9. 后续扩展与参考依据

只有在单 READ 上限或实测吞吐成为瓶颈后，再考虑“同一 OFFER 内多个 READ”。
届时需要将 READY 的可接收 OFFER 上限与单 WR 上限分开，增加子 READ 完成
汇总与失败处理；仍可保持一个远端 lease 和一次 DONE。这不是首版隐含能力，
也不能在当前完成回调中直接循环 post 多条 READ。

本轮相对旧设计的关键调整是：大块子区取代普通 IOBuf slot；固定一 OFFER
一 READ；复用现有 segment 和控制格式；设备不足时缩短 OFFER；把必要的
控制进展与生命周期修复列入交付条件。并发窗口、零拷贝和 WRITE 均后置。

技术材料：

- [openEuler URMA User Guide](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20User%20Guide.ch.html)：区分 max_read_size、max_write_size 与消息长度能力。
- [openEuler URMA API Guide](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)：READ/WRITE、SGE、完成与资源接口；以部署 SDK/provider 为准。
- [Mooncake URMA endpoint](https://github.com/kvcache-ai/Mooncake/blob/main/mooncake-transfer-engine/src/transport/kunpeng_transport/urma/urma_endpoint.cpp) 与本机 Mooncake 的既有审阅：参考远端 src、本地 dst、WR context 完成分派；不代替 brpc 的 credit、字节流排序和回收设计。
- [当前代码复审记录](urma_read_transport_review.md)：保留具体问题和 fengweiqi 优化核对；其中旧的多 slot 优先路线由本文替代。

## 10. 本轮实现状态与构建记录

已在当前工作区实现，尚未提交 Git：

- 在原注册 segment 内追加大块子区，固定规格 slot 成对分配，独立 deleter
  回收；配置使用字节数：`--urma_read_slot_size=1048576`、
  `--urma_read_pool_size=67108864`。大小在初始化时固定。
- 保持 56 字节控制帧；发送长度采用本地 TX 容量和 peer READY 上限；
  接收端仍每 OFFER 投递一条 READ。控制帧使用三个预留的连续注册小缓冲区，
  分别供 READY/OFFER/DONE 使用，各自等待本地 SEND CQE 后复用。
- 增加每连接提交/完成互斥保护，SQ/RQ 预留两个普通额度供 READ/DONE 进展，
  原 pure ACK 预算保持独立。READ、READY、DONE 的背压保存为有界待办状态，
  CQ/credit 触发推进；控制 receive 及时刷新 credit，业务字节计数排除控制帧。
- READY 的 target_epoch 固定为零，在 post 成功后公布就绪；保存一个提前
  到达的 OFFER。DATA/READ 按字节偏移衔接，接收大块先完整复制再追加输入流。
- 正常 event 模式下最后一条消息也异步分发；显式 inline/coroutine 模式
  保持原行为，使用这些模式时仍需业务回调不阻塞 CQ。
- 异常断连时保留相关 URMA 资源对象、收发/控制缓冲及整对 READ slot；
  不确定 DMA 已停止时不归还 slot。资源删除检查返回值，失败即停止后续删除。
  隔离项占用固定大块池额度；池耗尽将使后续 READ 连接资源申请失败。
  对没有取得 slot 对就发生资源销毁失败的异常建链，禁用后续 URMA 准入。
- DebugInfo 增加 slot 容量、两端 READ 上限及 OFFER/READ/DONE 待办状态。

构建环境：brpc-dev，`WITH_URMA=ON`、`URMA_USE_MOCK=ON`。
已完成 `brpc-static` 和 `rpc_press` 编译/链接，命令为：

```sh
cmake --build build-urma --target brpc-static rpc_press -j4
```

现有目录未开启共享库，因此不以 `brpc-shared` 作为本轮验收目标。
编译仍有既有 protobuf 废弃接口及 signed/unsigned 比较警告。
首版实现时未新增或运行单元测试、端到端传输和性能测试；后续单测复审结果见第 11 节。

实际 liburma/provider 不在此次链接环境中；混合 opcode 排序、真实大 READ、
DMA 完成可见性及正常销毁语义仍须按第 8 节在设备上验证。当前没有实现
provider reset/flush 后重新回收隔离资源：隔离持续至进程退出，不能把它
描述为可长期无限重连的资源回收闭环。默认池仅支持最多 32 个连接端点
（含隔离项），上线前必须按连接数量和故障恢复要求调整并验证。

## 11. READ 单测复审与最小性能修复（2026-10-17）

审阅基于 `b83738fa` 与新增 READ 单测工作区。最初可见的 brpc-dev 报告
只有旧的 14 个用例，新增测试文件和 mock 与本地哈希不同；本轮重新同步、
构建和执行，以下结果均来自当前源码。

### 11.1 已复现并修复的问题

| 问题 | 修复前证据 | 修改 |
|---|---|---|
| 新测试无法编译 | 7 个 flag 引用缺少 `urma::` 命名空间 | 修正测试引用，保留原用例 |
| 阈值测试混淆请求路径标记和 source lease | 4096 字节输入消费完后仍要求路径集合大小为 1 | 改为输入路径标记已移除、TX credit 未归还且 OFFER 仍 pending |
| credit 不足时重复大块拷贝 | SQ/RQ 分别只剩预留额度时返回 EAGAIN，但 TX 哨兵数据已被覆盖 | 在复制前检查 OFFER 的 SQ/RQ 额度；提交互斥锁保证检查到 post 之间没有其他提交者抢占 |
| 可写后未唤醒 writer | SQ 从 2 恢复到 3、RQ 为 3 时，IsWritable 为真，但旧的 capacity/8 阈值使 butex 不变 | 完成处理使用 IsWritable 判断唤醒，统一准入和唤醒条件 |
| mock 的 READ 限制未实际执行 | 仅修改能力声明，post 仍可接受超长 READ | 校验非零长度、max_read_size、有效 target，并统计实际成功 READ WR 数 |
| 测试环境依赖执行顺序 | 旧 mock fixture 结束时无条件恢复 skip=true，可破坏已建立的注册池环境 | 保存并恢复原全局状态，初始化失败视为测试失败，避免被 SKIP 掩盖 |

生产代码仅修改 endpoint 的复制前检查和两处完成唤醒条件；没有扩大协议、
数据窗口、slot 或 wire 格式。两项生产问题均保留了修复前失败、修复后通过
的回归证据。性能收益取决于实际背压和 credit 水位，尚无设备时延数据。

### 11.2 当前覆盖与结果

- 38 个用例全部通过，其中 24 个为 READ 控制、arena 与数据路径用例；
  在用户新增用例基础上补充 9 个回归用例。
- 2MiB/4MiB 数据逐字节一致，并按实际 READ 计数检查分块；测试配置使用
  64KiB slot，因此对应 32/64 条 READ，不是单条 2MiB/4MiB 硬件 READ。
- 覆盖双向 READ、READ 小尾块、非对称读取上限、DONE 早于 OFFER 本地 CQE、
  SQ 背压后由完成事件继续 READ、RQ credit 恢复后继续 DONE 且不重复交付。
- mock 超长 READ 拒绝时 SQ 退款且不产生 CQE；CQ 为空但协议状态仍 pending
  不再被 PumpBoth 当作完成。
- `brpc_urma_unittest`、`brpc-static`、`rpc_press` 构建/链接通过；CTest
  通过；随机种子 20261、随机顺序重复 10 轮，共 380 次用例执行通过。

```sh
cmake --build build-urma --target brpc_urma_unittest brpc-static rpc_press -j4
ctest --test-dir build-urma -R '^brpc_urma_unittest$' --output-on-failure
build-urma/test/brpc_urma_unittest --gtest_shuffle --gtest_random_seed=20261 --gtest_repeat=10
```

远端日志：`/tmp/brpc-read-review-build.log`、
`/tmp/brpc-read-review-regressions.log`（两个生产问题的修复前失败）、
`/tmp/brpc-read-review-final.log`、`/tmp/brpc-read-review-final.xml`、
`/tmp/brpc-read-review-repeat.log`。

验证边界：目前使用同步 memcpy 的 link-time mock，手工投递/处理 CQE；
部分背压用例直接设置窗口并模拟完成/credit。没有覆盖真实设备异步 DMA、
event poller、并发 writer/CQ 压力、DMA 在途断连和自动解除隔离。
第 10 节的隔离预算和真实 provider 验收限制仍然适用。
