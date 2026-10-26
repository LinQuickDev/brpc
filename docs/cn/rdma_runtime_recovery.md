<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# RDMA 运行期故障恢复与 TCP 降级设计

状态：设计草案，尚未实现。本文中的接口、参数和指标均为提案。

基线为 LinQuickDev/brpc master 的
`87d498dd62d8b3b3562407cdf38ac0c475d312b3`。
本提案改变后续连接的传输选择；不承诺存量 RPC 无损迁移或 exactly-once。

## 问题与源码依据

当前握手可以回落 TCP，但 RDMA 建立后的故障主要通过连接失败处理：

| 位置 | 当前行为 | 需要补充的能力 |
| --- | --- | --- |
| `RdmaConnect::StartConnect`、握手状态机 | RDMA 不可用或协商失败时回落 TCP | 后续建链能记住运行期故障并绕过故障路径 |
| `RdmaEndpoint::PollCq` | 错误 CQE 调用 `Socket::SetFailed` | 错误分类、隔离和恢复探测 |
| `OnRdmaAsyncEvent` | PORT_ERR / PORT_ACTIVE 只确认事件 | 端口状态与连接准入联动 |
| `GlobalDisableRdma` | 修改可用状态并记录 FATAL | 独立、非致命的运维降级控制 |
| `CutFromIOBufList` | 窗口耗尽返回 EAGAIN；post_send 失败按不可恢复处理 | 保留背压语义，检测有未完成发送时的长期无进展 |
| `BringUpQp` | timeout=14、retry_cnt=7、rnr_retry=0 | 明确错误语义及受限配置，而非把所有错误归为超时 |

源码分别位于 `src/brpc/rdma/rdma_endpoint.cpp`、
`src/brpc/rdma/rdma_helper.cpp` 和 `src/brpc/rdma_transport.cpp`。
RPC deadline、verbs ACK 重试耗尽、RNR 重试耗尽和本地 SQ 背压是不同事件。

## 目标和兼容性

1. 本地 RDMA 端口故障后，快速停止该端口的新建链并使相关连接失败。
2. 后续连接优先使用可达的备用 RDMA 路径，无可用路径时通过新 TCP 连接恢复。
3. 对故障路径设置冷却和有界恢复探测，避免每次 RPC 都重新尝试故障 RDMA。
4. 提供运行时 TCP-only 开关，并明确存量连接的处理方式。
5. 给出 SQ 停顿、RNR 和链路错误的独立策略、指标和测试标准。

自动恢复默认关闭；启用前保留现有单设备、单端口和握手协议行为。
TCP fallback 遵循现有协议与 SSL 限制，不绕过认证，不隐式降低安全配置。
发布第一阶段只实现单设备故障隔离和 TCP 恢复；跨 HCA 主备在资源模型改造后提供。

## 恢复边界

故障连接执行正常 SetFailed、唤醒写等待者并进入回收流程。
策略先发布故障状态，再传播连接失败，使同时发生的新建链看到最新路由。
已经建立的 RDMA 连接不改为在原 TCP fd 上发送 RPC 数据。
该 fd 在握手后有独立语义，直接切换会造成消息边界、解析状态和顺序问题。

只由现有 Controller、RetryPolicy、max_retry 和 RPC deadline 决定是否重试。
新增策略不增加重试配额、不重置 deadline、不自动重放已 post 的 WR。
请求可能已被服务端执行时保留现有错误语义；有副作用的业务仍需幂等键或去重。
恢复策略只保证后续连接避开故障 RDMA，不保证正在执行的调用成功。

## 组件与资源所有权

在 `src/brpc/rdma/` 新增 `RdmaRecoveryPolicy`，管理准入、冷却和探测。
`RdmaTransport` 在建链前获取一次不可变的 route lease；
`RdmaEndpoint` 的 QP、CQ、GID、port 和注册内存绑定到该 lease。

状态分为两个作用域：

- 本地端口键：device identity、port number、资源 generation。
- 对端路径键：本地端口键、远端地址、传输模式、协商相关身份。

QP 错误先影响连接或对端路径，不根据一个对端失败就禁用整块网卡。
新连接可读取端口状态和对端状态的不可变快照；数据发送热路径不加全局锁。
有界缓存使用过期时间和容量限制；只逐出无活动 lease、无探测的对端状态。
端口配置固定在启动时，运维开关在运行时更新策略 epoch。

## 状态机与故障分类

端口和对端路径分别维护 HEALTHY、COOLDOWN、PROBING、DISABLED。
单调时钟驱动冷却；每个状态更新带 generation 和 policy epoch。

| 输入 | 状态变化 | 行为 |
| --- | --- | --- |
| 本地 PORT_ERR | 端口进入 COOLDOWN | 禁止新 RDMA；使绑定该 generation 的连接失败 |
| PORT_ACTIVE | 保留隔离，记录链路恢复 | 不立即恢复全量流量，等待冷却和探测 |
| QP/CQ 错误 | 连接失败，必要时对端路径进入 COOLDOWN | 不据此断定本地端口故障 |
| RETRY_EXC | 对端路径进入 COOLDOWN | 原因可能是路径、对端或网络；不累计为本地端口故障 |
| RNR_RETRY_EXC | 对端路径进入 COOLDOWN | 记录接收资源/流控异常，不切坏整个本地端口 |
| 正常 SQ/RQ 窗口为零 | 不改变健康状态 | EAGAIN 背压，等待 completion / credit |
| post_send 失败 | 连接失败，按 errno 分类 | 保存原始错误；不直接重投 WR 或循环忙重试 |
| 运维 TCP-only | 准入进入 DISABLED | 新建链直接选择 TCP，按下文处理存量连接 |
| DEVICE_FATAL | 对应资源域永久 DISABLED | 仅在安全排空得到验证后支持非致命降级，否则保留现有 FATAL |

禁止把 CQ flush 连锁错误重复计为多次独立故障。
同一 socket generation 的失败使用一次性报告，过期事件不得修改新一代状态。
内存注册错误、远端访问错误和本地保护错误保留独立诊断；不以自动重试掩盖编程错误。

## 冷却与恢复探测

首次隔离使用基础冷却时间，连续探测失败指数退避至上限，并加入有界 jitter。
冷却期所有新连接跳过该路径，选择健康候选路径或 TCP。
冷却结束不等于恢复：原子准入最多一个新 RDMA 连接作为探测，其余请求继续走 TCP。
PORT_ERR 后还必须先确认本地端口 ACTIVE，才能发放探测 lease。

握手成功只能证明建链成功。探测连接收到有效 RDMA 数据、且至少一次发送获得完成后，
才把路径恢复为 HEALTHY；不能以 TCP 成功证明 RDMA 已恢复。
不构造会产生业务副作用的探测 RPC；使用正常业务流量并受原调用 deadline 约束。
探测在预算内失败或没有足够证据时重新冷却，不长期占用探测名额。
一条探测连接同时持有端口和对端 lease，成功/失败只释放对应 epoch 的名额。

## 运行时控制与连接复用

提议 `RdmaRecoveryMode { LEGACY, AUTO, TCP_ONLY }` 及进程级原子 setter/getter。
setter 校验输入、增加 epoch，并唤醒控制任务；不直接销毁 QP 或 MR。
运维系统可调用 API，初版不新增可远程修改状态的 HTTP 接口。

TCP_ONLY 的完成语义是“停止新 RDMA 准入并返回新策略 epoch”。
它不表示所有旧连接已排空；单独暴露旧 epoch 的活动连接数。
默认允许健康存量 RPC 完成，之后将连接退休并阻止新 RPC 复用。
对于持续复用或长连接，必须在请求获取 socket 时检查退休位，不能只在握手时检查。
可选强制模式使旧连接失败，但不自动重放其中请求；其影响须由调用方显式选择。
重新切回 AUTO 后通过 PROBING 恢复，不瞬间全量重建 RDMA 连接。

SocketMap / 连接池的复用键需区分路由模式、端口资源域和 policy epoch。
同一逻辑 Channel 下，失败的 RDMA socket、临时 TCP socket 和探测 socket 不得误复用。
TCP fallback 必须创建明确的 TCP transport/socket，不再进入 RDMA 初始化和握手。
现有 socket 复活路径重新获取 lease，不把旧 endpoint 的路由状态带到新 generation。
本地 RDMA 故障不得直接把仍可经 TCP 访问的服务实例长期标记为不可用；
需要验证 SetFailed 与健康检查、负载均衡惩罚的现有交互，保持单次 RPC 的错误通知。

## 多端口主备与注册内存

候选路径启动时配置并验证；顺序选择健康主路径，再选择健康备用路径。
不在单个 QP 上修改活动路径，也不把旧 QP 搬到另一 HCA。
切换意味着建立新 QP，重新交换正确的 GID/QPN 等信息。
本地备用端口不一定能访问同一个对端，探测失败应隔离该对端路径。

当前全局 context / PD / MR / lkey 模型不能直接支撑跨 HCA 切换。
跨 HCA 必须先引入 `RdmaDeviceContext`：每个 context 拥有 PD、事件源及 MR 注册表，
IOBuf 内存块按目标 PD 查找 lkey，不能把主设备的 lkey 用于备用设备。
预注册备用设备需要容量预算及失败回滚；用户注册内存也需按设备注册和保活。
不能安全注册到备用 PD 的 buffer 走明确的受控复制路径或 TCP，不能使用无效 lkey。
同 HCA 不同端口若共享 PD 仍需验证 GID、路由和对端可达性；不默认等同于跨 HCA。

## SQ 停顿和 RNR 策略

SQ 窗口耗尽保留 EAGAIN。可选 watchdog 只跟踪有 outstanding WR 或待发送且被窗口阻塞的连接。
使用单调时间记录最后发送完成及 credit 前进，不以 idle 时长判断故障。
待发送数据、未完成 WR 和 credit 的状态分别记录，避免接收数据持续到达掩盖发送停顿。
超过无进展阈值后使该连接失败并隔离对端路径；不导致整个端口隔离或进程退出。
RPC 超时与传输 watchdog 独立，后者不延长任何调用的 deadline。

ACK timeout 指数、retry count 和 RNR retry 保持现有值作为默认，分别校验 verbs 范围。
首版不提供无限 RNR 重试：它会把错误变成无法及时检测的停顿。
允许的有限 RNR 重试需结合收包 buffer 补充延迟测试；调参不能替代滑动窗口正确性。
watchdog 默认关闭，硬件和拥塞测试确定阈值后再建议生产开启。

## 提议配置与可观测性

以下名称尚未实现。配置启动时校验，运行时只允许通过专用 API 更新恢复模式。

| 参数 | 初始建议 | 约束 |
| --- | --- | --- |
| rdma_recovery_mode | legacy | legacy / auto / tcp_only；auto 显式启用 |
| rdma_path_cooldown_ms | 5000 | 正数，使用单调时钟 |
| rdma_path_max_cooldown_ms | 60000 | 不小于基础冷却时间 |
| rdma_recovery_probe_timeout_ms | 1000 | 正数；探测调用仍遵守更短的原 deadline |
| rdma_send_stall_timeout_ms | 0 | 0 关闭；启用时必须为正数 |
| rdma_backup_paths | 空 | 启动时配置；仅在设备资源域改造后支持 |

暴露连接失败原因、RDMA-to-TCP 新建链次数、隔离次数、探测成功/失败、
端口状态、旧 epoch 活动连接数、无进展发送时长及 SQ/RQ credit。
指标避免使用无界远端地址作为 bvar 名称；对端细节放入有界诊断视图。
日志包含 socket generation、device、port、WC status/vendor_err、errno 和策略 epoch，
对相同故障做限频，并保留首次根因，避免被 WR_FLUSH 覆盖。

## 并发、回收与停机

控制任务只持有弱 socket 标识和带代际的 route lease；执行时重新寻址并校验 generation。
状态发布使用明确的 acquire/release 配对；事件回调不在持有策略锁时调用 SetFailed。
防止 SetFailed、复活、探测定时器和端口事件之间形成锁顺序循环。
故障后先停止准入和 WR 提交，再停止对应 CQ 处理并按现有安全流程 reset/drain QP/CQ，
最后释放发送 buffer、MR、PD 和 context。完成状态未知的 DMA 内存不能提前复用。
停机取消探测和 watchdog、等待回调退出，再销毁设备资源。
DEVICE_FATAL 的资源回收必须单独验证；无法证明安全排空时不移除当前 FATAL 保护。

## 分阶段交付

1. 单设备策略、错误分类、端口事件、冷却/探测、TCP 新连接恢复及受控运行时模式。
   包含 SocketMap/连接池退休与复用边界、代际隔离和基本指标。
2. 有界发送无进展检测、有限 RNR 参数校验与故障注入；默认保持现有背压行为。
3. 多设备资源域、按 PD 注册内存及候选路径选择，完成后开放多端口/跨 HCA 主备。

每阶段单独实现和验证；阶段一不宣称支持跨 HCA 或 DEVICE_FATAL 无损恢复。
本设计 PR 只增加文档；后续实现 PR 才启用功能和增加对应测试。

## 验证和验收标准

| 层级 | 场景 | 验收条件 |
| --- | --- | --- |
| 策略单测，注入时钟 | 冷却、退避、epoch 变化、并发准入 | 冷却零 RDMA 准入；到期最多一个探测；旧结果无效 |
| 连接测试，mock verbs | PORT_ERR 与并发建链、复活竞争 | 受影响连接失败并唤醒等待者；后续连接使用 TCP |
| 错误分类 | RNR、RETRY_EXC、WR_FLUSH、post_send errno | RNR 不隔离本地端口；连锁 flush 不重复计数 |
| 背压测试 | SQ/RQ 为零后 CQ/credit 恢复 | EAGAIN 可恢复，无误降级、忙循环或丢失唤醒 |
| watchdog | idle、持续接收但发送停顿、慢恢复 | idle 不触发；真实发送停顿有界失败；恢复更新进度 |
| RPC 语义 | 部分 post 后失败、服务端已执行 | 不在 transport 中重放；deadline 和重试配额不增加 |
| 复用与开关 | TCP_ONLY、AUTO、旧池连接、持续流量 | 新请求不复用退休 RDMA；TCP 成功不算 RDMA 探测成功 |
| 兼容性 | 开关关闭、新旧对端组合 | legacy 行为及现有握手格式不变 |
| 生命周期 | reset/revive、晚到 CQ 回调、停机 | 无 UAF、重复回收、提前释放 DMA buffer 或停机挂起 |
| 双端 Linux RDMA | 拔链路、恢复链路、接收资源不足 | 后续连接可走 TCP；恢复仅经探测；无重连风暴 |
| 双 HCA Linux RDMA | 主路径故障、不同 PD 的 lkey、注册失败 | 备用 QP 使用匹配 MR；失败可退 TCP；无无效 DMA |

mock 验证不能代替真机验证。硬件验收记录 RDMA provider、固件、拓扑、
RPC 成功率及恢复时延分布；使用受控 RoCE/IB 测试环境完成端口故障注入。
分别测量关闭特性、健康 AUTO、冷却 TCP 三种情况下的吞吐、P99 和 CPU。
合入实现前需给出恢复预算及性能回归容限；本草案不提供未经测量的性能承诺。

## 待评审决策

- 首阶段运行时控制是否采用进程级 API，后续是否需要 Channel 级覆盖？
- 长连接退休的默认排空期限和强制失败入口如何与现有连接池接口对齐？
- 多 HCA 用户内存注册是否提供显式 registration set，还是通过逐设备查询接口？
- 发送停顿阈值及允许的有限 RNR 重试是否需要按部署配置档提供建议？
