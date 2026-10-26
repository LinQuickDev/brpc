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

基于 LinQuickDev/brpc master 的 `87d498dd62d8b3b3562407cdf38ac0c475d312b3`。

## 首版实现

`RdmaRecoveryPolicy` 已接入客户端和服务端握手入口，控制新连接的 RDMA 准入：

- LEGACY 为默认模式，沿用现有行为；TCP_ONLY 让新连接使用 TCP。
- AUTO 下，客户端收到 RETRY_EXC / RNR_RETRY_EXC 后，按远端地址冷却 5 秒。
- 当前设备的 PORT_ERR 阻止新 RDMA 建链，端口恢复且冷却结束后重新准入。
- 对端冷却缓存最多 1024 项，满时淘汰最早到期项；模式切换用 epoch 过滤旧连接错误。

通过进程级 API 配置：

```cpp
#include "brpc/rdma/rdma_recovery_policy.h"

brpc::rdma::SetRdmaRecoveryCooldownMs(5000);
brpc::rdma::SetRdmaRecoveryMode(brpc::rdma::RdmaRecoveryMode::AUTO);
// 运维降级
brpc::rdma::SetRdmaRecoveryMode(brpc::rdma::RdmaRecoveryMode::TCP_ONLY);
```

当前模式切换作用于新建连接，存量连接继续使用已协商的传输。
冷却到期后重新开放准入，TCP 连接沿用现有复用与重建机制。
后续迭代增加存量连接退休、单连接探测、指数退避、发送 watchdog 和多端口主备。

以下为完整方案。

## 当前问题

RDMA 握手阶段支持回落 TCP，建链后的错误 CQE 主要通过 `Socket::SetFailed` 处理。
`PORT_ERR` / `PORT_ACTIVE` 事件仅做确认，缺少端口隔离、冷却和恢复探测。
`GlobalDisableRdma` 使用 FATAL，需要独立的运行时降级入口。

相关代码：`src/brpc/rdma/rdma_endpoint.cpp`、`src/brpc/rdma/rdma_helper.cpp`、
`src/brpc/rdma_transport.cpp`。

## 恢复流程

故障检测 → 隔离故障路径 → 新连接选择备用 RDMA 或 TCP → 冷却 → 单连接探测 → 恢复。

新增 `RdmaRecoveryPolicy`，分别管理本地端口和对端路径的健康状态。
本地端口以 device、port 标识，对端路径增加远端地址。
`RdmaTransport` 建链时获取路由快照，`RdmaEndpoint` 持有对应设备资源。

发生故障时，先发布隔离状态，再使受影响连接失败并唤醒写等待者。
后续连接按主 RDMA、备用 RDMA、TCP 的顺序选择可用路径。
TCP 降级使用新连接；存量 RPC 交给现有 Controller 重试策略处理，沿用原配额和 deadline。

## 状态与错误处理

路径状态为 HEALTHY、COOLDOWN、PROBING、DISABLED。

| 事件 | 处理 |
| --- | --- |
| PORT_ERR | 端口进入 COOLDOWN，关闭该端口上的连接 |
| PORT_ACTIVE | 标记端口可探测，冷却结束后进入 PROBING |
| QP/CQ 错误、RETRY_EXC | 连接失败并隔离对应对端路径 |
| RNR_RETRY_EXC | 隔离对端路径，记录接收资源或流控异常 |
| SQ/RQ 窗口耗尽 | 返回 EAGAIN，等待 completion 或 credit |
| post_send 失败 | 保存 errno，使连接失败并按错误类型更新路径状态 |
| DEVICE_FATAL | 禁用对应设备资源域，沿用现有 FATAL 处理 |

冷却使用单调时钟，连续失败采用指数退避和 jitter。
到期后最多准入一个 RDMA 探测连接，其余新连接继续走备用路径或 TCP。
探测使用正常业务流量；收到有效 RDMA 数据且发送获得完成后恢复 HEALTHY，
失败或探测超时则重新冷却。

每次状态更新携带 generation 和 policy epoch。
同一连接代际只报告一次故障，晚到事件按代际过滤。

## 运行时降级

提供进程级 `RdmaRecoveryMode { LEGACY, AUTO, TCP_ONLY }` 和 setter/getter。
LEGACY 为默认值；AUTO 启用恢复策略；TCP_ONLY 将后续请求切到 TCP。

模式更新递增 policy epoch。TCP_ONLY 下，健康存量 RPC 完成后连接退休；
请求获取 socket 时检查退休状态，新请求使用新连接。
切回 AUTO 后进入 PROBING，逐步恢复 RDMA。

SocketMap 和连接池按传输模式、设备资源域和 epoch 区分连接。
socket 复活时重新获取路由快照。服务实例的健康检查与 RDMA 路径状态分开维护。

## 多端口主备

启动时配置主备路径，切换时创建新 QP 并交换对应 GID/QPN。
同 HCA 多端口共享 PD 时复用注册内存，各端口独立维护路由和健康状态。

跨 HCA 引入 `RdmaDeviceContext`，分别持有 context、PD、事件源和 MR 注册表。
IOBuf 和用户内存按目标 PD 查找 lkey，并管理各设备上的注册与引用生命周期。
备用设备注册失败时选择 TCP。

回收顺序为：停止准入和 WR 提交，停止 CQ 处理，reset/drain QP/CQ，
再释放 buffer、MR、PD 和 context。停机先取消探测与 watchdog，再等待回调退出。

## SQ 停顿与 RNR

正常窗口耗尽保持 EAGAIN 背压。发送 watchdog 跟踪 outstanding WR、
待发送数据和 credit 的进展；超过阈值后使连接失败并隔离对端路径。
计时从发送受阻开始，发送完成或 credit 前进时更新。

ACK timeout、retry count 和 RNR retry 保持当前默认值，支持配置有限重试次数。
RNR 重试耗尽作为对端路径故障处理，发送停顿检测独立于 RPC deadline。

## 配置与指标

| 参数 | 建议默认值 | 用途 |
| --- | --- | --- |
| rdma_recovery_mode | legacy | 选择恢复模式 |
| rdma_path_cooldown_ms | 5000 | 初始冷却时间 |
| rdma_path_max_cooldown_ms | 60000 | 退避上限 |
| rdma_recovery_probe_timeout_ms | 1000 | 探测预算，受调用 deadline 约束 |
| rdma_send_stall_timeout_ms | 0 | 发送停顿阈值，0 关闭 |
| rdma_backup_paths | 空 | 启动时配置主备路径 |

指标覆盖端口状态、隔离次数、TCP 降级次数、探测结果、旧 epoch 活动连接数和发送停顿时长。
日志记录 device、port、socket generation、WC status/vendor_err 和 errno。

## 实现顺序与验证

1. 实现单设备故障分类、端口隔离、冷却探测、TCP 降级和运行时模式切换。
2. 增加发送停顿检测、有限 RNR 重试配置和诊断指标。
3. 拆分设备资源域，支持多端口及跨 HCA 主备。

单测覆盖冷却与并发探测、错误分类、窗口恢复、模式切换、连接复用和代际回收。
双端 Linux RDMA 测试覆盖断链恢复、RNR 和发送停顿；双 HCA 测试验证主备切换与 MR 注册。
对比 LEGACY、健康 AUTO 和降级 TCP 的恢复时延、吞吐、P99 和 CPU。
