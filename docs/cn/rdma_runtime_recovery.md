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

# RDMA 与 URMA 运行期故障恢复

运行期故障后，恢复策略控制后续连接的传输选择：

故障上报 → 对端或端口冷却 → 新连接使用 TCP → 冷却结束后恢复准入。

## 模式

| 模式 | 新连接行为 |
| --- | --- |
| LEGACY（默认） | 沿用原有握手和错误处理 |
| AUTO | 按端口和对端状态准入，冷却期间选择 TCP |
| TCP_ONLY | 直接选择 TCP |

模式切换作用于新连接，存量连接继续使用已协商的传输。
当前 RPC 沿用 Controller 的重试次数和 deadline。

## 冷却与错误处理

默认冷却 5 秒，使用单调时钟。客户端按服务端 IP 和端口记录对端状态，
缓存上限为 1024 项，满时淘汰最早到期项。RDMA 和 URMA 独立维护状态与指标，
模式切换递增 epoch，过滤旧连接的故障上报。

| 事件 | 处理 |
| --- | --- |
| RDMA PORT_ERR | 暂停当前端口的新 RDMA 连接，记录端口冷却 |
| RDMA PORT_ACTIVE | 更新端口状态，冷却结束后恢复准入 |
| 重试耗尽、RNR、QP/CQ/JFC 错误、WR 提交失败 | 上报故障，AUTO 下冷却客户端对端 |
| 访问错误 | 单独计数 |
| WR flush | 沿用已记录的根因 |
| SQ 或接收 credit 耗尽 | 返回 EAGAIN，等待 completion 或 credit |

同一连接只记录首个运行期故障。QP/CQ 异步事件核对资源归属，故障资源释放。
URMA 握手支持 v2/v3；短连接关闭后回收队列和接收缓冲，TCP 解析与关闭检查串行执行。

## 配置

```cpp
#include "brpc/rdma/rdma_recovery_policy.h"
#include "brpc/urma/urma_recovery_policy.h"

brpc::rdma::SetRdmaRecoveryCooldownMs(5000);
brpc::rdma::SetRdmaRecoveryMode(brpc::rdma::RdmaRecoveryMode::AUTO);

brpc::urma::SetUrmaRecoveryCooldownMs(5000);
brpc::urma::SetUrmaRecoveryMode(brpc::urma::UrmaRecoveryMode::AUTO);
```

运行时调用对应 setter 切换 `LEGACY`、`AUTO` 或 `TCP_ONLY`。
URMA 使用 CTP/RTP 路径，设备选择见 [URMA 配置](urma.md#传输路径选择)。

## 指标与验证

`GetRdmaRecoveryStatus()`、`GetUrmaRecoveryStatus()` 及 `/vars/rdma_recovery_*`、
`/vars/urma_recovery_*` 提供模式、epoch、分原因故障计数、冷却状态、
传输协商数和 TCP 准入数。TCP 准入按连接累计。

- [RDMA 双机测试](../../example/rdma_performance/RECOVERY.md)：模式切换、端口和对端故障。
- [URMA 定向注入](urma.md#定向发送失败测试)：一次 SEND 提交失败、TCP 冷却和 URMA 恢复。
