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

# RDMA 双机故障恢复测试

两台 Linux 主机运行 RDMA 服务端和 recovery_client，TCP 地址通过独立可达的网络通信，
故障注入作用于 RDMA 测试链路。记录设备、端口、GID、驱动和固件版本。

## 编译

在仓库根目录执行：

```bash
cmake -S . -B build -DWITH_RDMA=ON -DBUILD_SHARED_LIBS=OFF
cmake --build build -j8
cmake --install build --prefix "$PWD/build/install"
cmake -S example/rdma_performance -B build/recovery -DCMAKE_PREFIX_PATH="$PWD/build/install"
cmake --build build/recovery --target server recovery_client -j8
```

Bazel 入口为 `//example:rdma_recovery_client`，Make 入口为示例目录中的 `recovery_client`。

## 运行与切换

主机 B 启动服务端，按环境选择 RDMA 设备及 GID：

```bash
./build/recovery/server --port=8002 --use_rdma=true --rdma_device=mlx5_0 --rdma_gid_index=3
```

主机 A 启动客户端，将 B_TCP_IP 替换为服务端 TCP 地址：

```bash
printf 'auto\n' > /tmp/brpc-rdma-mode
./build/recovery/recovery_client --server=B_TCP_IP:8002 --seconds=180 \
  --mode_file=/tmp/brpc-rdma-mode --connection_type=short \
  --rdma_device=mlx5_0 --rdma_gid_index=3
```

客户端持续发送 RPC，失败后继续下一次调用。每秒输出累计成功数、失败数、RDMA 协商数、
TCP 回落准入数、冷却对端数和端口状态。运行中修改模式文件：

```bash
printf 'tcp_only\n' > /tmp/brpc-rdma-mode
printf 'auto\n' > /tmp/brpc-rdma-mode
```

`short` 为每次调用建立连接，便于验证准入和冷却后的 RDMA 重建。
`single` 用于观察存量连接：已回落的 TCP 连接继续复用，模式切换影响后续新连接。

客户端诊断入口：

```bash
curl 'http://127.0.0.1:8001/vars/rdma_recovery_*'
```

`rdma_recovery_status` 提供模式、epoch、端口状态、冷却剩余时间和分原因故障计数；
`rdma_recovery_tcp_fallback_admissions` 为策略选择 TCP 的累计准入次数。
计数以连接准入为单位，独立于 RPC 成功数。

## 测试步骤

| 场景 | 操作 | 观测 |
| --- | --- | --- |
| 健康链路 | AUTO 运行 | RPC 成功，RDMA 协商数持续增加 |
| 运维降级 | 文件改为 tcp_only | 后续连接成功，TCP 准入数增加，RDMA 协商数停止增加 |
| 恢复模式 | 文件改回 auto | short 模式下 RDMA 协商数恢复增加 |
| 端口故障 | 关闭客户端 RDMA 测试端口，保持 TCP 地址可达 | port_active=false，新连接经 TCP 成功 |
| 端口恢复 | 恢复测试端口，等待 cooldown_ms | 端口变为 active，冷却结束后 RDMA 协商数恢复增加 |
| 在途故障 | single 模式下断开 RDMA 链路 | 记录在途失败和恢复时延，后续连接经 TCP 继续调用 |
| 对端故障 | 中断服务端 RDMA 数据路径，保持 TCP 服务 | retry_exhausted / qp_fatal 等计数增加，对端进入冷却 |

RNR 测试需要接收端故障注入，暂停补充接收 WR 并突破接收 credit 保护后观察 rnr 计数。
普通拥塞可能只表现为背压；源码 mock CQE 测试可确定性验证 RNR 分支。

使用 `--use_rdma=false` 与 TCP 服务端可做无网卡烟测。
客户端退出码 0 表示至少一次 RPC 成功，2 表示整个测试均失败；故障窗口内的失败数单独查看。
验收记录故障前后成功率、恢复时延和各原因计数；性能基线使用原 performance client。

## URMA

链接启用 URMA 的 brpc，服务端配置 `ServerOptions.socket_mode = SOCKET_MODE_URMA`，
客户端增加 `--transport=urma`。`--mode`、`--mode_file`、`--cooldown_ms` 和连接类型沿用上述配置。
CSV 的 `transport_negotiated` 记录选定传输的协商数，诊断入口使用 `/vars/urma_recovery_*`。
断开 URMA 数据路径并保持 TCP 可达，观察重试耗尽或 RNR 计数、对端冷却和后续 TCP 调用；
冷却到期后使用 short 连接验证 URMA 重新准入。
