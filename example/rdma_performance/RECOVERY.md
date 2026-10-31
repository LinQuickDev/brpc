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

两台 Linux 主机运行服务端和 recovery_client，TCP 控制地址保持可达。
记录设备、端口、GID、驱动和固件版本。

## 编译

在仓库根目录执行：

```bash
cmake -S . -B build -DWITH_RDMA=ON -DBUILD_SHARED_LIBS=OFF
cmake --build build -j8
cmake --install build --prefix "$PWD/build/install"
cmake -S example/rdma_performance -B build/recovery -DCMAKE_PREFIX_PATH="$PWD/build/install"
cmake --build build/recovery --target server recovery_client -j8
```

## 运行与切换

服务端：

```bash
./build/recovery/server --port=8002 --use_rdma=true --rdma_device=mlx5_0 --rdma_gid_index=3
```

客户端，将 B_TCP_IP 替换为服务端地址：

```bash
printf 'auto\n' > /tmp/brpc-rdma-mode
./build/recovery/recovery_client --server=B_TCP_IP:8002 --seconds=180 \
  --mode_file=/tmp/brpc-rdma-mode --connection_type=short \
  --rdma_device=mlx5_0 --rdma_gid_index=3
```

在客户端另一终端切换模式并查看指标：

```bash
printf 'tcp_only\n' > /tmp/brpc-rdma-mode
curl --noproxy '*' 'http://127.0.0.1:8001/vars/rdma_recovery_*'
printf 'auto\n' > /tmp/brpc-rdma-mode
```

`short` 每次调用建立连接，`single` 复用已协商的传输。
CSV 每秒输出累计成功、失败、传输协商、TCP 准入及冷却状态。
健康阶段检查 `failure=0`；注入阶段记录新增失败和恢复时延。
`rdma_recovery_status` 提供分原因故障计数，TCP 准入指标按连接累计。

## 验收

| 场景 | 操作 | 预期 |
| --- | --- | --- |
| 正常通信 | AUTO 运行 | RPC 成功，RDMA 协商数增加 |
| 运行中降级 | 切换 TCP_ONLY | TCP 准入增加，RDMA 协商暂停 |
| 恢复 RDMA | 切回 AUTO | short 连接的 RDMA 协商恢复 |
| 端口故障 | 关闭 RDMA 测试端口 | port_active=false，后续连接使用 TCP |
| 端口恢复 | 恢复端口并等待冷却 | RDMA 准入恢复 |
| 对端故障 | 中断服务端 RDMA 数据路径 | 故障计数增加，对端冷却，后续连接使用 TCP |
| RNR | 暂停接收 WR 补充并突破 credit 保护 | rnr 计数增加，对端冷却 |

使用 `single` 观察在途失败，使用 `short` 检查新连接的回落和恢复。

## URMA

[URMA 测试入口](../../docs/cn/urma.md#运行期故障恢复)提供恢复客户端及 SEND 失败注入运行器，
支持 v2/v3 握手、模式切换、对端冷却、TCP 准入和 URMA 恢复。
