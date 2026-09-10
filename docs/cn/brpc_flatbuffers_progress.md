# bRPC FlatBuffers 集成进展说明

更新日期：2026-09-10

## 1. 当前结论

已在个人 fork 中完成 FlatBuffers 消息构造及 RPC 原型移植、可选构建支持、消息与 TCP RPC 自动化测试。当前两个测试程序包含 7 个测试用例，均已通过本地验证。

当前阶段是具备基本正确性验证的集成原型。完整的安全视图设计、拷贝路径分析、性能收益证明和社区合入仍需继续推进。

本说明依据本地开发过程中记录的 Git 提交、构建输出和测试日志编写，不代表 Apache 社区已接受或合入这些改动。

## 2. 代码位置与提交

- 仓库：https://github.com/Spicy-cream/brpc
- 当前开发分支：`feature/flatbuffers-rpc-tests`
- 分支地址：https://github.com/Spicy-cream/brpc/tree/feature/flatbuffers-rpc-tests
- 本文记录的代码版本：`0f591739`

| 提交 | 内容 |
| --- | --- |
| `d14d1002` | 移植社区 FlatBuffers 消息构造实现 |
| `f7255f84` | 移植 FlatBuffers RPC 协议，并完成相关兼容修复与示例调整 |
| `4cad8ef6` | 增加 FlatBuffers RPC 可选构建支持 |
| `dd2c20ae` | 增加消息所有权与校验测试 |
| `0f591739` | 增加 RPC 收发、请求拒绝与拒绝后恢复测试 |

以上分支和提交已推送到个人 fork；本文编写时尚未据此创建新的 Apache PR。

## 3. 与社区原有工作的关系

集成基于 Apache bRPC 的已有贡献：

- [PR #3196：FlatBuffers 消息构造](https://github.com/apache/brpc/pull/3196)
- [PR #3197：FlatBuffers RPC 协议](https://github.com/apache/brpc/pull/3197)

消息构造与协议的基础设计来源于原有社区工作。本阶段主要完成移植、问题修复、构建开关以及回归验证。后续贡献应保留原作者署名，并与原作者及维护者协调提交范围。

当前分支还包含前期设计和 benchmark 文档。面向 Apache 提交时，需要按目标分支重新整理差异，不宜直接提交整个开发分支。

## 4. 已完成的实现

### 4.1 消息构造与缓冲区所有权

- 接入 `MessageBuilder`、`Message` 及相关缓冲区适配实现。
- 修复 `Message` 的移动构造与移动赋值，通过 `SingleIOBuf::swap` 转移缓冲区及元数据。
- 验证 Builder 销毁后，已释放的 Message 仍能被校验和读取。
- 验证移动操作保留数据地址和长度，移动后的源对象不会使目标消息失效。

这些测试证明所覆盖的消息生命周期和所有权行为，不等价于证明整个 RPC 链路零拷贝。

### 4.2 RPC 接入

- 接入 Channel、Server、协议注册及请求响应处理。
- 提供 `fb_rpc` 调用路径和 `example/benchmark_fb` 示例。
- 完成本地 TCP 请求响应验证。
- 示例服务在访问请求字段前调用 `Verify`；示例客户端能够校验响应并比较字段。

当前协议响应携带错误码，不传递服务端详细错误文本。客户端收到非零错误码时使用通用文本 `server response error`。自动化测试据此检查错误码，而不要求服务端错误字符串原样返回。

### 4.3 可选构建

- 增加 `WITH_FLATBUFFERS` CMake 选项，默认关闭。
- OFF 时排除相应实现及受保护的接口依赖。
- ON 时启用 FlatBuffers 实现并发现依赖头文件。
- 导出配置头中记录 `BRPC_WITH_FLATBUFFERS` 的 0/1 值。
- OFF 时排除 `brpc_flatbuffers_*_unittest.cpp` 测试目标。

已验证 ON/OFF 核心库构建通过，ON 导出头文件和静态库可以用于构建外部示例。自动化测试 OFF 配置验证了 FlatBuffers 测试未注册，此项不是一次完整的 OFF 测试套件运行。

## 5. 自动化测试覆盖

### 5.1 消息测试

文件：`test/brpc_flatbuffers_message_unittest.cpp`

| 测试 | 验证内容 |
| --- | --- |
| `ReleasedMessageSurvivesBuilderDestruction` | Builder 销毁后消息仍有效，校验及各字段读取正确 |
| `MoveConstructorPreservesBuffer` | 移动构造保留缓冲区地址和长度，源对象清理后目标仍有效 |
| `MoveAssignmentReplacesExistingMessage` | 正确替换已有消息，源对象销毁后目标仍有效 |
| `RejectsCorruptRootOffset` | 根偏移被破坏后，Verifier 返回失败 |

### 5.2 TCP RPC 测试

文件：`test/brpc_flatbuffers_rpc_unittest.cpp`

测试使用 `127.0.0.1:0` 自动分配端口，启动真实 Server 与 Channel，禁用重试，并设置 RPC 超时。

| 测试 | 验证内容 |
| --- | --- |
| `ValidRequestRoundTrip` | 完成请求响应，校验响应结构并比较全部业务字段 |
| `CorruptRequestIsRejectedByService` | 服务校验损坏请求后返回 `EREQUEST`，并确认服务端拒绝计数 |
| `ValidRequestSucceedsAfterRejection` | 同一 Server、Channel 先成功、再拒绝损坏请求、随后再次成功 |

负向测试结合错误码与服务端接受/拒绝计数，避免仅凭 `Failed()` 就把连接失败或超时误判为成功拒绝。

这里测试的是服务实现主动调用 `Verify` 的行为，尚未实现协议层对任意业务类型的统一校验。

### 5.3 已记录的执行结果

```text
configure_status=0
build_status=0

brpc_flatbuffers_message_unittest ... Passed
brpc_flatbuffers_rpc_unittest ....... Passed

100% tests passed, 0 tests failed out of 2
```

CTest 的两个条目分别对应两个测试程序，合计 4 + 3 = 7 个测试用例。OFF 配置下查询 FlatBuffers 测试得到 `Total Tests: 0`。

验证环境为 Ubuntu 24.04 WSL2、GCC 13.3、Protobuf 3.21.12、FlatBuffers 25.12.19，以及本地 GoogleTest 源码。以上结果来自本地运行，尚不等同于跨平台 CI 验证。

## 6. 本地复现

以下命令从仓库根目录执行。FlatBuffers 安装目录和 GoogleTest 源码目录需按实际环境调整。

```bash
cmake -S . -B build-flatbuffers-tests-on -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DWITH_FLATBUFFERS=ON \
  -DBUILD_UNIT_TESTS=ON \
  -DDOWNLOAD_GTEST=OFF \
  -DBRPC_SYSTEM_GTEST_SOURCE_DIR=/usr/src/googletest \
  -DCMAKE_PREFIX_PATH=/home/l30084420/workspace/serialization-benchmark/local

cmake --build build-flatbuffers-tests-on \
  --target brpc_flatbuffers_message_unittest brpc_flatbuffers_rpc_unittest \
  --parallel 8

ctest --test-dir build-flatbuffers-tests-on \
  -R '^brpc_flatbuffers_(message|rpc)_unittest$' \
  --timeout 60 --output-on-failure
```

OFF 配置及测试注册检查：

```bash
cmake -S . -B build-flatbuffers-tests-off -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug \
  -DWITH_FLATBUFFERS=OFF \
  -DBUILD_UNIT_TESTS=ON \
  -DDOWNLOAD_GTEST=OFF \
  -DBRPC_SYSTEM_GTEST_SOURCE_DIR=/usr/src/googletest

ctest --test-dir build-flatbuffers-tests-off -N \
  -R '^brpc_flatbuffers_.*'
```

当前 CMake 配置会生成源码目录中的 `src/butil/config.h`。同一 checkout 切换 ON/OFF 配置后，重新构建前应重新配置目标模式；不要在同一源码目录同时进行两种配置的构建。

当前测试复用了 `example/benchmark_fb` 中的 schema 生成代码。生成头文件带有 FlatBuffers 版本检查，后续需要整理测试 schema 和生成流程，避免将社区测试长期绑定到本机版本。

## 7. 性能数据的适用范围

此前在本地 TCP 短时冒烟运行中观察到约 18k–19k QPS、约 50–52 微秒 RPC 延迟。这些结果用于说明链路可运行，不作为 FlatBuffers 相对 Protobuf 的性能收益结论。

尚需统一编译模式、负载、并发、校验策略和测量区间，进行可重复的对照测试。消息构造耗时、校验/读取耗时和完整 RPC 延迟应分别统计。

## 8. 后续工作

1. 与原 PR 作者及 Apache 维护者沟通，整理基于 Apache 的提交范围和贡献归属。
2. 整理测试 schema、版本依赖和生成流程，并接入适当的 CI 构建与回归验证。
3. 根据协议审查补充边界、异常报文、附件、并发及生命周期测试，开展内存检查工具验证。
4. 继续设计校验后的类型化视图，明确缓冲区所有权、可变性及访问边界。
5. 分析发送与接收路径中的实际拷贝，进行有对照的性能测试。

完整安全视图、端到端零拷贝、RDMA/URMA 路径和正式社区合入均不属于当前已完成的验证范围。
