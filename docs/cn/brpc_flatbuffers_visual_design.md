# bRPC FlatBuffers 免反序列化方案（图示版）

> 用途：方案串讲与技术评审。  
> 当前范围：先验证 FlatBuffers over TCP；后续再提取通用 Region，并接入 RDMA/URMA。

## 1. 一句话目标

> 将网络收到的编码字节保存在生命周期受控的数据区域中，经过类型安全校验后，为业务提供可直接读取的类型化视图，减少完整反序列化、对象分配和不必要的内存复制。

第一阶段承诺的是“免反序列化 + 条件式零拷贝快路径”，不是无条件的全链路零拷贝。

## 2. 传统路径与目标路径

```mermaid
flowchart LR
    subgraph OLD[传统 Protobuf 路径]
        A1[C++ 业务对象] --> A2[序列化]
        A2 --> A3[编码缓冲区]
        A3 --> A4[bRPC / TCP]
        A4 --> A5[接收缓冲区]
        A5 --> A6[反序列化]
        A6 --> A7[新消息对象]
        A7 --> A8[业务访问]
    end

    subgraph NEW[目标 FlatBuffers 路径]
        B1[FlatBuffers Builder] --> B2[编码缓冲区]
        B2 --> B3[bRPC / TCP]
        B3 --> B4[SerializedRegion]
        B4 --> B5[Verifier]
        B5 --> B6[VerifiedView]
        B6 --> B7[业务按需访问]
    end
```

主要变化发生在接收端：

```text
传统：字节 → 解析全部字段 → 构造新对象 → 业务访问
目标：字节 → 安全校验 → 建立只读视图 → 业务按需访问
```

## 3. 整体分层设计

```mermaid
flowchart TB
    APP[业务层<br/>读取字段，不关心底层传输]
    VIEW[VerifiedView&lt;T&gt;<br/>类型化只读访问 + 已验证状态]
    REGION[SerializedRegion<br/>地址 + 长度 + 所有权 + 生命周期 + 连续性]
    ADAPTER[Transport Adapter<br/>将不同数据来源转换成 Region]

    TCP[TCP / bRPC IOBuf]
    SHM[共享内存]
    RDMA[RDMA 注册内存]
    URMA[URMA 内存区域]

    APP --> VIEW
    VIEW --> REGION
    REGION --> ADAPTER
    ADAPTER --> TCP
    ADAPTER --> SHM
    ADAPTER --> RDMA
    ADAPTER --> URMA
```

| 层次 | 主要职责 |
|---|---|
| 业务层 | 使用类型化接口读取字段 |
| `VerifiedView<T>` | 格式解释、安全状态和访问入口 |
| `SerializedRegion` | 数据地址、长度、所有权、生命周期和内存属性 |
| Transport Adapter | 接入 IOBuf、共享内存、RDMA、URMA |

设计原则是格式与传输解耦：FlatBuffers 不直接依赖 RDMA，RDMA/URMA Region 也不只服务于 FlatBuffers。

## 4. 普通 C++ 对象与 FlatBuffers 布局

### 4.1 普通 C++ 对象

```mermaid
flowchart LR
    OBJ[User 对象<br/>id<br/>name 指针<br/>scores 指针]
    NAME[另一块内存<br/>Alice]
    SCORES[另一块内存<br/>1.0 2.0 3.0]

    OBJ -->|name 指针| NAME
    OBJ -->|scores 指针| SCORES
```

普通 `std::string`、`std::vector` 通常包含进程内指针。发送端的指针值在接收端没有意义，因此不能直接传输普通 C++ 对象内存。

### 4.2 FlatBuffers 编码布局

```mermaid
flowchart LR
    ROOT[Root Offset]
    TABLE[User Table<br/>id + 相对偏移]
    VTABLE[VTable<br/>字段位置]
    STR[String<br/>长度 + 字符]
    VECTOR[Vector<br/>长度 + 连续元素]

    ROOT -->|相对偏移| TABLE
    TABLE -->|字段布局| VTABLE
    TABLE -->|相对偏移| STR
    TABLE -->|相对偏移| VECTOR
```

FlatBuffers 使用相对偏移而不是进程指针。因此整块 buffer 移动到另一地址或另一机器后，内部关系仍然可以解释。

## 5. 类型化视图如何构造

```mermaid
flowchart TD
    A[收到 IOBuf / Region]
    B[获得连续地址、实际长度和所有权]
    C[根据 RPC Method 确定预期 Root 类型]
    D[构造 flatbuffers::Verifier]
    E{类型专用 Verify 函数通过?}
    F[拒绝请求<br/>设置 RPC 错误<br/>不调用业务]
    G[GetRoot&lt;T&gt; 计算根对象地址]
    H[将 root 与 Region 所有权绑定]
    I[生成 VerifiedView&lt;T&gt;]
    J[业务通过生成访问器读取字段]

    A --> B --> C --> D --> E
    E -- 否 --> F
    E -- 是 --> G --> H --> I --> J
```

概念代码：

```cpp
auto region = SerializedRegion::FromIOBuf(input);

flatbuffers::Verifier verifier(
    region.data(), region.size());

if (!VerifyRequestBuffer(verifier)) {
    return InvalidRequest();
}

const Request* root =
    flatbuffers::GetRoot<Request>(region.data());

VerifiedView<Request> view(
    std::move(region), root);
```

这里没有构造一套新的 `Request` 对象树。`root` 只是原始 FlatBuffers 数据的类型化入口。

## 6. 安全校验过程

```mermaid
flowchart TD
    NET[不可信网络字节]
    BOUNDARY{RPC 报文是否完整?}
    SIZE{消息大小是否在限制内?}
    CONTIG[得到生命周期受控的连续 Region]
    VERIFY[Verifier 检查<br/>Root / VTable / Offset / String / Vector / 嵌套]
    VALID{校验是否成功?}
    VIEW[创建 VerifiedView]
    SERVICE[调用业务 Service Method]
    ERROR[返回协议或数据错误<br/>释放资源并记录指标]

    NET --> BOUNDARY
    BOUNDARY -- 否 --> ERROR
    BOUNDARY -- 是 --> SIZE
    SIZE -- 否 --> ERROR
    SIZE -- 是 --> CONTIG --> VERIFY --> VALID
    VALID -- 否 --> ERROR
    VALID -- 是 --> VIEW --> SERVICE
```

必须校验的原因：FlatBuffers 内部保存大量相对偏移和长度。恶意或损坏数据可能伪造 Root Offset、Vector 长度或 String 边界。直接 `GetRoot<T>()` 并不能证明这些值合法。

安全策略：

- 先检查 RPC 消息边界和最大长度；
- 再获得有效且不会提前释放的 Region；
- 使用当前 Method 对应的类型专用 Verifier；
- 校验失败时不得调用业务方法；
- 校验成功后才允许构造 View；
- 同时限制嵌套深度、对象数量和并发资源占用。

Checksum 不能代替 Verifier：Checksum 检查内容是否符合测试预期，Verifier 检查这些字节是否能被安全解释。

## 7. IOBuf 的连续与分片路径

```mermaid
flowchart TD
    INPUT[收到一条逻辑完整的 IOBuf 消息]
    CHECK{完整消息是否位于一个连续 Block?}
    REF[引用原 Block<br/>不合并数据]
    COPY[申请连续内存<br/>复制并合并多个 Block]
    REGION[连续 SerializedRegion]
    VERIFY[Verifier]
    VIEW[VerifiedView]

    INPUT --> CHECK
    CHECK -- 是：快路径 --> REF --> REGION
    CHECK -- 否：降级路径 --> COPY --> REGION
    REGION --> VERIFY --> VIEW
```

### 快路径

```text
单个连续 IOBuf Block → Region 引用原内存 → Verifier → View
```

可以做到接收端不合并复制，同时免去完整反序列化。

### 降级路径

```text
多个不连续 Block → 合并复制 → 连续 Region → Verifier → View
```

发生了一次复制，但仍不需要构造完整对象树。业务接口无需因底层布局不同而改变。

## 8. View 与 Region 的生命周期

```mermaid
flowchart LR
    VIEW[VerifiedView&lt;T&gt;]
    ROOT[const T* root]
    REGION[SerializedRegion]
    OWNER[IOBuf Block / Shared Memory / Registered Memory]

    VIEW --> ROOT
    VIEW -->|持有所有权| REGION
    REGION -->|保持存活| OWNER
    ROOT -.指向.-> OWNER
```

必须保证：

```text
View 的有效期 ≤ Region 的有效期
```

如果只保存 `const T*`，底层 IOBuf 被释放后该指针会悬空。跨 RPC 回调持有数据时，业务必须显式取得 Region 所有权或复制需要长期保存的数据。

## 9. FlatBuffers 接入 bRPC 的两种路线

```mermaid
flowchart TB
    START[FlatBuffers 接入 bRPC]

    START --> A[路线 A：独立 FlatBuffers RPC Protocol]
    START --> B[路线 B：Protobuf 控制消息 + FlatBuffers Attachment]

    A --> A1[类型化 Stub / Service]
    A --> A2[统一校验和生命周期]
    A --> A3[核心代码改动较大]

    B --> B1[复用现有 RPC 和 Attachment]
    B --> B2[核心改动较小]
    B --> B3[类型和校验更多由业务管理]
```

| 路线 | 优点 | 主要代价 |
|---|---|---|
| 独立 Protocol | 类型化接口完整，可统一生成 Stub/Service | 修改 Channel、Controller、Server，维护成本高 |
| Attachment View | MVP 快、对核心侵入较小 | 类型和校验不够自动化，用户接口不够自然 |

这是本次评审需要重点决定的问题。

## 10. 历史 PR 与当前工作的关系

```mermaid
flowchart TD
    P3196[PR #3196<br/>MessageBuilder + Message + IOBuf]
    P3197[PR #3197<br/>Protocol + Channel + Controller + Server]
    PROTO[当前可编译原型<br/>证明接入路径可行]
    FORMAL[正式 V1<br/>类型安全 + Verifier + 生命周期 + 测试]
    REGION[通用 SerializedRegion]
    REMOTE[RDMA / URMA]

    P3196 --> PROTO
    P3197 --> PROTO
    PROTO --> FORMAL --> REGION --> REMOTE
```

- PR #3196 回答消息怎样构造、持有和访问；
- PR #3197 回答消息怎样进入完整 RPC 请求/响应链路；
- 当前原型用于证明“能否接入”；
- 正式实现还要解决类型安全、校验、生命周期、兼容性和测试。

## 11. 分阶段实施路线

```mermaid
timeline
    title FlatBuffers 免反序列化能力演进
    V1 FlatBuffers over TCP
        : Builder / Message / View
        : 类型安全 RPC 接口
        : 强制 Verifier
        : 单 Block 快路径与多 Block 降级
        : 正确性、安全性和性能测试
    V2 通用 SerializedRegion
        : 格式与传输解耦
        : 连续和分片区域
        : 所有权与生命周期
        : 复制次数和字节数观测
    V3 RDMA / URMA
        : 注册内存与 Region Handle
        : 访问权限和 Lease
        : 按需读取
        : 超时、断连与撤销
        : 原生设备测试
```

V1 可以在本地 WSL 完成；V3 才需要具有 RDMA/URMA 设备的原生 Linux 环境。

## 12. 当前状态

```mermaid
flowchart LR
    DONE1[已完成<br/>三种格式 benchmark]
    DONE2[已完成<br/>历史 PR 调研]
    DONE3[已完成<br/>Builder 原型移植]
    DONE4[已完成<br/>bRPC 静态库与示例构建]
    TODO1[待完成<br/>RPC 运行和字段一致性]
    TODO2[待完成<br/>Verifier 与生命周期]
    TODO3[待完成<br/>类型安全接口]
    TODO4[待完成<br/>正式测试与性能证据]

    DONE1 --> DONE2 --> DONE3 --> DONE4 --> TODO1 --> TODO2 --> TODO3 --> TODO4
```

准确状态是：

> FlatBuffers builder/protocol 的可编译原型已经打通，但尚未形成可向社区提交的正式功能实现。

## 13. 希望会议确认的事项

1. 是否认可“FlatBuffers over TCP → 通用 Region → RDMA/URMA”的路线？
2. V1 是否只承诺免反序列化和条件式零拷贝快路径？
3. FlatBuffers 应采用独立 Protocol 还是 Attachment View？
4. 是否接受 FlatBuffers 为可选依赖？
5. 接收端是否必须执行类型专用 Verifier？
6. V1 是否允许多分片输入合并复制？
7. View 能否跨 RPC 回调持有，采用何种所有权接口？
8. 历史 PR 中哪些机制保留，哪些接口重写？
9. 哪些正确性、安全性和性能指标作为合入门槛？

## 14. 建议结论

```text
先保证正确和安全
        ↓
再验证免反序列化收益
        ↓
再优化连续缓冲区零拷贝快路径
        ↓
最后推广至 RDMA/URMA
```

建议暂不把当前历史 PR 移植代码作为正式功能提交。先完成类型安全接口、强制校验、生命周期规则和 TCP 测试，再根据可复现证据提取通用 Region。

## 15. 相关链接

- 内部方案 PR：<https://github.com/LinQuickDev/brpc/pull/44>
- Apache bRPC：<https://github.com/apache/brpc>
- 历史 Builder PR：<https://github.com/apache/brpc/pull/3196>
- 历史 Protocol PR：<https://github.com/apache/brpc/pull/3197>
- FlatBuffers：<https://github.com/google/flatbuffers>

