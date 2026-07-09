# PMS Client 设计文档

## 1. 模块定位
Java SDK，供外部程序（如 Flink Connector）调用，屏蔽底层 RPC、batching 与序列化细节。

当前实现分为两层：

- raw client：依赖 `pms-protocol`，只处理 HTTP/2 binary transport、handshake、raw `byte[] keyBytes` / `byte[] rowBytes`、batching、结果分类和关闭 drain。
- row-aware facade：在 raw client 之上依赖 `pms-codec`，负责 Paimon row/key 编码、RowKind 归一化和 schema 相关能力。

早期零依赖客户端包不再作为强约束。若后续需要面向用户作业提供更轻的 artifact，可在 raw client 与 row-aware facade 稳定后再评估 shading 或拆包。

## 2. 核心组件

### 2.1 PMSClient

与 PMS Server 通信的主入口。

**初始化流程**：

```
1. 创建 JDK HttpClient，并对 Server 调用 handshake
2. 校验协议版本、HTTP/2、capability 与请求/响应大小限制
3. 解析 Server 返回的 RowType JSON、primaryKeys 和 SchemaId
4. 校验 schema hash 与 codec version
5. 初始化 RowValueCodec/PrimaryKeyCodec，并校验 primary key 类型是否受 PMS 支持
```

**接口**：

```java
class PmsClient implements AutoCloseable {
    // 初始化
    static PmsClient connect(PmsClientConfig config);

    // 写入
    WriteResult write(InternalRow row);
    WriteResult writeBatch(List<? extends InternalRow> rows);
    WriteResult delete(InternalRow keyTuple);

    // 查询
    PmsRowLookupResult get(InternalRow keyTuple);
    PmsRowLookupResult getLocal(InternalRow keyTuple);
    PmsRowLookupBatchResult prefixLocal(InternalRow keyPrefixTuple);

    // 关闭
    void close();
}
```

`get` 表示完整表点查，允许 PMS Server 在本地 miss 后穿透查询 Paimon。`getLocal`
只查询 PMS 本地层，返回值必须区分 `HIT`、`DELETED` 和 `MISS`，避免调用方在本地 tombstone
场景下继续查 Paimon 导致旧值复活。V1 的 prefix 查询只提供 local 语义；完整表 prefix 查询接口
预留但暂不支持。

raw client 的协议 envelope、status、batch 整批语义与 `LOOKUP_UNAVAILABLE` 可重试查询错误见
[pms-protocol.md](pms-protocol.md)。

V1 不处理 Paimon 表 Schema 变更。`PmsClient.connect(config)` 只在连接建立时从 handshake
获取一次 schema snapshot，并认为该 snapshot 在 client 生命周期内稳定。若后续发现 schema
变更，server 应进入 fatal 路径或通过后续版本的 schema reload 机制处理；当前版本不在 client
内部静默切换 codec。

### 2.2 Java SDK 使用示例

row-aware facade 的推荐入口是 `PmsClient.connect(config)`。调用方不需要手动传入
`RowType` 或 primary keys；client 会从 server handshake 获取固定表 schema snapshot，并在
本地初始化 row/key codec。

下面示例假设 PMS 绑定的 Paimon 表字段顺序为 `id INT, marker STRING`，主键为 `id`：

```java
URI serverUri = URI.create("http://127.0.0.1:9090");
PmsClientConfig config = PmsClientConfig.builder(serverUri).build();

try (PmsClient client = PmsClient.connect(config)) {
    GenericRow row = new GenericRow(RowKind.INSERT, client.rowType().getFieldCount());
    row.setField(0, 1001);
    row.setField(1, BinaryString.fromString("active"));

    WriteResult write = client.write(row);
    if (write.status() != PmsStatus.OK) {
        throw new IllegalStateException("PMS write failed: " + write.status());
    }

    GenericRow keyTuple = GenericRow.of(1001);
    PmsRowLookupResult lookup = client.get(keyTuple);
    if (lookup.found()) {
        InternalRow latest = lookup.row();
        // 使用 latest 继续业务处理。
    }

    client.delete(keyTuple);
}
```

构造 `InternalRow` 时需要遵循以下约定：

- 完整写入 row 的字段数量和字段顺序必须与 `client.rowType()` 一致。
- `delete/get/getLocal` 的 key tuple 只包含 primary key 字段，字段顺序为 `client.primaryKeyFieldNames()`。
- `prefixLocal` 的 key prefix tuple 只适用于复合主键的连续前缀，例如主键为 `[tenant_id, id]` 时可传 `GenericRow.of(tenantId)`。
- `INSERT` 和 `UPDATE_AFTER` 会被归一化为 PMS KV put；`DELETE` 和 `UPDATE_BEFORE` 会被归一化为 PMS KV delete。
- 当前边界是 Paimon `InternalRow`，调用方需要使用 Paimon internal value 类型，例如 `STRING` 字段使用 `BinaryString.fromString(...)`，`DECIMAL` 使用 `Decimal`，时间字段使用 Paimon `Timestamp`。
- `PmsClient` 不提供绕过 handshake 的公开 schema 重载，避免调用方使用与 server 不一致的
  `RowType`、primary keys 或 writer schema id 写入 opaque bytes。

对应的可编译最小样例见 `pms-client/src/test/java/org/qwh/pms/client/examples/PmsClientUsageExample.java`。

**WriteStatus**：

| 状态 | 含义 | Client 行为 |
|------|------|------------|
| `OK` | 写入成功 | 继续 |
| `OVERLOADED` | 系统过载，被拒绝 | 重试（指数退避） |
| `SCHEMA_MISMATCH` | Schema 不一致 | V1 停止写入，不自动 reload |
| `SHUTTING_DOWN` | 服务停机 | 当前 endpoint 等待恢复或由上层切换 |
| `INTERNAL_ERROR` | 普通服务端内部错误 | 不自动重试，由调用方处理 |

若 WAL 已成功但 MemTable apply 失败，server 不返回普通 `INTERNAL_ERROR`：连接会失败，
server runtime 进入 `FAILED` 并停止服务。此时该批写入结果未知，只能在 server 恢复后由
业务幂等或上层检查机制处理。

### 2.3 RowCodec / PrimaryKeyCodec（核心序列化边界）

当前客户端 row-aware facade 复用 `pms-codec` 中定义的行编码和主键编码，详见 [pms-codec.md](pms-codec.md)。

旧的 `[SchemaId + Column Offsets + Column Data]` 草案不再作为后续实现依据。新的 row value format 采用 `metadata + payload` 结构，使用 Paimon `DataField.id()` 作为持久字段标识，并显式区分非 NULL、NULL 和 missing 字段。

delete/tombstone 不通过 row value 内部的 `RowKind.DELETE` 表达，而是由 PMS KV 层表达：

```text
INSERT/UPDATE_AFTER  -> put(primaryKeyBytes, rowValueBytes)
DELETE/UPDATE_BEFORE -> delete(primaryKeyBytes)
```

当前实际实现使用 `PmsRowValueCodec.encode/decode` 与
`PmsPrimaryKeyCodec.encodeKey/encodeKeyTuple/encodePrefixTuple`。client 不再维护一套独立的
`RowCodec` 抽象。

### 2.4 SchemaTracker

后续版本可引入 SchemaTracker，定期检查 Server 端 Schema 是否变更，保持 Client 与 Server
的 Schema 一致性。V1 已明确不支持 Paimon 表 Schema 变更，因此当前不实现后台 tracker，
只保留一次性 handshake schema negotiation。

**工作机制**：

```
┌──────────────────────────────────────────────────┐
│               SchemaTracker                      │
│                                                  │
│  定期（默认 30s）向 Server 请求最新 SchemaId      │
│       │                                          │
│       ▼                                          │
│  比较本地 SchemaId 与 Server SchemaId             │
│       │                                          │
│   ┌───┴───┐                                      │
│   │一致   │不一致                                 │
│   ▼       ▼                                      │
│  无操作  1. 请求 Server 获取最新 Schema            │
│          2. 重新计算 SchemaId                     │
│          3. 更新 RowCodec/PrimaryKeyCodec        │
│          4. 记录日志                              │
└──────────────────────────────────────────────────┘
```

**被动触发**：除定期检查外，当写入收到 `SCHEMA_MISMATCH` 响应时，立即触发 Reload，不等下次定期检查。

**线程安全**：SchemaId 的更新使用 `volatile`，RowCodec/PrimaryKeyCodec 的替换使用 `AtomicReference`，保证序列化过程中不会使用到半更新状态的 codec。

以上 reload/thread-safety 设计属于 schema 变更版本的目标形态，V1 暂不落地。

## 3. 反压与重试策略

### 3.1 重试策略

当前 raw client 只对 server 明确拒绝、因而可以确认未写入的 `OVERLOADED` 与
`SHUTTING_DOWN` 做有限次数指数退避重试。默认延迟为
`10, 20, 40, 80, 160, 320, 640ms`，之后保持 640ms，上限默认 10 次。

网络超时、连接中断、`INTERNAL_ERROR` 和 fatal 断连都可能对应未知写入结果，client 不自动
重试，避免在没有 request id / 幂等协议的 V1 中制造重复提交。

### 3.2 连接管理

- 每个 `PmsRawClient` 持有一个 JDK `HttpClient`，由 JDK 负责 HTTP/2 连接复用与 stream multiplexing。
- 当前不提供显式连接池大小、多节点切换或 SDK 级自动重连策略。
- schema handshake 只在 `connect` 时执行一次；底层连接恢复不会触发 schema reload。
- binary response 按 handshake 的 `maxResponseBodyBytes` 流式限长读取。

## 4. 配置项

当前通过 `PmsClientConfig.builder(serverUri)` 配置：

| Builder 配置 | 默认值 | 说明 |
|--------|-------|------|
| `connectTimeout` | 5s | 建立连接超时，必须大于 0 |
| `writeTimeout` | 5s | 单次写请求超时，0 表示不设置 request timeout |
| `readTimeout` | 3s | 单次查询超时，0 表示不设置 request timeout |
| `writeRetryMax` | 10 | `OVERLOADED/SHUTTING_DOWN` 最大重试次数 |
| `retryInitialBackoff` | 10ms | 写重试初始退避 |
| `retryMaxBackoff` | 640ms | 写重试最大退避 |
| `requireHttp2` | true | 是否拒绝非 HTTP/2 响应 |

## 5. 当前落地状态

当前 PMS 主项目已新增 `pms-client` 模块，并落地 raw bytes 层与第一版 row-aware facade：

- `PmsRawClient`：基于 JDK `HttpClient` 使用 HTTP/2/h2c 访问 `/pms/api/v1/...`。
- 初始化时执行 `/pms/api/v1/handshake`，校验协议名、版本、HTTP/2 要求和必需 capability，并缓存 server 端 key/row/batch/body 限制。
- raw 写入：支持 `put`、`delete`、`writeBatch`，批写请求映射为一个 `RecordBatch`，成功时要求 `acceptedCount` 与请求条数一致。
- raw 查询：支持 `getLocal`、`getFull` 和 `getPrefixLocal`，保留 `HIT` / `MISS` / `DELETED` / `LOOKUP_UNAVAILABLE` 等协议语义。
- 写入重试：对 `OVERLOADED`、`SHUTTING_DOWN` 做有限次数退避重试；非 OK 结果仍按协议状态返回给调用方。
- `PmsRawBatchWriter`：面向零散 put/delete 的轻量缓冲器，按条数阈值自动 flush，close 时 drain 剩余 batch。
- `PmsClient`：在 raw client 之上依赖 `pms-codec`，以 Paimon `InternalRow` / `RowType` 为 SDK 数据边界。
- `PmsClient.connect(config)`：通过 handshake 获取 server 端固定表 schema snapshot，自动初始化 `RowType`、primary keys、SchemaId 和 codec version 校验。
- row-aware 写入：使用 `PmsPrimaryKeyCodec` 编码 primary key，使用 `PmsRowValueCodec` 编码 row value，并将 `INSERT/UPDATE_AFTER` 归一化为 put、`DELETE/UPDATE_BEFORE` 归一化为 delete。
- row-aware 查询：使用 key tuple 编码查询 key，并将 raw row bytes 解码为 `InternalRow`；`getLocal` 仍保留 `DELETED` 三态语义，`LOOKUP_UNAVAILABLE` 仍以非 OK status 暴露。

尚未实现：

- POJO / `Map<String, Object>` 等业务对象映射层；当前 row-aware facade 的正式边界是 Paimon `InternalRow`。
- schema tracker、schema reload、连接池、多节点切换、异步 API 和更完整的运行时指标。
