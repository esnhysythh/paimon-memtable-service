# PMS Client 设计文档

## 1. 模块定位
Java SDK，供外部程序（如 Flink Connector）调用，屏蔽底层 RPC、batching 与序列化细节。

后续落地按两层推进：

- raw client：依赖 `pms-protocol`，只处理 HTTP/2 binary transport、handshake、raw `byte[] keyBytes` / `byte[] rowBytes`、batching、结果分类和关闭 drain。
- row-aware facade：在 raw client 之上依赖 `pms-codec`，负责 Paimon row/key 编码、RowKind 归一化和 schema 相关能力。

早期零依赖客户端包不再作为强约束。若后续需要面向用户作业提供更轻的 artifact，可在 raw client 与 row-aware facade 稳定后再评估 shading 或拆包。

## 2. 核心组件

### 2.1 PMSClient

与 PMS Server 通信的主入口。

**初始化流程**：

```
1. 建立 RPC 连接（连接池，默认 1 连接，可配置）
2. 调用 Server 的 handshake 接口，获取当前 Schema 和 SchemaId
3. 初始化 RowCodec/PrimaryKeyCodec（命名和实现以后续 codec 阶段为准）
4. 启动 SchemaTracker 定期检查线程
```

**接口**：

```java
class PMSClient implements AutoCloseable {
    // 初始化
    PMSClient(PMSClientConfig config);

    // 写入
    WriteStatus write(RowData row);
    WriteStatus write(byte[] primaryKey, byte[] binaryRow);

    // 查询
    Optional<RowData> get(byte[] primaryKey);
    Optional<byte[]> getRaw(byte[] primaryKey);
    LocalLookupResult getLocal(byte[] primaryKey);

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

**WriteStatus**：

| 状态 | 含义 | Client 行为 |
|------|------|------------|
| `OK` | 写入成功 | 继续 |
| `OVERLOADED` | 系统过载，被拒绝 | 重试（指数退避） |
| `SCHEMA_MISMATCH` | Schema 不一致 | 触发 Reload 后重试 |
| `SHUTTING_DOWN` | 服务停机 | 切换节点或等待 |

### 2.2 RowCodec / PrimaryKeyCodec（核心序列化边界）

当前阶段尚未在主项目中实现正式 codec。后续客户端与服务端应复用 `pms-codec` 中定义的行编码和主键编码，详见 [pms-codec.md](pms-codec.md)。

旧的 `[SchemaId + Column Offsets + Column Data]` 草案不再作为后续实现依据。新的 row value format 采用 `metadata + payload` 结构，使用 Paimon `DataField.id()` 作为持久字段标识，并显式区分非 NULL、NULL 和 missing 字段。

delete/tombstone 不通过 row value 内部的 `RowKind.DELETE` 表达，而是由 PMS KV 层表达：

```text
INSERT/UPDATE_AFTER  -> put(primaryKeyBytes, rowValueBytes)
DELETE/UPDATE_BEFORE -> delete(primaryKeyBytes)
```

**接口**：

```java
class RowCodec {
    // 序列化
    byte[] serialize(RowData row);
    byte[] serialize(byte[] primaryKey, byte[] binaryRow);

    // 反序列化
    RowData deserialize(byte[] data);
    byte[] extractColumn(byte[] data, int columnIndex);

    // SchemaId / SchemaVersion
    int currentSchemaId();
}

class PrimaryKeyCodec {
    byte[] encodePrimaryKey(RowData row);
}
```

### 2.3 SchemaTracker

定期检查 Server 端 Schema 是否变更，保持 Client 与 Server 的 Schema 一致性。

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

## 3. 反压与重试策略

### 3.1 重试策略

```java
class WriteRetryPolicy {
    // 指数退避 + 抖动
    long nextRetryDelayMs(int attempt, WriteStatus status) {
        if (status == SCHEMA_MISMATCH) return 0; // 立即 Reload 后重试
        if (status == SHUTTING_DOWN) return 5000; // 等 5s 再试

        // OVERLOADED: 指数退避
        long base = 10; // 10ms
        long delay = base * (1L << Math.min(attempt, 6)); // 10, 20, 40, 80, 160, 320, 640ms
        long jitter = ThreadLocalRandom.current().nextLong(delay / 2);
        return delay + jitter;
    }

    int maxRetries() { return 10; } // 最大重试次数
}
```

### 3.2 连接管理

- 单连接模式：适用于 Flink Sink 等单线程写入场景。
- 连接池模式：适用于多线程并发写入场景，连接数可配置。
- 连接断开后自动重连（指数退避，最大间隔 30s）。
- 连接建立时自动触发 Schema 校验。

## 4. 配置项

| 配置项 | 默认值 | 说明 |
|--------|-------|------|
| `pms.client.server_host` | localhost | PMS Server 地址 |
| `pms.client.server_port` | 9090 | PMS Server 端口 |
| `pms.client.connection_pool_size` | 1 | 连接池大小 |
| `pms.client.schema_check_interval_ms` | 30000 | Schema 检查间隔 |
| `pms.client.write_retry_max` | 10 | 最大重试次数 |
| `pms.client.write_timeout_ms` | 5000 | 单次写入超时 |
| `pms.client.read_timeout_ms` | 3000 | 单次查询超时 |
