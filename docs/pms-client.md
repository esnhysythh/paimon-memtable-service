# PMS Client 设计文档

## 1. 模块定位
轻量级 Java SDK，供外部程序（如 Flink Connector）调用，屏蔽底层 RPC 与序列化细节。零依赖设计（仅依赖 JDK），可直接打入用户 JAR 包而不引入依赖冲突。

## 2. 核心组件

### 2.1 PMSClient

与 PMS Server 通信的主入口。

**初始化流程**：

```
1. 建立 RPC 连接（连接池，默认 1 连接，可配置）
2. 调用 Server 的 handshake 接口，获取当前 Schema 和 SchemaId
3. 初始化 PMSerializer
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

    // 关闭
    void close();
}
```

**WriteStatus**：

| 状态 | 含义 | Client 行为 |
|------|------|------------|
| `OK` | 写入成功 | 继续 |
| `SERVICE_OVERLOADED` | 系统过载，被拒绝 | 重试（指数退避） |
| `SCHEMA_MISMATCH` | Schema 不一致 | 触发 Reload 后重试 |
| `SHUTTING_DOWN` | 服务停机 | 切换节点或等待 |

### 2.2 PMSerializer (核心序列化器)

**编码格式**：

```
[SchemaId (4 bytes)] + [Column Offsets (N * 4 bytes)] + [Column1 Data] + [Column2 Data] + ...
```

| 字段 | 大小 | 说明 |
|------|------|------|
| SchemaId | 4 bytes | Paimon 当前 Schema 结构的 Hash 值 |
| Column Offsets | N * 4 bytes | 每列数据的起始偏移量（相对于 Payload 起始位置） |
| Column Data | 变长 | 各列的二进制数据 |

**设计要点**：

- **SchemaId 计算**：对 Paimon Schema 的列名 + 列类型做 Hash（MurmurHash3），保证相同的 Schema 结构产生相同的 SchemaId。Schema 变更（增删列、改类型）一定会导致 SchemaId 变化。
- **偏移量设计**：使用偏移量而非长度，允许查询时直接定位某一列，无需遍历前方所有列。
- **NULL 列处理**：偏移量为 0xFFFFFFFF 表示该列为 NULL，无后续数据。

**接口**：

```java
class PMSerializer {
    // 序列化
    byte[] serialize(RowData row);
    byte[] serialize(byte[] primaryKey, byte[] binaryRow);

    // 反序列化
    RowData deserialize(byte[] data);
    byte[] extractColumn(byte[] data, int columnIndex);

    // SchemaId
    int currentSchemaId();
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
│          3. 更新 PMSerializer                    │
│          4. 记录日志                              │
└──────────────────────────────────────────────────┘
```

**被动触发**：除定期检查外，当写入收到 `SCHEMA_MISMATCH` 响应时，立即触发 Reload，不等下次定期检查。

**线程安全**：SchemaId 的更新使用 `volatile`，`PMSerializer` 的替换使用 `AtomicReference`，保证序列化过程中不会使用到半更新状态的 Serializer。

## 3. 反压与重试策略

### 3.1 重试策略

```java
class WriteRetryPolicy {
    // 指数退避 + 抖动
    long nextRetryDelayMs(int attempt, WriteStatus status) {
        if (status == SCHEMA_MISMATCH) return 0; // 立即 Reload 后重试
        if (status == SHUTTING_DOWN) return 5000; // 等 5s 再试

        // SERVICE_OVERLOADED: 指数退避
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
