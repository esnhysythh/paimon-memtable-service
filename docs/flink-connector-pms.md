# Flink Connector PMS 设计文档

## 1. 模块定位
实现 Flink 的 `Sink` 接口，将 Flink 的流式数据无缝导入 PMS。是 PMS 与 Flink 生态的桥梁，依赖 `pms-client` 完成实际通信。

## 2. 核心组件

### 2.1 PmsSinkWriter (实现 SinkWriter)

**核心方法 `write(T record)`**：

```
1. 提取记录的主键
   └─ 根据表定义的主键列，从 RowData 中提取
   └─ 按 Paimon 主键序编码规则序列化为主键字节数组

2. 调用 RowCodec 将 RowData/InternalRow 序列化为二进制串

3. 调用 PMSClient.write() 发送
   └─ 成功 → 继续
   └─ OVERLOADED → 反压处理（见 § 2.2）
   └─ SCHEMA_MISMATCH → 防御性错误；停止写入并检查部署约束
   └─ SHUTTING_DOWN → 当前 endpoint 等待恢复或由 connector 上层策略处理
```

**生命周期**：

| 方法 | 行为 |
|------|------|
| `open()` | 初始化 `PMSClient`，建立 RPC 连接 |
| `write(record)` | 序列化 + 发送 |
| `flush(boolean endOfInput)` | 等待所有 in-flight 写入完成 |
| `close()` | 关闭 `PMSClient`，释放连接 |

### 2.2 OVERLOADED 处理

当 PMS Server 处于 OVERLOADED 水位时，写入会被拒绝。Flink Connector 的处理策略：

```
收到 OVERLOADED
    │
    ▼
1. 记录 `flink.write.reject_count` 指标
    │
    ▼
2. 判断当前 Mailbox 状态
   ┌────────────────────┬─────────────────────────────┐
   │ Mailbox 有容量      │ 将重试操作放入 Mailbox       │
   │                    │ 延迟 10~640ms 后重试         │
   │                    │ 不阻塞当前线程               │
   ├────────────────────┼─────────────────────────────┤
   │ Mailbox 接近满     │ Thread.sleep(退避时间)       │
   │                    │ 天然形成 Flink 反压           │
   │                    │ 上游算子自动减速              │
   └────────────────────┴─────────────────────────────┘
    │
    ▼
3. 超过 maxRetries (默认 10) → 抛出 IOException
   → 触发 Flink 的 Failover 机制
```

**不直接抛异常导致 Job 失败的原因**：OVERLOADED 是可恢复的临时状态，PMS 后台任务正在消化积压，短暂等待后即可恢复。直接 Failover 代价更大。

### 2.3 PmsCommitter (实现 SinkCommitter - 可选)

**当前策略**：PMS 接受 At-Least-Once 语义，自行管理 Sink 时机。因此此组件可以省略，或仅作为一个空实现/心跳占位。

**未来扩展**：如果需要与 Flink Checkpoint 强绑定的 Exactly-Once 语义，可实现 `SinkCommitter`：

```
checkpoint barrier 到达
    │
    ▼
PmsSinkWriter.snapshotState()
    │ 通知 PMS 立即触发 Sink
    │ 等待 Sink 完成返回 snapshotId
    │ 将 snapshotId 作为 state 返回
    ▼
PmsCommitter.commit()
    │ 确认 snapshotId 对应的 Paimon Snapshot 可见
    │ 此时 Flink Source 可以读取到新数据
    ▼
完成
```

> 当前版本不实现此流程，待核心路径稳定后再评估。

## 3. Flink 配置项

通过 Flink 的 `TableConfig` 或 `WITH` 子句传递：

| 配置项 | 默认值 | 说明 |
|--------|-------|------|
| `pms.server.host` | localhost | PMS Server 地址 |
| `pms.server.port` | 9090 | PMS Server 端口 |
| `pms.write.retry-max` | 10 | OVERLOADED 最大重试次数 |
| `pms.write.retry-base-ms` | 10 | 重试基础延迟 |
| `pms.write.batch-size` | 1 | 批量写入大小（当前单条，预留批量接口） |

## 4. Flink SQL 集成示例

```sql
-- 创建 PMS Catalog（可选）
CREATE CATALOG pms_catalog WITH (
    'type' = 'pms',
    'pms.server.host' = 'pms-host',
    'pms.server.port' = '9090'
);

-- 通过 PMS Sink 写入
INSERT INTO pms_table
SELECT * FROM kafka_source;
```

## 5. 指标集成

Flink Connector 向 Flink 的 MetricGroup 注册以下指标：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `pms.write.total` | Counter | 总写入次数 |
| `pms.write.success` | Counter | 成功次数 |
| `pms.write.reject` | Counter | 被拒绝次数（OVERLOADED） |
| `pms.write.retry` | Counter | 重试次数 |
| `pms.write.latency_ms` | Histogram | 写入延迟分布 |
| `pms.schema_mismatch` | Counter | 收到防御性 Schema mismatch 状态的次数；出现后停止使用当前部署 |
