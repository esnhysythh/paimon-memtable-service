# PMS (Paimon MemTable Service) 架构设计文档 V1.2

## 1. 项目定位
PMS 是一个独立于计算引擎的单机存储服务，作为 Apache Paimon 数据湖的加速层。
- **核心价值**：提供实时（毫秒级）的数据新鲜度点查能力；作为高性能缓冲池，吸收高并发写入，降低对 Paimon 底层存储的 IO 压力与 Snapshot 膨胀。
- **设计原则**：PMS 是绑定单一 Paimon 表的专用服务；PMS 是该 Paimon 表的**唯一写入者**，独占 Snapshot 生成权；不影响 Paimon 原有的 AP 分析能力；PMS 内部和 Sink 到 Paimon 的数据处理均遵循 Paimon Deduplicate Merge Engine 规则（同一主键只保留最新记录，最新记录为 DELETE 则删除全部同主键记录），不允许其他 Merge Engine，这保证了 PMS 内部数据处理和查询的简单性。
- **V1 边界**：PMS 绑定的 Paimon 表 Schema 不变，Schema 变更不在 V1 预期范围内。若运行时检测到 Schema 变更，PMS 将报 Fatal Error 并停止运行。后续版本再考虑 Schema 变更的应对策略。

## 2. 核心架构：双层 LSM 模型
PMS 在 Paimon 的 LSM 之上，构建了一层基于本地内存和磁盘的 LSM 缓冲。

### 2.1 写入路径

1. 数据通过 RPC 写入 `curMemTable`（基于 SkipList）。
2. Bucket 内部分配单调递增的 `sequenceId`，WAL 与 MemTable 同时记录该值。V1 使用轻量级 sequence：只确定写入边界，不提供 MVCC 快照读。
3. `curMemTable` 满后或达到时间阈值，冻结为 `ImmutableMemTable`，冻结结果携带 `minSequenceId/maxSequenceId`，异步刷盘生成 SST。
4. 触发 Sink 流程时，选择待 sink 的 `newSST` 形成 `SinkBatch`，后续通过有序 iterator 与 RowCodec 对接 Paimon 2PC。`SINK_PREPARE/SINK_SUCCESS` 写入 WAL，成功记录中的 `sstIds/persistedSequenceId` 是判断 SST 是否已 sink 和未来 WAL 截断的可靠依据；Paimon snapshotId 只表示外部提交结果。

详细流程参见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md)。

### 2.2 查询路径
点查请求按层级穿透，命中即返回：`curMemTable → ImmutableMemTable → 本地 SST → Paimon 穿透`。本地 SST 带有 BloomFilter 加速；Paimon 穿透基于 Manifest 索引定位底层 Parquet 读取。

> **TODO:** Paimon 穿透查询的 Manifest 索引设计待补充。V1 可采用全量加载索引策略，后续按需分级优化。

### 2.3 缓存与淘汰
- **内存淘汰**：ImmutableMemTable 维护引用计数，归零后退役释放内存。带 Mem 缓存的双持状态（newSSTWithMem / sinkedSSTWithMem）可在内存不足时退化为不带 Mem 的状态。
- **本地 SST 淘汰**：对已 Sink 的 `sinkedSST` 采用"只淘汰最老"策略，规避幽灵数据问题。
- **小文件合并**：对较小/零碎的 SST（含带 Mem 缓存的 SSTWithMem）进行多路归并合并，减少文件数量，提升查询效率。详见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md) § 8.3。

## 3. 关键机制

| 机制 | 说明 | 详细文档 |
|------|------|---------|
| WAL 与崩溃恢复 | V1 采用单盘 WAL，每条记录带 CRC32 校验和 Magic Number。WAL 包含数据写入记录及 Sink 状态机记录，支持崩溃后恢复。 | [pms-core.md](docs/pms-core.md) § 5 |
| 轻量级 Sequence | 每条写入分配单调递增 sequenceId，作为 freeze/flush/sink/WAL 截断的内部边界坐标。V1 不做 MVCC 多版本。 | [pms-sequence-and-write-boundary.md](docs/pms-sequence-and-write-boundary.md) |
| 本地 SST 格式 | 参考 LevelDB/RocksDB Block Based Table，保留 Data Block/Index/Footer 结构，不照搬 MVCC InternalKey；SST 查询使用 `Optional<Value>` 表达 miss/put/delete 三态。 | [pms-core-sst-format.md](docs/pms-core-sst-format.md) |
| SST 当前状态 | 汇总当前 SST/MockSink/WAL 恢复边界状态，并列出后续 RowCodec 与 Paimon sink 对接要求。 | [pms-core-sst-current-status.md](docs/pms-core-sst-current-status.md) |
| Paimon 独占与 Compaction | PMS 独占 Paimon 表写入，内部直接调用 Paimon 原生 API 触发 Compaction | [pms-core.md](docs/pms-core.md) § 3.4 |
| 行编码与 Schema 兼容 | `pms-codec` 负责 Paimon `InternalRow` 与 PMS KV bytes 的转换；delete/tombstone 由 KV 层表达，不写入 row value。 | [pms-codec.md](docs/pms-codec.md) |
| 流控 | 两层水位线：NORMAL（正常）/ OVERLOADED（拒绝写入） | [pms-core.md](docs/pms-core.md) § 4 |
| 并发模型 | 写入路径保持短临界区以对齐 WAL 顺序、MemTable 可见顺序和 sequence 边界；flush/sink 等慢路径异步执行，初期不做快照读 | [pms-core.md](docs/pms-core.md) § 5 |
| 优雅停机 | Drain → Quiesce → Shutdown 三阶段 | [pms-server.md](docs/pms-server.md) § 2.5 |

> **后续演进**：双盘 WAL（主盘 + 备盘同步写、互恢复）作为后续演进方向，V1 不实现。

## 4. 模块拆分

### 4.1 pms-core
核心组件代码，单机 LSM 缓冲引擎，不依赖 RPC 框架、Web 容器或外部配置中心。`pms-core` 的接口保持 byte-oriented：`byte[] key`、`byte[] value` 和 `delete(key)`，不直接暴露 Paimon `InternalRow`。
- `config`: 配置契约层，定义 `PMSConfig` Record，启动时一次性注入。
- `memtable-engine`: 管理 SkipList、内存状态机、引用计数。
- `local-storage`: 本地行存 SST 的写入/读取/归并/BloomFilter，SST 文件带 Footer CRC 校验。
- `wal-engine`: V1 单盘 WAL 的写入、索引与重放。
- `sink`: 定义 `SinkManager` SPI、`SinkBatch`、`PreparedSinkCommit`、`SinkCommitResult` 和 WAL 编码；当前提供 `MockSinkManager`，后续真实 Paimon sink 由上层注入实现。
- `bucket-director`: 总协调器，管理状态机流转、查询穿透、后台任务协调。详见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md)。
- `statistic`: 可观测性基础设施。TODO: 详细设计待核心组件稳定后再补充。
- 详见 [pms-core.md](docs/pms-core.md)。

### 4.2 pms-codec
Paimon 行格式与 PMS KV bytes 的适配层，依赖 Paimon 类型系统，但不反向依赖 `pms-core`。
- `RowValueCodec`: 将 Paimon `InternalRow` 编码为 row value bytes，并支持从 bytes 解码完整行或投影列。
- `RowValueView`: 只解析 row value 元数据和 payload 边界，支持按 `DataField.id()` 查找字段。
- `PrimaryKeyCodec`: 将 Paimon `InternalRow` 的主键列编码为与 Paimon 主键排序一致的 `byte[] key`。
- `RowKind` 归一化：`INSERT/UPDATE_AFTER` 写入 `put(key, valueBytes)`；`DELETE/UPDATE_BEFORE` 转为 `delete(key)`，不把 delete tombstone 编码进 value bytes。
- 详见 [pms-codec.md](docs/pms-codec.md)。

### 4.3 pms-sink-paimon
真实 Paimon Sink 适配层，后续用于替换 `pms-core` 中当前的 `MockSinkManager`。
- 实现 `pms-core` 定义的 `SinkManager` SPI，由 `pms-server` 注入 `PMSBucketDirectorImpl`。
- 读取 `pms-core` 暴露的 SST ordered iterator。
- 使用 `pms-codec` 将 value bytes 解码为 Paimon `InternalRow`。
- 调用 Paimon 2PC API 完成 prepare / commit，并把 `persistedSequenceId`、`sstIds` 等结果写回 WAL 成功记录。
- 该模块依赖 `pms-core`、`pms-codec` 和 Paimon。

### 4.4 pms-server
PMS 的可执行外壳。负责解析配置、管理生命周期、暴露 RPC 接口、编排故障恢复流程。它是 pms-core 的消费者。
- `rpc-server`: 对外暴露写入与点查接口，集成流控水位线。
- `recovery-manager`: 启动恢复管理器。
- `config-manager`: 配置管理器，启动时从外部配置源加载配置构造 `PMSConfig`，注入到各核心组件。V1 不支持运行时热更新。
- `background-task-scheduler`: 定时任务调度，驱动 Freeze/Flush/Sink/Compaction/Evict/WAL 截断。
- `graceful-shutdown`: 分阶段优雅停机。
- 详见 [pms-server.md](docs/pms-server.md)。

### 4.5 pms-client
Java SDK，负责 Schema 获取、RPC 通信、反压重试，并通过 `pms-codec` 完成行编码。早期可与 server 共用 codec 实现；是否继续保持零依赖客户端包，后续在 client 模块落地时再评估。
- 详见 [pms-client.md](docs/pms-client.md)。

### 4.6 flink-connector-pms
Flink Sink 实现，负责对接 Flink 记录格式，调用 `pms-client`。
- 详见 [flink-connector-pms.md](docs/flink-connector-pms.md)。

### 4.7 依赖方向

第一阶段先落地父工程和 `pms-core` 子模块；`pms-codec`、`pms-sink-paimon`、`pms-server`、`pms-client` 后续逐步拆出。

```text
pms-codec       -> Paimon
pms-core        -> 不依赖 pms-codec，不暴露 InternalRow
pms-sink-paimon -> pms-core + pms-codec + Paimon
pms-server      -> pms-core + pms-codec + pms-sink-paimon
pms-client      -> pms-codec + RPC client
flink-connector -> pms-client
```

## 5. 设计文档索引

| 文档 | 说明 |
|------|------|
| [design.md](design.md) | 顶层架构设计（本文档） |
| [pms-core.md](docs/pms-core.md) | 核心引擎组件、接口定义、流控、并发模型、数据完整性 |
| [pms-core-bucket-director.md](docs/pms-core-bucket-director.md) | 状态机协调器 |
| [pms-core-sst-format.md](docs/pms-core-sst-format.md) | 本地 SST 文件格式、LevelDB 对照、查询三态语义 |
| [pms-core-sst-current-status.md](docs/pms-core-sst-current-status.md) | SST 当前实现状态、mock 边界与后续 codec 交接说明 |
| [pms-codec.md](docs/pms-codec.md) | Paimon 行编码、主键编码、RowKind 与 tombstone 边界 |
| [pms-sequence-and-write-boundary.md](docs/pms-sequence-and-write-boundary.md) | Sequence、写入边界、锁粒度与 WAL 优化设计说明 |
| [pms-core-statistic.md](docs/pms-core-statistic.md) | 可观测性基础设施（待详细设计） |
| [pms-server.md](docs/pms-server.md) | 服务端外壳 |
| [pms-client.md](docs/pms-client.md) | 客户端 SDK |
| [flink-connector-pms.md](docs/flink-connector-pms.md) | Flink Connector |
| [pms-testing.md](docs/pms-testing.md) | 测试策略 |
| [paimon-primary-key-encoding.md](references/paimon-primary-key-encoding.md) | Paimon 主键序列化与排序语义参考 |
