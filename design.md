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
2. `curMemTable` 满后或达到时间阈值，冻结为 `ImmutableMemTable`，异步刷盘生成 SST。
3. 触发 Sink 流程时，归并本地数据生成 Parquet 格式的 `preSink` 文件，调用 Paimon 2PC 接口提交新 Snapshot。

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
| Paimon 独占与 Compaction | PMS 独占 Paimon 表写入，内部直接调用 Paimon 原生 API 触发 Compaction | [pms-core.md](docs/pms-core.md) § 3.4 |
| 序列化与 Schema 兼容 | PMS Client 传输格式：`SchemaId (Hash) + 各列偏移量 + 二进制串` | [pms-client.md](docs/pms-client.md) |
| 流控 | 两层水位线：NORMAL（正常）/ OVERLOADED（拒绝写入） | [pms-core.md](docs/pms-core.md) § 4 |
| 并发模型 | 无全局锁，volatile 引用原子切换 + 引用计数，初期不做快照读 | [pms-core.md](docs/pms-core.md) § 5 |
| 优雅停机 | Drain → Quiesce → Shutdown 三阶段 | [pms-server.md](docs/pms-server.md) § 2.5 |

> **后续演进**：双盘 WAL（主盘 + 备盘同步写、互恢复）作为后续演进方向，V1 不实现。

## 4. 模块拆分

### 4.1 pms-client
Java SDK，负责序列化、Schema 获取、RPC 通信。零依赖设计。
- 详见 [pms-client.md](docs/pms-client.md)。

### 4.2 flink-connector-pms
Flink Sink 实现，负责对接 Flink 记录格式，调用 `pms-client`。
- 详见 [flink-connector-pms.md](docs/flink-connector-pms.md)。

### 4.3 pms-server
PMS 的可执行外壳。负责解析配置、管理生命周期、暴露 RPC 接口、编排故障恢复流程。它是 pms-core 的消费者。
- `rpc-server`: 对外暴露写入与点查接口，集成流控水位线。
- `recovery-manager`: 启动恢复管理器。
- `config-manager`: 配置管理器，启动时从外部配置源加载配置构造 `PMSConfig`，注入到各核心组件。V1 不支持运行时热更新。
- `background-task-scheduler`: 定时任务调度，驱动 Freeze/Flush/Sink/Compaction/Evict/WAL 截断。
- `graceful-shutdown`: 分阶段优雅停机。
- 详见 [pms-server.md](docs/pms-server.md)。

### 4.4 pms-core
核心组件代码，单机 LSM 存储引擎，完全不依赖任何 RPC 框架、Web 容器或外部配置中心。
- `config`: 配置契约层，定义 `PMSConfig` Record，启动时一次性注入。
- `memtable-engine`: 管理 SkipList、内存状态机、引用计数。
- `local-storage`: 本地行存 SST 的写入/读取/归并/BloomFilter，SST 文件带 Footer CRC 校验。
- `wal-engine`: V1 单盘 WAL 的写入、索引与重放。
- `paimon-sink-manager`: 封装 Paimon 2PC 提交、Compaction 触发、穿透查询。
- `bucket-director`: 总协调器，管理状态机流转、查询穿透、后台任务协调。详见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md)。
- `statistic`: 可观测性基础设施。TODO: 详细设计待核心组件稳定后再补充。
- 详见 [pms-core.md](docs/pms-core.md)。

## 5. 设计文档索引

| 文档 | 说明 |
|------|------|
| [design.md](design.md) | 顶层架构设计（本文档） |
| [pms-core.md](docs/pms-core.md) | 核心引擎组件、接口定义、流控、并发模型、数据完整性 |
| [pms-core-bucket-director.md](docs/pms-core-bucket-director.md) | 状态机协调器 |
| [pms-core-statistic.md](docs/pms-core-statistic.md) | 可观测性基础设施（待详细设计） |
| [pms-server.md](docs/pms-server.md) | 服务端外壳 |
| [pms-client.md](docs/pms-client.md) | 客户端 SDK |
| [flink-connector-pms.md](docs/flink-connector-pms.md) | Flink Connector |
| [pms-testing.md](docs/pms-testing.md) | 测试策略 |
| [paimon-primary-key-encoding.md](references/paimon-primary-key-encoding.md) | Paimon 主键序列化与排序语义参考 |
