# PMS (Paimon MemTable Service) 架构设计文档 V1.2

## 1. 项目定位
PMS 是一个独立于计算引擎的单机存储服务，作为 Apache Paimon 数据湖的加速层。
- **核心价值**：提供实时（毫秒级）的数据新鲜度点查能力；作为高性能缓冲池，吸收高并发写入，降低对 Paimon 底层存储的 IO 压力与 Snapshot 膨胀。
- **设计原则**：PMS 是绑定单一 Paimon 表的专用服务；PMS 是该 Paimon 表的**唯一写入者**，独占 Snapshot 生成权；不影响 Paimon 原有的 AP 分析能力；PMS 内部和 Sink 到 Paimon 的数据处理均遵循 Paimon Deduplicate Merge Engine 规则（同一主键只保留最新记录，最新记录为 DELETE 则删除全部同主键记录），不允许其他 Merge Engine，这保证了 PMS 内部数据处理和查询的简单性。
- **V1 边界**：PMS 绑定的 Paimon 表 Schema 在 PMS 本地状态生命周期内保持不变。与“PMS 是该表唯一写入者”相同，这是一项由部署与用户行为保证的前置条件，V1 不主动监控或阻止 Schema 变更，也不承诺变更后的读写行为。需要变更 Schema 时，应先停止上游写入并将当前 fence 完整 Sink 到 Paimon，停止 PMS 后使用全新的 WAL、storage 与 lookup cache 目录部署新实例，不复用旧 Schema 对应的本地状态。后续版本再考虑 Schema 变更的程序化检测与演进策略。

## 2. 核心架构：双层 LSM 模型
PMS 在 Paimon 的 LSM 之上，构建了一层基于本地内存和磁盘的 LSM 缓冲。

### 2.1 写入路径

1. 数据通过 RPC 写入 `curMemTable`（基于 SkipList）。
2. Bucket 内部分配单调递增的 `sequenceId`，WAL 与 MemTable 同时记录该值。V1 使用轻量级 sequence：只确定写入边界，不提供 MVCC 快照读。
3. `curMemTable` 达到条目数或字节数上限时自动冻结为 `ImmutableMemTable`；Paimon 可见性或手动管理请求也可以建立固定 sequence fence 并触发冻结。冻结结果携带 `minSequenceId/maxSequenceId`，由独立 Flush worker 异步刷盘生成 SST。V1 不按固定时间周期制造小 MemTable/SST。
4. 触发 Sink 流程时，选择待 sink 的 `newSST` 形成 `SinkBatch`，后续通过有序 iterator 与 RowCodec 对接 Paimon 2PC。Sink prepare/success 写入独立 `SinkMeta`，其中的 `sstIds/persistedSequenceId` 用于恢复 SST sinked 状态和安全 WAL 截断；Paimon snapshotId 只表示外部提交结果。

详细流程参见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md)。

### 2.2 当前查询路径
点查请求分为默认完整表点查和 PMS-local 点查。默认 `get` 按层级穿透，命中即返回：`curMemTable → ImmutableMemTable → 本地 SST → pms-lookup-paimon`。`getLocal` 只查询 PMS 内部层，并返回 HIT / DELETED / MISS 三态；其中 DELETED 必须阻断后续历史数据查询。

`pms-lookup-paimon` 为 `(partition, bucket)` 维护完整的 live `DataFileMeta` view，按 Paimon merge-tree 文件优先级执行 direct Parquet point lookup，并可为热点文件构建本地 value SST。其结果为 HIT / DELETED / MISS / UNKNOWN：只有完整有效 view 的 MISS 才返回 not found；UNKNOWN 表示 PMS 不能证明结果正确，server 必须返回可重试错误，不能将其降级为 MISS。生产路径不使用 Paimon `ReadBuilder`；它仅保留为测试和压测的正确性对照。详见 [pms-lookup-paimon.md](docs/pms-lookup-paimon.md)。

> direct Parquet 查询、普通 sink delta 发布、server 切换与热点 value SST cache 的 server 集成已实现。本地 core/direct/cached 基准已实现；显式 compaction、远端压测和更细粒度指标仍按 [pms-lookup-paimon.md](docs/pms-lookup-paimon.md) 后续阶段推进。

### 2.3 分层保留与淘汰
- **MemTable 是写缓冲，不是长期缓存**：ImmutableMemTable 只在等待 Flush 时参与查询。SST 完整落盘并原子发布后，源 ImmutableMemTable 从可见状态移除；不维护 `SST + MemTable` 双持缓存。
- **本地 SST 分为 NEW 与 SINKED**：NEW 只存在于 PMS 本地与 WAL 恢复链路中；SINKED 已确认进入 Paimon，仍可作为本地热点窗口保留。
- **同状态合并**：NEW 只与 NEW 合并，SINKED 只与 SINKED 合并。Sink 与本地 compact 相互独立，已经 Sink 的 SST 仍可继续 compact。
- **最老优先淘汰**：只有 SINKED run 可以被淘汰，并且每次只淘汰最老的一个。V1 分别通过 NEW/SINKED 文件数量水位维护本地窗口，不在进程内实现磁盘剩余空间调度；部署侧为 storage 目录提供独立磁盘或配额与外部告警。
- **连续本地 run 后缀**：当前可见 SST 的 flushId range 必须构成一个连续后缀；最老 run 可被淘汰，所以不要求从 1 开始。Flush 失败或 boundary orphan 恢复不消费 flushId。retired 文件按 data-first 顺序幂等清理，启动只自动修复 compact 覆盖、最老 evict 半删除和未推进 boundary 的 orphan，其他内部缺口直接失败并由人工介入。

调度器由独立的 Flush worker 与 Maintenance worker 驱动。Maintenance 每次基于最新状态按固定优先级只选择一个操作，完成后重新采样，避免维护目的与 core 操作 API 耦合。详见 [pms-server.md](docs/pms-server.md) § 2.4。

## 3. 关键机制

| 机制 | 说明 | 详细文档 |
|------|------|---------|
| WAL 与崩溃恢复 | V1 采用单盘 WAL，每条记录带 CRC32 校验和 Magic Number。WAL 只记录数据变更；Flush/Sink 使用独立可读 metadata 记录恢复边界与 Paimon prepare/commit 进度。 | [pms-core.md](docs/pms-core.md) § 5 / [pms-recovery-metadata.md](docs/pms-recovery-metadata.md) |
| 轻量级 Sequence | 每条写入分配单调递增 sequenceId，作为 freeze/flush/sink/WAL 截断的内部边界坐标。V1 不做 MVCC 多版本。 | [pms-sequence-and-write-boundary.md](docs/pms-sequence-and-write-boundary.md) |
| 本地 SST 格式 | 参考 LevelDB/RocksDB Block Based Table，保留 Data Block/Index/Footer 结构，不照搬 MVCC InternalKey；SST 查询使用 `Optional<Value>` 表达 miss/put/delete 三态。 | [pms-core-sst-format.md](docs/pms-core-sst-format.md) |
| SST 当前状态 | 汇总 SST/WAL 恢复边界、已接入的 RowCodec 与真实 Paimon sink。 | [pms-core-sst-current-status.md](docs/pms-core-sst-current-status.md) |
| Paimon 独占与 Compaction | 当前保留普通 sink 的 Paimon 隐式 compaction 与 snapshot 清理；独立显式 compaction 调度延后 | [pms-core.md](docs/pms-core.md) § 3.4 / [pms-sink-paimon.md](docs/pms-sink-paimon.md) § 10 |
| Paimon 历史点查 | `pms-lookup-paimon` 维护 partition-bucket live 文件视图；成功 commit 后严格有序地发布 data/compact delta，失效后由完整 snapshot 重建 | [pms-lookup-paimon.md](docs/pms-lookup-paimon.md) |
| 行编码与 Schema 兼容 | `pms-codec` 负责 Paimon `InternalRow` 与 PMS KV bytes 的转换；delete/tombstone 由 KV 层表达，不写入 row value。 | [pms-codec.md](docs/pms-codec.md) |
| 外部协议 | `pms-protocol` 定义 HTTP/2 binary hot path 的 raw bytes wire contract、handshake、status 与 batch envelope；不解释 key/value bytes。 | [pms-protocol.md](docs/pms-protocol.md) |
| 流控 | 两层水位线：NORMAL（正常）/ OVERLOADED（拒绝写入） | [pms-core.md](docs/pms-core.md) § 4 |
| 并发模型 | 写入路径保持短临界区以对齐 WAL 顺序、MemTable 可见顺序和 sequence 边界；Freeze 使用对象切换；SST 查询通过 read epoch 快照隔离 compact/evict；慢 IO 不持写入锁 | [pms-core.md](docs/pms-core.md) § 5 |
| 后台调度 | Flush 与 Maintenance 两个单线程 worker；按 Paimon 可见性 fence 和 NEW/SINKED 数量水位执行单步调和 | [pms-server.md](docs/pms-server.md) § 2.4 |
| 停机 | 进入 `SHUTTING_DOWN` 后停止接收请求并关闭 scheduler，不强制 Freeze/Flush/Sink；未下沉数据由 WAL、本地 SST 和 SinkMeta 在下次启动恢复 | [pms-server.md](docs/pms-server.md) § 2.5 |

> **后续演进**：双盘 WAL（主盘 + 备盘同步写、互恢复）作为后续演进方向，V1 不实现。

## 4. 模块拆分

### 4.1 pms-core
核心组件代码，单机 LSM 缓冲引擎，不依赖 RPC 框架、Web 容器或外部配置中心。`pms-core` 的接口保持 byte-oriented：`byte[] key`、`byte[] value` 和 `delete(key)`，不直接暴露 Paimon `InternalRow`。
- `config`: 配置契约层，定义 `PMSConfig` Record，启动时一次性注入。
- `memtable-engine`: 管理可写 CurMemTable、等待 Flush 的 ImmutableMemTable 与原子状态切换。
- `local-storage`: 本地行存 SST 的写入/读取/归并/BloomFilter，SST 文件带 Footer CRC 校验。
- `wal-engine`: V1 单盘 DATA WAL 的写入、索引与重放。
- `sink`: 定义 `SinkManager` SPI、`SinkBatch`、`PreparedSinkCommit`、`SinkCommitResult` 和 `SinkMetaStore`；生产实现由 `pms-sink-paimon` 注入，core 测试使用 fake/mock。
- `bucket-director`: 总协调器，管理状态机流转、查询穿透、后台任务协调。详见 [pms-core-bucket-director.md](docs/pms-core-bucket-director.md)。
- 可观测性：当前通过 core 状态快照与 server `/state` 暴露边界、水位和后台任务状态；独立 `statistic` 模块与统一指标注册接口尚未实现，作为后续演进。
- 详见 [pms-core.md](docs/pms-core.md)。

### 4.2 pms-codec
Paimon 行格式与 PMS KV bytes 的适配层，依赖 Paimon 类型系统，但不反向依赖 `pms-core`。
- `RowValueCodec`: 将 Paimon `InternalRow` 编码为 row value bytes，并支持从 bytes 解码完整行或投影列。
- `RowValueView`: 只解析 row value 元数据和 payload 边界，支持按 `DataField.id()` 查找字段。
- `PrimaryKeyCodec`: 将 Paimon `InternalRow` 的主键列编码为与 Paimon 主键排序一致的 `byte[] key`。
- `RowKind` 归一化：`INSERT/UPDATE_AFTER` 写入 `put(key, valueBytes)`；`DELETE/UPDATE_BEFORE` 转为 `delete(key)`，不把 delete tombstone 编码进 value bytes。
- 详见 [pms-codec.md](docs/pms-codec.md)。

### 4.3 pms-sink-paimon
真实 Paimon Sink 适配层，为 `pms-core` 的 `SinkManager` SPI 提供生产实现。
- 实现 `pms-core` 定义的 `SinkManager` SPI，由 `pms-server` 注入 `PMSBucketDirectorImpl`。
- 读取 `pms-core` 暴露的 SST ordered iterator。
- 使用 `pms-codec` 将 value bytes 解码为 Paimon `InternalRow`。
- 调用 Paimon 2PC API 完成 prepare / commit，并把 `persistedSequenceId`、`sstIds` 等结果交由 `pms-core` 写入 `SinkMeta`；本地 SST 逻辑顺序由 `flushId` range 表达。
- 该模块依赖 `pms-core`、`pms-codec` 和 Paimon。

### 4.4 pms-server
PMS 的可执行外壳。负责解析配置、管理生命周期、暴露 RPC 接口、编排故障恢复流程。它是 pms-core 的消费者。
- `rpc-server`: 对外暴露写入与点查接口，集成流控水位线；热路径使用 `pms-protocol` 定义的 HTTP/2 binary raw bytes API。
- `recovery-manager`: 启动恢复管理器。
- `config-manager`: 配置管理器，启动时从外部配置源加载配置构造 `PMSConfig`，注入到各核心组件。V1 不支持运行时热更新。
- `background-task-scheduler`: 两个独立 worker 驱动 Immutable Flush、可见性 fence、Sink、local compact 和 Evict；每次从 core 快照重新决策。
- `shutdown`: 采用 recovery-first 语义，停止入口和后台调度后直接关闭本地组件，不在停机路径执行强制 Drain。
- 详见 [pms-server.md](docs/pms-server.md)。

### 4.5 pms-lookup-paimon
Paimon primary-key 历史点查模块。由 `pms-server` 直接创建和调用，维护 bucket-scoped `DataFileMeta` live view，执行 direct Parquet lookup，并管理热点文件的本地 value SST cache。
- 本模块不依赖 `pms-core`，不改变 core 的 byte-oriented 边界。
- server 使用 Paimon `RowKeyExtractor` 从完整主键请求解析 partition、bucket 和 trimmed primary key。
- sink/compaction commit 成功后，server 使用 Paimon `CommitMessage` 中的 data/compact file delta 更新 view；重启或失效后从完整 snapshot 重建，不回放不确定 delta。
- 当前只支持固定 hash bucket、Parquet、primary-key + deduplicate 的已验证 profile；UNKNOWN 以可重试错误暴露。
- 详见 [pms-lookup-paimon.md](docs/pms-lookup-paimon.md)。

### 4.6 pms-protocol
PMS 对外协议契约模块，不依赖 `pms-core`、`pms-codec` 或 Paimon runtime。
- 定义 HTTP/2 binary hot path 的 endpoint、capability、handshake 与 status code。
- 定义 raw `keyBytes` / `rowBytes` 写入、删除、点查与 prefix 查询的 DTO。
- 提供 `RecordBatch`、`KeyBatch`、`WriteResult`、`LookupBatchResult` 的 v1 binary codec。
- 协议层只传输 opaque bytes，不解释 PMS primary key 或 row value 格式。
- 详见 [pms-protocol.md](docs/pms-protocol.md)。

### 4.7 pms-client
Java SDK，负责 RPC 通信、batching、反压重试，并在 row-aware facade 中通过 `pms-codec` 完成行编码。已实现 raw bytes HTTP/2 client、batching client 和依赖 `pms-codec` 的 row-aware facade。
- 详见 [pms-client.md](docs/pms-client.md)。

### 4.8 flink-connector-pms
Flink 1.20 SQL/Table Connector，负责把 Flink `RowData` 适配为 PMS 写入与当前态点查：
提供 At-Least-Once Sink、processing-time Lookup Join Source，以及仅接受完整主键常量
等值条件的 SQL DELETE pushdown。Connector 依赖 `pms-client`，不向 `pms-core` 泄漏
Flink/Paimon 行类型；MVP 不提供 Scan Source、Catalog、Connector 本地 Lookup cache 或
Exactly-Once 协议。发布 JAR 将 Flink 依赖保持为 provided，重定位随包发布的 Paimon
API/Common 类和 PMS 协议使用的 Jackson，并排除 Connector 未使用的 Paimon
Core/Format 及重复传递依赖。
- 详见 [flink-connector-pms.md](docs/flink-connector-pms.md)。

### 4.9 pms-dist
Linux 二进制发行包组装模块。它不包含业务实现，只把 `pms-server` 及其运行时依赖、Shell
脚本和默认配置组装为展开目录与 `tar.gz`。发行包使用 `lib/*` classpath，不构建 fat JAR。
- 详见 [pms-dist.md](docs/pms-dist.md)。

### 4.10 pms-tests
仓库内测试子项目，不进入 `pms-dist`，包含可复用的 `pms-testkit` 和显式 profile 执行的
`pms-integration-tests`，以及提供本地 core 与 Paimon direct/cached 查询基准的 `pms-benchmark`。生产模块不依赖任何测试模块。
- 详见 [PMS 测试策略与测试子项目设计](docs/tests/pms-testing-strategy.md)。

### 4.11 依赖方向

当前模块按协议契约、core、codec、server/client 外壳和 Paimon 适配层分层，依赖方向如下。

```text
pms-protocol    -> JDK + Jackson(handshake JSON)
pms-codec       -> Paimon
pms-core        -> 不依赖 pms-codec，不暴露 InternalRow
pms-sink-paimon -> pms-core + pms-codec + Paimon
pms-lookup-paimon -> Paimon
pms-server      -> pms-protocol + pms-core + pms-codec + pms-sink-paimon + pms-lookup-paimon
pms-client      -> pms-protocol + pms-codec(row-aware facade)
flink-connector -> pms-client
pms-dist        -> pms-server(runtime distribution)
pms-tests       -> production modules（仅测试/benchmark，不进入生产依赖）
```

## 5. 设计文档索引

| 文档 | 说明 |
|------|------|
| [design.md](design.md) | 顶层架构设计（本文档） |
| [pms-core.md](docs/pms-core.md) | 核心引擎组件、接口定义、流控、并发模型、数据完整性 |
| [pms-core-bucket-director.md](docs/pms-core-bucket-director.md) | 状态机协调器 |
| [pms-core-sst-format.md](docs/pms-core-sst-format.md) | 本地 SST 文件格式、LevelDB 对照、查询三态语义 |
| [pms-core-local-run-compaction.md](docs/pms-core-local-run-compaction.md) | 基于 flushId range 的 local run、SST 命名、compact/sink/evict 边界设计 |
| [pms-core-sst-current-status.md](docs/pms-core-sst-current-status.md) | SST 当前实现状态、恢复边界与 codec/sink 协作 |
| [pms-recovery-metadata.md](docs/pms-recovery-metadata.md) | WAL 只记录数据、SSTMeta/SinkMeta 独立记录恢复边界的设计 |
| [pms-codec.md](docs/pms-codec.md) | Paimon 行编码、主键编码、RowKind 与 tombstone 边界 |
| [pms-protocol.md](docs/pms-protocol.md) | PMS HTTP/2 binary 外部协议、handshake、status、batch envelope |
| [pms-sink-paimon.md](docs/pms-sink-paimon.md) | 真实 Paimon sink、prepare/commit、delete 语义与恢复协作 |
| [pms-lookup-paimon.md](docs/pms-lookup-paimon.md) | Paimon 历史点查、live 文件视图、commit delta 发布、失败与恢复语义 |
| [pms-row-codec-format.md](docs/pms-row-codec-format.md) | PMS row value byte layout、字段查找、列值编码规则 |
| [pms-primary-key-codec.md](docs/pms-primary-key-codec.md) | PMS primary key ordered byte layout、TiDB mem-comparable 对照、前缀扫描规则 |
| [pms-sequence-and-write-boundary.md](docs/pms-sequence-and-write-boundary.md) | Sequence、写入边界、锁粒度与 WAL 优化设计说明 |
| [pms-core-statistic.md](docs/pms-core-statistic.md) | 可观测性基础设施（待详细设计） |
| [pms-benchmark.md](docs/pms-benchmark.md) | PMS benchmark 分层、pms-core db_bench-like 基准设计 |
| [pms-server.md](docs/pms-server.md) | 服务端外壳 |
| [pms-dist.md](docs/pms-dist.md) | Linux 二进制发行包、脚本与运行目录约定 |
| [pms-client.md](docs/pms-client.md) | 客户端 SDK |
| [flink-connector-pms.md](docs/flink-connector-pms.md) | Flink Connector |
| [pms-testing-strategy.md](docs/tests/pms-testing-strategy.md) | 全项目测试策略、发布门槛与 `pms-tests` 子项目边界 |
| [pms-testkit.md](docs/tests/pms-testkit.md) | 测试环境、资源所有权、Paimon fixture/verifier 与 PMS 子进程基础设施 |
| [pms-integration-tests.md](docs/tests/pms-integration-tests.md) | 远端 HDFS、独立 PMS 进程与崩溃恢复集成测试 |
| [paimon-primary-key-encoding.md](references/paimon-primary-key-encoding.md) | Paimon 主键序列化与排序语义参考 |
