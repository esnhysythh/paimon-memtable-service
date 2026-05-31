# PMS Core Engine 设计文档

## 1. 模块定位
单机 LSM 缓冲引擎，完全不依赖任何 RPC 框架、Web 容器或外部配置中心。`pms-core` 负责数据的内存管理、本地持久化、WAL 协调、SST 生命周期和内部 sink 状态机边界。

`pms-core` 的运行语义服务于 Paimon，但接口保持 byte-oriented：`byte[] key`、`byte[] value` 和 `delete(key)`。Paimon `InternalRow`、`RowType`、字段投影和主键编码由后续独立的 `pms-codec` 模块负责，上层组合模块把 codec 输出接入 `pms-core`。

## 2. 配置契约

pms-core 定义所有核心配置的类型和默认值。配置的加载与解析由 pms-server 的 ConfigManager 负责，core 本身不知道配置来源（YAML / Properties / 环境变量）。

### 2.1 设计原则

- **谁消费配置，谁定义类型**：core 消费配置，所以配置类定义在 core 中。
- **纯 Record**：配置类无外部依赖，仅包含字段、默认值和校验逻辑。
- **构造时注入**：core 组件通过构造函数接收 `PMSConfig` 对象，不主动查找配置。V1 不支持运行时热更新，配置变更需重启。

### 2.2 配置类

采用分层配置结构：各模块定义自己的配置 Record，顶层 `PMSConfig` 组合各子配置，并提供 `from(Properties)` 工厂方法统一加载。

```java
// 各模块配置
record MemTableConfig(int maxEntries, int maxSizeMb) { ... }   // 默认 1_000_000 / 256
record WalConfig(String dir, int fileSizeMb, boolean useMmap) { ... }  // 必填 dir / 默认 256 / false
record StorageConfig(String dir, long sinkedMaxSizeMb, int sinkedMaxCount,
                     long localSstMaxRows, int compactThresholdMb, int compactMinFiles) { ... }
record SinkConfig(int intervalMs, int maxPendingSsts) { ... }  // 默认 30000 / 8
record FlowControlConfig(int overloadedImmutableCount,
                         int overloadedPendingSstCount) { ... }  // 默认 4 / 16
record PaimonConfig(String tablePath, String warehouse) { ... }  // 必填 tablePath

// 顶层组合
record PMSConfig(
    MemTableConfig memtable,
    WalConfig wal,
    StorageConfig storage,
    SinkConfig sink,
    FlowControlConfig flowcontrol,
    PaimonConfig paimon
) {
    static PMSConfig from(Properties props) { ... }
}
```

**配置键映射**（`from(Properties)` 使用的键名）：

| 键名 | 子配置 | 字段 | 默认值 |
|------|--------|------|--------|
| `pms.memtable.max_entries` | MemTableConfig | maxEntries | 1_000_000 |
| `pms.memtable.max_size_mb` | MemTableConfig | maxSizeMb | 256 |
| `pms.wal.dir` | WalConfig | dir | 必填 |
| `pms.wal.file_size_mb` | WalConfig | fileSizeMb | 256 |
| `pms.wal.use_mmap` | WalConfig | useMmap | false |
| `pms.storage.dir` | StorageConfig | dir | 必填 |
| `pms.storage.sinked_max_size_mb` | StorageConfig | sinkedMaxSizeMb | 10240 |
| `pms.storage.sinked_max_count` | StorageConfig | sinkedMaxCount | 100 |
| `pms.storage.local_sst_max_rows` | StorageConfig | localSstMaxRows | 0（禁用） |
| `pms.storage.compact_threshold_mb` | StorageConfig | compactThresholdMb | 32 |
| `pms.storage.compact_min_files` | StorageConfig | compactMinFiles | 4 |
| `pms.sink.interval_ms` | SinkConfig | intervalMs | 30000 |
| `pms.sink.max_pending_ssts` | SinkConfig | maxPendingSsts | 8 |
| `pms.flowcontrol.overloaded_immutable_count` | FlowControlConfig | overloadedImmutableCount | 4 |
| `pms.flowcontrol.overloaded_pending_sst_count` | FlowControlConfig | overloadedPendingSstCount | 16 |
| `pms.paimon.table_path` | PaimonConfig | tablePath | 必填 |
| `pms.paimon.warehouse` | PaimonConfig | warehouse | — |

### 2.3 组件构造方式

core 组件通过构造函数接收各自需要的子配置：

```java
class SkipListCurMemTable implements CurMemTable {
    SkipListCurMemTable(MemTableConfig config) { ... }
}

class WALManagerImpl implements WALManager {
    WALManagerImpl(PMSConfig config) { ... }  // 内部使用 config.wal()
}
```

## 3. 核心组件与接口定义

**数据语义约束**：PMS 内部和 Sink 到 Paimon 的数据处理均遵循 Paimon Deduplicate Merge Engine 规则——同一主键只保留最新记录，最新记录为 DELETE 则删除全部同主键记录。不允许其他 Merge Engine。这保证了 PMS 内部数据处理和查询的简单性：同一 Key 取最新值即可，归并时最新 Key 胜出，无需特殊合并函数。

**轻量级 sequence 边界**：PMS 为每条成功进入 WAL 的数据写入分配单调递增的 `sequenceId`。V1 中 sequence 只用于确定内部处理边界，不提供 MVCC 快照读；同一 Key 在 MemTable 中仍只保留 latest value。sequence 必须随 WAL 持久化，恢复时从 WAL 中的最大 sequence 继续递增。

sequence 的边界语义：
- `Value/Entry` 携带 latest `sequenceId`，作为该 Key 最新写入的内部顺序。
- `CurMemTable.freeze()` 产生的 `ImmutableMemTable` 记录 `minSequenceId/maxSequenceId`。
- 后续 Flush/SST/Sink 元数据也必须记录覆盖的 sequence 范围。
- Paimon `snapshotId` 表示外部提交结果；WAL 安全截断以 SinkMeta 中的 PMS 内部 `persistedSequenceId` 为主边界。
- V1 不保存同 Key 多版本；未来如果要支持 MVCC，可将 MemTable/SST key 形态升级为 `(userKey, sequenceId)` 并引入 read sequence 可见性过滤。

**Value 编码语义**：PMS 不是通用 KV 存储，MemTable 中的 `Value.bytes` 不是任意用户字节值，而是一条 Paimon `InternalRow` 的序列化结果。非删除记录必须由后续 RowCodec/序列化管理器生成，代表完整的行编码。即使业务列全部为 `NULL`，编码结果也应包含格式头、字段数量、null bitmap 或其他必要元信息，因此设计语义上不应为空 `byte[]`。`Value.bytes == null` 专用于 tombstone/delete，不表示业务层 NULL。

RowCodec 不在 value 内部表达 delete。进入 `pms-core` 前，调用方必须将 `INSERT/UPDATE_AFTER` 归一化为 `put(key, rowValueBytes)`，将 `DELETE/UPDATE_BEFORE` 归一化为 `delete(key)`。WAL 和 SST 层继续用 `valueLen = -1` 表达 tombstone。

### 3.1 MemTableEngine

管理内存中的数据缓冲，基于 SkipList 实现。接口拆分为 `CurMemTable`（可写）和 `ImmutableMemTable`（只读 + 引用计数），由 `CurMemTable.freeze()` 产生 `ImmutableMemTable`。

**CurMemTable 接口**：

```java
interface CurMemTable {
    void put(Key key, Value value);
    Value get(Key key);
    ImmutableMemTable freeze();  // 冻结为 immutable，返回只读实例
    long estimatedSize();
    int estimatedEntryCount();
    long minSequenceId();
    long maxSequenceId();
    boolean shouldFreeze();
    Iterator<Entry> iterator();
}
```

- Schema 校验由上层处理，不在此接口传递。V1 中 PMS 绑定单表，Schema 不变（变更即 Fatal Error）。
- Key 使用无符号字节比较（与 Paimon 主键序一致），参见 [paimon-primary-key-encoding.md](../../references/paimon-primary-key-encoding.md)。
- 删除不通过 `CurMemTable.delete(Key)` 表达，而是写入 `Value.tombstone(sequenceId)`。这样 tombstone 与普通 upsert 一样携带明确的 sequence 边界。

**ImmutableMemTable 接口**：

```java
interface ImmutableMemTable {
    Value get(Key key);
    Iterator<Entry> iterator();
    long estimatedSize();
    int estimatedEntryCount();
    long minSequenceId();
    long maxSequenceId();
    void incrementRef();
    void decrementRef();
    long refCount();
}
```

**实现**：

- **SkipListCurMemTable**：当前活跃的可写 MemTable。
  - 底层 `ConcurrentSkipListMap<Key, Value>`，线程安全。
  - 写入后检查是否达到 Freeze 阈值（`estimatedEntryCount() >= config.maxEntries()` 或 `estimatedSize() >= config.maxSizeBytes()`），达到则触发 Freeze。不拒绝写入，不阻塞写入路径。
  - `estimatedEntryCount` 和 `estimatedSize` 均为启发式估算值，非精确计数：高并发下 `volatile int ++` 可能丢失增量，误差在可接受范围内。
  - 跟踪当前 MemTable 的 `minSequenceId/maxSequenceId`，作为 freeze 后的边界元数据。
  - `freeze()` 原子替换内部 Map 引用，返回持有旧 Map 与 sequence 边界的 `SkipListImmutableMemTable`。

- **SkipListImmutableMemTable**：冻结后的只读 MemTable。
  - 构造时接收 `SkipListCurMemTable` 的内部 SkipList 引用（浅拷贝，零开销）。
  - 暴露 `minSequenceId/maxSequenceId`，供后续 Flush/Sink/WAL 截断推进安全边界。
  - 维护 `AtomicLong refCount`，查询进入时 `incrementRef()`，离开时 `decrementRef()`。
  - `refCount` 归零后可安全退役（释放内存）。

**容量阈值**（来自 `PMSConfig`）：
- `memtableMaxEntries`：条目数上限，默认 1,000,000。
- `memtableMaxSizeMb`：内存占用上限，默认 256MB。
- 达到任一阈值触发 Freeze。

### 3.2 LocalStorageManager

负责 `newSST` 和 `sinkedSST` 的落盘、读取和归并。SST 采用参考 LevelDB/RocksDB Block Based Table 的自定义行存格式，文件由 Data Blocks、BloomFilter Block、Index Block、Properties Block 和 Footer 组成。详细格式、与 LevelDB 的一致点和差异点参见 [pms-core-sst-format.md](pms-core-sst-format.md)。

核心语义：
- Key 按主键序排列（与 Paimon 底层主键序编码 100% 一致，保证无需再排序即可写入 Paimon）。
- PMS V1 不采用 LevelDB `InternalKey = userKey + sequence + valueType` 设计；SST 排序 Key 只包含 user key，`sequenceId` 和 tombstone 信息放在 `Value` payload 与 `SSTMeta` 中。
- SST 查询必须能区分 miss、PUT 命中和 DELETE tombstone 命中，避免已删除数据从更老层或 Paimon 穿透中复活。
- V1 仅使用 Footer 中的全文件 CRC 校验完整性；逐 Data Block CRC 可作为后续演进。

**核心接口**：

```java
interface LocalStorageManager {
    // 将 ImmutableMemTable 刷盘为 newSST
    SSTMeta flushToSST(ImmutableMemTable memTable);

    // 读取 SST 中的指定 Key:
    // Optional.empty() = miss
    // Optional.of(Value with bytes != null) = PUT 命中
    // Optional.of(Value with bytes == null) = DELETE tombstone 命中
    Optional<Value> get(SSTMeta meta, Key key);

    // 打开 [startInclusive, endExclusive) 范围内的 SST 有序 iterator。
    // endExclusive 为 Optional.empty() 时表示扫描到文件末尾。
    SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive);

    // 多路归并合并多个 SST，保留最新 Key
    SSTMeta compactSSTs(List<SSTMeta> metas);

    // 删除指定 SST 文件
    void deleteSST(SSTMeta meta);

    // 淘汰最老的 sinkedSST（确认无引用后删除）
    Optional<SSTMeta> evictOldestSinkedSST();
}
```

**SSTMeta** 至少包含文件路径、文件大小、entryCount、minKey、maxKey、minSequenceId、maxSequenceId、createdAtMillis、状态和引用计数。SSTMeta 会写入独立 `sst-*.meta.json`，启动时再与 SST 文件 properties 交叉校验。BucketDirector 使用 `SSTMeta` 做查询剪枝、状态快照、淘汰和 WAL/Sink 边界推进。`entryCount` 表示 SST 物理 entry 数，包含 tombstone 和跨 SST 的旧版本；它不能由 `sequenceId` 范围推导，也不表示去重后的 live row 数。

SST 文件名使用 `sst-%06d.new.sst` / `sst-%06d.sinked.sst` 作为可观察标签。文件名不是可靠状态来源；启动恢复时以 SinkMeta success 信息推导真实状态，并 best-effort 修正文件名标签。启动扫描本地 SST 时，只有 `maxSequenceId <= lastFlushedSequenceId` 的 SST 会注册为有效本地文件；超过该边界的 SST 视为 orphan，不进入查询和 sink 列表，由 WAL replay 恢复对应数据。

**BloomFilter**：
- 每个 SST 文件包含基于主键的 BloomFilter。
- PUT 和 DELETE tombstone 都必须加入 BloomFilter；tombstone 命中时需要阻断更老层查询。
- 写入 SST 时构建，基于期望 FPP（False Positive Rate，默认 0.01）分配位图大小。
- 查询时先检查 BloomFilter，通过则读取 Data Block，不通过则跳过。

**SST 文件校验**：
- 打开或注册 SST 时校验 Footer 中的全文件 CRC32，覆盖 Footer 之前的全部数据。
- 校验失败 → 标记该 SST 文件为损坏，记录告警日志。损坏 SST 中的数据从其他层（更新层的 MemTable 或 Paimon 穿透）补全。
- 若本地 `flush-boundary.meta` 已经记录 `lastFlushedSequenceId > 0`，说明 SST 已经参与 WAL 恢复边界。此时启动阶段发现 SST 损坏应失败，而不是静默跳过，否则可能因为 WAL replay 跳过已 flush sequence 而丢失数据。
- V1 不做逐 Block 降级读取。

**本地 Flush 恢复边界**：
- `LocalStorageManager` 在 storage 目录维护 `flush-boundary.meta`，记录 `lastFlushedSequenceId`。
- `flushToSST` 成功写出 SST 后，BucketDirector 原子推进该边界到 `SSTMeta.maxSequenceId`。
- 重启时先加载 `sst-*.meta.json`、校验 SST 文件和该边界，再 replay WAL；`sequenceId <= lastFlushedSequenceId` 的 DATA 记录由 SST 承载，不再回放到 curMemTable。
- SST 文件、`sst-*.meta.json` 和 `flush-boundary.meta` 都必须在 rename 前 force 文件内容，并在 rename 后 force storage 目录，避免崩溃后边界可见但文件或目录项丢失。
- 该边界只表示本地 SST 已覆盖的数据范围，不表示 Paimon 已 commit；未来 WAL truncate 仍需以 sink 成功后的 `persistedSequenceId` 为准。

### 3.3 WALManager

V1 采用单盘 DATA WAL，保证数据变更的持久性和崩溃恢复能力。底层 I/O 和记录分片采用 LevelDB WAL 格式（32KB Block 对齐、CRC32C 逐 chunk 校验、FULL/FIRST/MIDDLE/LAST 分片重组），PMS 应用层 payload 只记录用户数据变更；Flush/Sink 进度通过独立 metadata 记录，详见 [pms-recovery-metadata.md](pms-recovery-metadata.md)。

> **后续演进**：双盘 WAL（主盘 + 备盘同步写、互恢复）作为后续演进方向。

DATA 记录中，Upsert 与 Delete 不占独立类型，通过 `valueLen = -1` 表示 Delete，通过非负 `valueLen` 携带 Upsert payload。这与 Paimon Deduplicate Merge Engine 语义一致（后写覆盖，最新为 DELETE 则删除全部同主键记录）。

**WAL 记录格式（双层结构）**：

```
LevelDB 传输层（由 LogWriter/LogReader 处理）：
┌──────────┬──────────┬──────────┬─────────────┐
│ CRC32C   │ Length   │ ChunkType│ Payload     │
│ (4 byte) │ (2 byte) │ (1 byte) │ (N bytes)   │
└──────────┴──────────┴──────────┴─────────────┘
Block = 32KB，Header = 7 bytes
ChunkType: FULL(1) / FIRST(2) / MIDDLE(3) / LAST(4)
CRC32C = masked CRC32C(ChunkType + Payload)
```

```
PMS 应用层 Payload（由 WALManager 序列化/反序列化）：

DATA:
┌────────────┬──────────┬──────────┬────────────┬──────────┐
│ sequenceId │ keyLen   │ key      │ valueLen   │ value    │
│ (8 byte)   │ (4 byte) │ (N byte) │ (4 byte)   │ (M byte) │
└────────────┴──────────┴──────────┴────────────┴──────────┘
  valueLen > 0  → Upsert（value 为 serialized InternalRow）
  valueLen = 0  → 保留/非法行编码（不表示 tombstone 或业务 NULL）
  valueLen = -1 → Delete（无 value 字段）
```

**接口**：

```java
interface WALManager {
    // 写入数据记录（Upsert: value 为 serialized InternalRow; Delete: value 为 null）
    long appendDataRecord(byte[] key, byte[] value);

    long lastSequenceId();

    // 恢复重放
    void replay(ReplayCallback callback);

    // 截断（清理已确认提交的旧日志）
    void truncate(long safeSequenceId);

    // 关闭（刷盘缓冲区）
    void close();
}

interface ReplayCallback {
    void onDataRecord(byte[] key, byte[] value);
    default void onDataRecord(long sequenceId, byte[] key, byte[] value) { ... }
}
```

**WAL 文件管理**：
- 按固定大小滚动（默认 256MB 一个文件）。
- 每个文件的第一条记录是文件头部，格式为 `magic(4 bytes, "PMS\0") + reserved(8 bytes) + lastSequenceId(8 bytes, 文件创建时的全局 sequence 水位)`。头部记录作为普通 WAL 记录写入（经 LevelDB 传输层封装），而非文件级独立 header。
- WALManager 扫描文件头和 DATA 记录恢复 `lastSequenceId`，新写入从 `max(sequenceId) + 1` 继续分配；即使旧 WAL 文件被截断，当前空 WAL 文件的头部也能保留 sequence 水位。

**WAL 截断策略**：
- 安全截断条件：SinkMeta 中存在已成功提交的 `persistedSequenceId`。
- 截断时删除所有 `maxSequenceId <= persistedSequenceId` 的 WAL 文件，正在写入的文件永不删除。
- 截断触发：当前在 sink success 或 recovered prepare commit 后即时触发；后续可增加 `BackgroundTaskScheduler` 定期补偿和 WAL 配额水位触发。

**WAL 恢复时校验**：
- 传输层：由 LevelDB LogReader 逐 chunk 校验 CRC32C。尾部不完整 chunk 自动截断，中间 chunk 校验失败报告损坏。
- 应用层：解析 PMS Payload 时校验 sequenceId、keyLen/valueLen 范围。
- 中间记录校验失败 → 磁盘损坏 → 报错，人工介入（V1 单盘无法从备盘恢复）。

### 3.4 SinkManager 与后续 Paimon Sink

封装 PMS 内部 sink 边界。当前实现为 `MockSinkManager`，只用于打通 BucketDirector、SST 状态转换和 WAL 记录；后续真实实现再封装 Paimon 底层 API，并严格遵循 2PC 流程。

**2PC 流程**：

```
1. 选择本地待 sink 的 newSST，形成 `SinkBatch(batchId, sstIds, minSeq, maxSeq)`
2. 将 SST 适配为有序 iterator，归并同 Key 最新记录
3. 调用 Paimon prepareCommit → CommitMessage 和 data file refs
4. 通过 `SinkMetaStore` 写入 prepare metadata，包含 batch、sstIds、sequence 范围和 prepared commit 信息
5. 调用 Paimon commit → 新 Snapshot
6. 通过 `SinkMetaStore` 写入 success metadata，包含 batch、snapshotId、persistedSequenceId 和 sstIds
7. BucketDirector 根据 SinkMeta success 将对应 SST 视为 sinked，并 best-effort rename 文件名标签
```

**接口**：

```java
interface SinkManager {
    PreparedSinkCommit prepare(SinkBatch batch);
    SinkCommitResult commit(PreparedSinkCommit prepared);
}
```

当前实现使用 `SinkCoordinator` 编排 `SinkManager` 与 `SinkMetaStore`；默认 `MockSinkManager` 可跑通 PMS 内部状态流转，真实 Paimon sink 由 `pms-sink-paimon` 注入。

**Compaction 集成**：
- 调用 Paimon 原生 `Table.compact()` 接口，不自己实现合并逻辑。
- 触发时机：Paimon L0 文件数超过阈值时，由 `BackgroundTaskScheduler` 触发。
- Compaction 是异步操作，不阻塞 Sink 路径。

**Paimon Manifest 索引**：
- 维护 Paimon 最新 Snapshot 的元信息索引，用于加速穿透查询。
- V1 采用全量加载策略。后续按需引入分区分级加载优化。
- TODO: Manifest 索引的具体结构、加载时机、内存占用量化设计待补充。

### 3.5 PMSBucketDirector

总协调器，管理数据从写入到最终落盘的完整生命周期。

详细设计参见 [pms-core-bucket-director.md](pms-core-bucket-director.md)。

**核心职责**：
- 管理 `curMemTable → ImmutableMemTable → newSST → sinkedSST` 的状态机流转。
- 编排查询路径的多层穿透。
- 协调 Freeze、Flush、Sink、Evict 各阶段。
- 向上层暴露统一的 `put` / `get` 接口。

### 3.6 Statistic

可观测性基础设施，提供低开销的指标采集与查询能力。

TODO: 详细设计待核心组件稳定后再补充。初期仅定义 `MetricsRegistry` 接口，具体指标类型和体系待定。

## 4. 流控与内存预算

### 4.1 两层模型

初期采用最简单的两层判断：

```
                ┌──────────────────────────────────┐
                │         Write Admission           │
                │                                  │
                │  ┌────────────┐  ┌────────────┐  │
Write Request ──│─►│  NORMAL    │  │ OVERLOADED │  │
                │  │  正常写入  │─►│   拒绝     │  │
                │  └────────────┘  └────────────┘  │
                └──────────────────────────────────┘
```

**判断条件**（满足任一即为 OVERLOADED）：

| 维度 | 阈值 | 默认值 |
|------|------|--------|
| Immutable MemTable 数量 | >= `PMSConfig.flowcontrolOverloadedImmutableCount` | 4 |
| 待 Sink 的 newSST 数量 | >= `PMSConfig.flowcontrolOverloadedPendingSstCount` | 16 |

**响应策略**：
- **NORMAL**：正常接受写入，RPC 返回 `OK`。
- **OVERLOADED**：快速拒绝，RPC 返回 `SERVICE_OVERLOADED`，Client 走反压重试逻辑。不阻塞写入线程。

### 4.2 实现

```java
enum WatermarkLevel { NORMAL, OVERLOADED }

class WriteAdmissionController {
    private final PMSConfig config;
    private final PMSBucketDirector bucketDirector;

    WatermarkLevel evaluate() {
        BucketStateSnapshot snapshot = bucketDirector.stateSnapshot();

        if (snapshot.immutableMemTableCount() >= config.flowcontrolOverloadedImmutableCount()) {
            return OVERLOADED;
        }
        if (snapshot.newSSTCount() >= config.flowcontrolOverloadedPendingSstCount()) {
            return OVERLOADED;
        }
        return NORMAL;
    }
}
```

### 4.3 与后台任务的联动

| 水位 | 后台任务策略 |
|------|-------------|
| NORMAL | Flush/Sink 按配置间隔正常触发 |
| OVERLOADED | Flush/Sink 立即触发，加速消化积压 |

### 4.4 后续扩展方向

当系统跑起来并有真实负载数据后，可考虑：
- 增加 YELLOW（限速）层，在 NORMAL 和 OVERLOADED 之间提供缓冲。
- 增加写入等待耗时维度。
- 内存使用率维度。

## 5. 并发模型与数据完整性

### 5.1 核心原则

- **短写入临界区**：WAL 写入、sequence 分配和 MemTable 可见顺序必须保持一致；flush/sink/compaction 等慢路径不得持有写入临界区。
- **Volatile 引用切换**：状态变更通过 volatile 引用的原子替换实现，而非就地修改。
- **文件延迟删除**：被淘汰的 SST 文件不立即删除，确认无查询引用后才删除。
- **初期简化**：查询时直接读 volatile 引用遍历，不做快照拷贝。引用计数仅在 Evict 删除文件时检查。

### 5.2 关键场景的并发控制

**并发写入 curMemTable**：底层 `ConcurrentSkipListMap` 本身线程安全；但写入提交顺序由 WALManager 分配的 `sequenceId` 确定，调用方必须保证 WAL record 与 MemTable value 使用同一个 sequence。

**Freeze（curMemTable → ImmutableMemTable）**：
- `curMemTable` 字段用 `volatile` 修饰。
- `freeze()` 时先构造新的空 `CurMemTable`，再原子替换引用。
- 不需要 Copy-on-Write：Freeze 后原 MemTable 天然变为只读。

**查询穿透过程中的并发**：
- 初期方案：直接遍历 volatile 列表，不做快照拷贝。
- 层列表通过 volatile 引用替换整个列表（不是就地修改），查询线程看到的要么是旧列表要么是新列表，不会看到半更新状态。
- 最坏情况：查 curMemTable 时数据刚被 Freeze，查 immutableList 时列表已更新包含了该 MemTable，结果正确。
- 后续扩展：如果查询一致性要求更严格，可引入快照读 + 防御性拷贝 + 引用计数。

**Sink 过程中查询正在归并的 SST**：
- 归并操作不影响查询——查询读的是归并前的 SST 文件，文件内容不变。
- 归并完成后，BucketDirector 原子替换列表（newSST → sinkedSST）。

**Evict 删除磁盘文件**：
- 淘汰 sinkedSST 时，确认文件不被任何查询正在读取。
- V1 简化方案：Evict 时检查 `refCount()`，如果 > 0 则跳过本次删除（下次 Evict 检查时再试）。

### 5.3 内存可见性总结

| 变量 | 类型 | 写入方 | 读取方 | 可见性保证 |
|------|------|--------|--------|-----------|
| `curMemTable` | volatile 引用 | Freeze 线程 | 写入线程 / 查询线程 | volatile 读写 |
| `newImmutableList` | volatile 引用 | Freeze 线程 | 查询线程 | volatile 读写 |
| `newSSTList` | volatile 引用 | Flush/Sink 线程 | 查询线程 | volatile 读写 |
| `sinkedSSTList` | volatile 引用 | Sink/Evict 线程 | 查询线程 | volatile 读写 |
| SkipList 内部 | ConcurrentSkipListMap | 写入线程 | 查询线程 | ConcurrentMap 内部保证 |
| `refCount` | AtomicLong | Evict 线程 | Evict 线程 | Atomic 操作 |

### 5.4 崩溃恢复流程

```
RecoveryManager 启动
        │
        ▼
1. 加载本地 SST 和 flush boundary
        │
        ▼
2. 初始化 WALManager，扫描 WAL 文件
   获取 WAL 中记录的最高 sequenceId
        │
        ▼
3. 重放 DATA 记录，恢复 curMemTable
   - 本地 SST 已覆盖的数据由 lastFlushedSequenceId 跳过
   - 扫描 DATA 记录中的 sequenceId，恢复 lastSequenceId，保证后续写入继续递增
   - 传输层由 LevelDB LogReader 逐 chunk 校验 CRC32C
   - 应用层解析 PMS Payload 时校验 sequenceId、keyLen/valueLen 合法性
        │
        ▼
4. 加载 SinkMeta，检查是否存在 prepare 但无 success
   ┌───────────────────────────────────────────────────────┐
   │ 有 prepare，无 success                                │
   │ → 使用 SinkMeta 中保存的 prepared payload 和 fileRefs  │
   │   校验 Paimon data files 后重试 commit                 │
   ├───────────────────────────────────────────────────────┤
   │ 有 success                                            │
   │ → 使用 success.sstIds 推导 sinkedSST                   │
   └───────────────────────────────────────────────────────┘
        │
        ▼
5. 根据 SinkMeta success 修正 SST 状态和文件名标签
        │
        ▼
6. 恢复完毕，启动 RPC 和后台任务
```

## 6. 核心状态机流转

详见 [pms-core-bucket-director.md](pms-core-bucket-director.md) § 3。

```
curMemTable ──freeze──► ImmutableMemTable (new)
                          │
                        flush
                          ▼
                      newSSTWithMem
                          │
                        sink
                          ▼
                     sinkedSSTWithMem
                          │
                      mem退役
                          ▼
                       sinkedSST
                          │
                       evict
                          ▼
                       [删除]
```

> 注：newSSTWithMem 和 sinkedSSTWithMem 中的 Mem 缓存是纯性能优化，可在内存不足时随时退化为不带 Mem 的状态（newSST / sinkedSST），详见 [pms-core-bucket-director.md](pms-core-bucket-director.md)。
