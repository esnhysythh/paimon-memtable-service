# PMS Core Engine 设计文档

## 1. 模块定位
单机 LSM 缓冲引擎，完全不依赖任何 RPC 框架、Web 容器或外部配置中心。`pms-core` 负责数据的内存管理、本地持久化、WAL 协调、SST 生命周期和内部 sink 状态机边界。

`pms-core` 的运行语义服务于 Paimon，但接口保持 byte-oriented：`byte[] key`、`byte[] value` 和 `delete(key)`。Paimon `InternalRow`、`RowType`、字段投影和主键编码由独立的 `pms-codec` 模块负责，上层组合模块把 codec 输出接入 `pms-core`。

## 2. 配置契约

core 组件通过构造函数接收纯 Java Record，不读取文件、环境变量或全局配置。生产配置由 `pms-server` 的 `ConfigManager` 解析并一次性注入；V1 不支持热更新。

配置所有权按“谁做决策，谁拥有配置”划分：

- core 拥有 MemTable 容量、WAL、本地 storage 路径、flow-control 和 Paimon 连接等操作所需配置。
- server 拥有 Paimon 可见性目标、NEW/SINKED 数量水位、worker 周期以及单次 Sink/Compact 字节上限。
- Freeze/Flush/Sink/Compact/Evict API 不读取调度阈值，只执行调用方明确请求的一步。

生产路径支持的 core 配置如下；完整 server/scheduler 配置见 [pms-server.md](pms-server.md) § 3。

| 键名 | 默认值 | 用途 |
|------|--------|------|
| `pms.memtable.max_entries` | `1_000_000` | CurMemTable 自动 Freeze 条目水位 |
| `pms.memtable.max_size_mb` | `256` | CurMemTable 自动 Freeze 字节水位 |
| `pms.wal.dir` | 必填 | WAL 目录 |
| `pms.wal.file_size_mb` | `256` | WAL segment 大小 |
| `pms.wal.use_mmap` | `false` | WAL mmap writer 开关 |
| `pms.storage.dir` | 必填 | SST、flush boundary 与 SinkMeta 本地目录 |
| `pms.flowcontrol.overloaded_immutable_count` | `4` | Immutable backlog 写入拒绝水位 |
| `pms.flowcontrol.overloaded_pending_sst_count` | `20` | NEW backlog 写入拒绝水位 |
| `pms.paimon.table_path` | 必填/由 server 补全 | core 表标识 |
| `pms.paimon.warehouse` | 必填 | Paimon warehouse |
| `pms.paimon.cache_enabled` | `true` | Paimon catalog cache |
| `pms.paimon.manifest_cache_small_file_memory` | `128mb` | manifest small-file cache |
| `pms.paimon.manifest_cache_small_file_threshold` | `1mb` | small manifest 判定阈值 |
| `pms.paimon.manifest_cache_max_memory` | 未设置 | 可选 manifest 总缓存预算 |

当前 Java `StorageConfig` / `SinkConfig` 类型仍包含早期调度字段，但生产 `ConfigManager` 明确拒绝相应旧键，scheduler 也不读取这些字段。它们不是受支持的产品配置；后续可随 core 配置类型清理移除，不能据此恢复旧的定时 Sink、总大小淘汰或 `compactMinFiles` 行为。

`pms.wal.dir`、`pms.storage.dir` 和 lookup cache 必须隔离，避免删除/恢复协议互相影响。

## 3. 核心组件与接口定义

**数据语义约束**：PMS 内部和 Sink 到 Paimon 的数据处理均遵循 Paimon Deduplicate Merge Engine 规则——同一主键只保留最新记录，最新记录为 DELETE 则删除全部同主键记录。不允许其他 Merge Engine。这保证了 PMS 内部数据处理和查询的简单性：同一 Key 取最新值即可，归并时最新 Key 胜出，无需特殊合并函数。

**轻量级 sequence 边界**：PMS 为每条成功进入 WAL 的数据写入分配单调递增的 `sequenceId`。V1 中 sequence 只用于确定内部处理边界，不提供 MVCC 快照读；同一 Key 在 MemTable 中仍只保留 latest value。sequence 必须随 WAL 持久化，恢复时从 WAL 中的最大 sequence 继续递增。

sequence 的边界语义：
- `Value/Entry` 携带 latest `sequenceId`，作为该 Key 最新写入的内部顺序。
- `CurMemTable.freeze()` 产生的 `ImmutableMemTable` 记录 `minSequenceId/maxSequenceId`。
- 后续 Flush/SST/Sink 元数据也必须记录覆盖的 sequence 范围。
- Paimon `snapshotId` 表示外部提交结果；WAL 安全截断以 SinkMeta 中的 PMS 内部 `persistedSequenceId` 为主边界。
- V1 不保存同 Key 多版本；未来如果要支持 MVCC，可将 MemTable/SST key 形态升级为 `(userKey, sequenceId)` 并引入 read sequence 可见性过滤。

**Value 编码语义**：PMS 不是通用 KV 存储，MemTable 中的 `Value.bytes` 不是任意用户字节值，而是一条 Paimon `InternalRow` 的序列化结果。非删除记录必须由 `pms-codec` 生成，代表完整的行编码。即使业务列全部为 `NULL`，编码结果也应包含格式头、字段数量、null bitmap 或其他必要元信息，因此设计语义上不应为空 `byte[]`。`Value.bytes == null` 专用于 tombstone/delete，不表示业务层 NULL。

RowCodec 不在 value 内部表达 delete。进入 `pms-core` 前，调用方必须将 `INSERT/UPDATE_AFTER` 归一化为 `put(key, rowValueBytes)`，将 `DELETE/UPDATE_BEFORE` 归一化为 `delete(key)`。WAL 和 SST 层继续用 `valueLen = -1` 表达 tombstone。

### 3.1 MemTableEngine

管理内存中的写缓冲，基于 SkipList 实现。接口拆分为 `CurMemTable`（可写）和 `ImmutableMemTable`（等待 Flush 的只读对象），由 `CurMemTable.freeze()` 产生 `ImmutableMemTable`。

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

- Schema 不进入 core 接口。V1 在产品与部署层保证绑定表 Schema 不变，core 不负责监控或阻止 Schema 变更。
- Key 使用无符号字节比较（与 Paimon 主键序一致），参见 [paimon-primary-key-encoding.md](../references/paimon-primary-key-encoding.md)。
- 删除不通过 `CurMemTable.delete(Key)` 表达，而是写入 `Value.tombstone(sequenceId)`。这样 tombstone 与普通 upsert 一样携带明确的 sequence 边界。

**ImmutableMemTable 接口**：

```java
interface ImmutableMemTable {
    Value get(Key key);
    Iterator<Entry> iterator();
    Iterator<Entry> iterator(Key startInclusive, Optional<Key> endExclusive);
    long estimatedSize();
    int estimatedEntryCount();
    long minSequenceId();
    long maxSequenceId();
    long oldestWriteAtMillis();
}
```

**实现**：

- **SkipListCurMemTable**：当前活跃的可写 MemTable。
  - 底层 `ConcurrentSkipListMap<Key, Value>`，线程安全。
  - 写入后检查是否达到 Freeze 阈值（`estimatedEntryCount() >= config.maxEntries()` 或 `estimatedSize() >= config.maxSizeBytes()`），在完整 batch apply 后触发 Freeze。
  - `estimatedEntryCount` 和 `estimatedSize` 均为启发式估算值，非精确计数：高并发下 `volatile int ++` 可能丢失增量，误差在可接受范围内。
  - 第一次成功写入记录 `oldestWriteAtMillis`，用于 Paimon 可见性 lag；不沿写入调用链传递 wall-clock 时间。
  - 跟踪当前 MemTable 的 `minSequenceId/maxSequenceId`，作为 freeze 后的边界元数据。
  - `freeze()` 封存当前对象并返回持有原 SkipList 与边界的 `SkipListImmutableMemTable`；BucketDirector 创建新的 CurMemTable，并通过不可变 `MemTableState` 原子发布对象切换，不原地复用旧 CurMemTable。

- **SkipListImmutableMemTable**：冻结后的只读 MemTable。
  - 构造时接收旧 CurMemTable 的内部 SkipList 引用（浅拷贝，零开销）。
  - 暴露 `minSequenceId/maxSequenceId`，供后续 Flush/Sink/WAL 截断推进安全边界。
  - 只在等待 Flush 时参与查询。Flush 将目标 SST 完整发布后，它立即退出可见状态并由 GC 回收，不作为 SST cache 保留。

**容量阈值**（来自 `PMSConfig`）：
- `memtableMaxEntries`：条目数上限，默认 1,000,000。
- `memtableMaxSizeMb`：内存占用上限，默认 256MB。
- 达到任一阈值触发 Freeze。

### 3.2 LocalStorageManager

负责 `newSST` 和 `sinkedSST` 的落盘、读取和归并。SST 采用参考 LevelDB/RocksDB Block Based Table 的自定义行存格式，文件由 Data Blocks、BloomFilter Block、Index Block、Properties Block 和 Footer 组成。详细格式、与 LevelDB 的一致点和差异点参见 [pms-core-sst-format.md](pms-core-sst-format.md)。

核心语义：
- Key 按主键序排列（与 Paimon 底层主键序编码 100% 一致，保证无需再排序即可写入 Paimon）。
- PMS V1 不采用 LevelDB `InternalKey = userKey + sequence + valueType` 设计；SST 排序 Key 只包含 user key，`sequenceId` 和 tombstone 信息放在 `Value` payload 与 `SSTMeta` 中。
- SST 查询必须能区分 miss、PUT 命中和 DELETE tombstone 命中，避免已删除数据从更老层或 Paimon 历史数据中复活。
- V1 仅使用 Footer 中的全文件 CRC 校验完整性；逐 Data Block CRC 可作为后续演进。

**核心接口**：

```java
interface LocalStorageManager {
    // 将 ImmutableMemTable 刷盘为 newSST
    SSTMeta flushToSST(ImmutableMemTable memTable);

    // 获取 SST 读快照。点查、scan 和 sink 必须通过返回的 snapshot 读取 SST。
    // snapshot 注册 read epoch, 保护调用方已经选中的 SST 列表不被物理删除。
    SSTReadSnapshot readSnapshot(List<SSTMeta> metas);

    // 原子捕获当前全部可见 SST，并进入对应 read epoch。lookup/scan 使用此入口。
    SSTReadSnapshot readVisibleSnapshot();

    // 多路归并合并多个 SST，保留最新 Key
    SSTMeta compactSSTs(List<SSTMeta> metas);

    // 从可见集合退役指定 SST；活跃 read epoch 结束后物理删除。
    void deleteSST(SSTMeta meta);
}
```

`SSTReadSnapshot` 是 SST 查询读取的唯一外部入口：

```java
interface SSTReadSnapshot extends AutoCloseable {
    List<SSTMeta> metas();
    Optional<Value> get(SSTMeta meta, Key key);
    SSTEntryIterator openIterator(SSTMeta meta);
    SSTEntryIterator openIterator(SSTMeta meta, Key startInclusive, Optional<Key> endExclusive);
}
```

`SSTReadSnapshot` 的语义是 read epoch, 不是文件级 reader lease。创建 snapshot 时记录当前 SST 可见集合 epoch, 删除和 compact 会先从可见集合移除旧 SST, 推进 epoch, 再把旧 SST 放入 retired queue。只有当所有活跃 snapshot 的最小 epoch 已经不早于某个 retired SST 的 `retireEpoch` 时, storage 才会关闭对应 reader并按 `data -> meta` 的顺序删除文件。retired entry 只有在两次删除都成功后才能移除；删除失败保留 entry，后续 reclaim 幂等重试。这样点查仍可按 SST 顺序按需读取并在命中后停止, 不需要提前获取列表中所有 SST 的 reader lease。

**SSTMeta** 至少包含 `runId`、`minFlushId/maxFlushId`、文件路径、文件大小、entryCount、minKey、maxKey、minSequenceId、maxSequenceId、`oldestWriteAtMillis`、`createdAtMillis` 和状态。SSTMeta 会写入独立 `sst-*.meta.json`，启动时再与 SST 文件 properties 交叉校验。BucketDirector 使用 `SSTMeta` 做查询剪枝、状态快照、淘汰和 WAL/Sink 边界推进。`entryCount` 表示 SST 物理 entry 数，包含 tombstone 和跨 SST 的旧版本；它不能由 `sequenceId` 范围推导，也不表示去重后的 live row 数。

SST 数据文件 publish 后不再 rename, 文件名使用 `sst-%06d-%06d.sst` 表达稳定的 `minFlushId/maxFlushId` 范围。`NEW` / `SINKED` 状态写入 `sst-*.meta.json` 供观测，但启动恢复不信任旧 state，而是使用 SinkMeta success 的 `lastPersistedSequenceId` 重新推导：`maxSequenceId <= boundary` 为 SINKED，`minSequenceId > boundary` 为 NEW，跨越 boundary 则拒绝启动。历史 `sstIds` 只服务 exact Sink finalization，不再作为长期 run 状态判定依据。

启动扫描由 `SSTRecoveryPlanner` 集中完成。它对完整 data/meta pair 使用单 candidate 贪心选择范围最大的 compact 输出，最终可见 run 必须形成一个 flushId 连续的本地后缀；内部缺口、部分重叠、sequence/state 交叉和无法解释的损坏均拒绝启动。只自动处理三类正常崩溃残留：

- compact 输出完整覆盖的旧输入文件；
- `maxSequenceId <= lastPersistedSequenceId` 且位于可见后缀之前的 meta-only 最老 evict 残留；
- `minSequenceId > lastFlushedSequenceId` 的单 flush orphan。

orphan 不进入垃圾清理队列，也不推进 `nextFlushId`；WAL replay 后的下一次 Flush 复用并覆盖该 flushId。运行期若 SST 已发布但 flush boundary 写入失败，Director 通过非持久化 FlushFlight 复用同一个 SST 完成 handoff。`flushToSST` 自身也只在完整发布后推进 allocator，因此写 SST 或 meta 失败同样不会消费 flushId。

**BloomFilter**：
- 每个 SST 文件包含基于主键的 BloomFilter。
- PUT 和 DELETE tombstone 都必须加入 BloomFilter；tombstone 命中时需要阻断更老层查询。
- 写入 SST 时构建，基于期望 FPP（False Positive Rate，默认 0.01）分配位图大小。
- 查询时先检查 BloomFilter，通过则读取 Data Block，不通过则跳过。

**SST 文件校验**：
- 打开或注册 SST 时校验 Footer 中的全文件 CRC32，覆盖 Footer 之前的全部数据。
- 校验失败后由启动恢复规划判断：只有被完整 compact 输出覆盖的输入残留或尚未推进 flush boundary 的 orphan 可以忽略；其他损坏拒绝启动。
- 若损坏位于最终保留的连续 SST 后缀中，不能静默跳过，否则可能因为 WAL replay 跳过已 flush sequence 而丢失数据。V1 不为人工删除、磁盘损坏等低概率内部缺口设计推断恢复。
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
- 截断触发：当前在 sink success 或 recovered prepare commit 后即时触发；后续可增加 server scheduler 定期补偿和 WAL 配额水位触发。

**WAL 恢复时校验**：
- 传输层：由 LevelDB LogReader 逐 chunk 校验 CRC32C。尾部不完整 chunk 自动截断，中间 chunk 校验失败报告损坏。
- 应用层：解析 PMS Payload 时校验 sequenceId、keyLen/valueLen 范围。
- 中间记录校验失败 → 磁盘损坏 → 报错，人工介入（V1 单盘无法从备盘恢复）。

### 3.4 SinkManager 与 Paimon Sink

`SinkManager` 是 core 的 Paimon 写入 SPI。生产环境由 `pms-sink-paimon` 注入真实实现；core 测试可注入 fake/mock。BucketDirector 与 `SinkCoordinator` 负责可靠的 2PC metadata 顺序，不依赖具体 Paimon row 类型。

**2PC 流程**：

```
1. 选择本地待 sink 的 newSST，形成 `SinkBatch(batchId, sstIds, minSeq, maxSeq)`
2. 将 SST 适配为有序 iterator，归并同 Key 最新记录
3. 调用 Paimon prepareCommit → CommitMessage 和 data file refs
4. 通过 `SinkMetaStore` 写入 prepare metadata，包含 batch、sstIds、sequence 范围和 prepared commit 信息
5. 调用 Paimon commit → 新 Snapshot
6. 通过 `SinkMetaStore` 写入 success metadata，包含 batch、snapshotId、persistedSequenceId 和 sstIds
7. BucketDirector 根据 SinkMeta success 将对应 SST 视为 sinked，并更新 SST metadata
```

**接口**：

```java
interface SinkManager {
    PreparedSinkCommit prepare(SinkBatch batch);
    SinkCommitResult commit(PreparedSinkCommit prepared);
}
```

当前实现使用 `SinkCoordinator` 编排 `SinkManager` 与 `SinkMetaStore`。一次操作只 Sink `SinkSelection` 选出的最老连续 NEW 前缀；单批受输入字节数约束，但不受 SST 个数限制。`resumeSinkFlight()` 统一处理两种在线恢复：`PREPARED_RETRY` 复用 durable prepare 重试原 commit；`FINALIZING` 从 exact batch success metadata 读取原 commit result，只幂等重做本地 NEW → SINKED 和 boundary 收尾，不再次创建 Paimon commit。两条路径都返回原 commit result 供 server 发布 lookup delta。WAL truncate 作为 success 后的可重试空间回收，不阻塞逻辑 Sink 完成。

**Compaction 集成**：
- PMS 不自己实现 Paimon 文件合并算法；由 `pms-sink-paimon` 调用 Paimon 原生 `TableWrite.compact(partition, bucket, fullCompaction)`，再通过 `prepareCommit(waitCompaction=true, commitIdentifier)` 和 `TableCommit` 提交 compact 结果。
- Paimon `Table` 本身没有面向 Java Program API 的 `compact()` 入口，compact 是 partition/bucket 级的 write operation。该决策来自 Paimon 1.4.x 源码：`TableWrite` 暴露 `compact(...)`，`StreamTableWrite` 暴露带 `commitIdentifier` 的 `prepareCommit(...)`。
- 为了让 PMS 掌控 snapshot 生成，写入 sink 与显式 compaction 应拆成两个 Paimon table view：写入路径使用 `table.copy(Map.of("write-only", "true"))` 关闭写入端隐式 compaction；compaction 路径使用 `table.copy(Map.of("write-only", "false"))` 执行显式 compact。这样 PMS 的普通 sink 只产生数据写入 snapshot，Paimon compact snapshot 只由 PMS compaction scheduler 产生。
- Compaction 不改变 PMS 本地 SST/WAL 边界，也不推进 `persistedSequenceId`；它只改变 Paimon manifest 中的数据文件布局。因此恢复 metadata 不应复用 `SinkMeta` 的 `sstIds/persistedSequenceId` 语义，而应由 `pms-sink-paimon` 或 `pms-server` 维护独立的 prepared/success compaction metadata。
- 触发时机：未来由 server scheduler 枚举 Paimon partition/bucket 候选，按文件数、L0/level 分布、距上次 compact 时间或手动 full compact 请求触发；当前 MVP 暂不实现显式 Paimon compaction scheduler。
- Compaction 与 Sink 在 PMS 内串行提交 Paimon snapshot，避免同一 PMS 进程内的 commit identifier 顺序和 Paimon manifest commit 竞争复杂化；实际文件 rewrite 可在 Paimon compact executor 中异步执行，但提交阶段必须纳入 PMS recovery。

**Paimon 历史点查**：
- `pms-core` 不维护 Paimon manifest 或 data-file 索引；它只暴露本地三态 `lookup`。Paimon 历史点查由 `pms-server` 调用 `pms-lookup-paimon` 完成，因此 core 不引入 Paimon API 依赖。
- `pms-lookup-paimon` 维护 partition-bucket `DataFileMeta` live view；普通 sink 与 compaction 的成功提交后由 server 发布严格有序 delta，重启或失效后从完整 snapshot 重建。
- `ReadBuilder` 不属于生产查询路径，只作为测试正确性对照。lookup 无法确定结果时由 server 返回可重试错误，不能返回 miss。详见 [pms-lookup-paimon.md](pms-lookup-paimon.md)。

### 3.5 PMSBucketDirector

总协调器，管理数据从写入到最终落盘的完整生命周期。

详细设计参见 [pms-core-bucket-director.md](pms-core-bucket-director.md)。

**核心职责**：
- 管理 `curMemTable → ImmutableMemTable → newSST → sinkedSST` 的状态机流转。
- 编排查询路径的多层穿透。
- 协调 Freeze、Flush、Sink、Compact、Evict 各阶段。
- 向上层暴露统一的 byte-oriented `put` / `delete` / `writeBatch` / `get` 接口；`writeBatch` 是写入提交边界，单条写入是 size=1 batch 的便捷入口。

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
| 待 Sink 的 newSST 数量 | >= `PMSConfig.flowcontrolOverloadedPendingSstCount` | 20 |

**响应策略**：
- **NORMAL**：正常接受写入，RPC 返回 `OK`。
- **OVERLOADED**：快速拒绝，RPC 返回 `OVERLOADED`，Client 走反压重试逻辑。不阻塞写入线程。

### 4.2 实现

`pms-core` 通过 `BucketStateSnapshot` 暴露水位所需状态，保持 byte-oriented 存储边界；
`pms-server` 在所有写入口进入 core 前读取 snapshot 并执行快速 admission：

- `immutableMemTableCount >= overloadedImmutableCount` 时拒绝。
- `newSSTCount >= overloadedPendingSstCount` 时拒绝。
- binary API 返回 `OVERLOADED`，因此 client 可以安全重试。

为覆盖多个并发请求已经通过 server 快照检查、随后在 core 排队的窗口，写 leader 在现有
`writeMutex` 内、每个内部合并 batch 写 WAL 前再次以 O(1) 方式读取 immutable / NEW count。
如果此时已经达到水位，core 抛出 `PmsWriteOverloadedException`，整个 batch 不进入 WAL、
不分配 sequence，并由 server 映射为同一个 `OVERLOADED`。

两次检查都不做容量预留，也不承诺 count 绝不瞬时越界；一个已经接受的完整 batch 可以触发
Freeze 并到达水位，后台 Flush 也可以并发发布新的 NEW run。该机制的目标是及时停止继续扩大
积压，而不是提供精确的全局内存配额或给写入热路径增加 SST maintenance 锁。

### 4.3 与后台任务的联动

| 水位 | 后台任务策略 |
|------|-------------|
| NORMAL | Flush / Maintenance worker 按各自周期正常调和 |
| OVERLOADED | 拒绝新写入；两个 worker 继续按周期和既有 signal 消化积压，不在写线程同步维护 |

### 4.4 后续扩展方向

当系统跑起来并有真实负载数据后，可考虑：
- 增加 YELLOW（限速）层，在 NORMAL 和 OVERLOADED 之间提供缓冲。
- 增加写入等待耗时维度。
- 内存使用率维度。

## 5. 并发模型与数据完整性

### 5.1 核心原则

- **写入短临界区**：`writeMutex` 只保护 writer queue、WAL/sequence/MemTable 提交顺序、Freeze 对象切换与写入前 backlog 复查。
- **慢 IO 不持写锁**：Flush、Sink、local compact、Evict 和查询不获取 `writeMutex`。
- **不可变状态发布**：Cur/Immutable 通过 `MemTableState`，NEW/SINKED 通过 `RunState` 整体替换；调用方不会看到半更新列表。
- **SST 生命周期由 read epoch 保护**：查询、scan、sink 与 compact 先获取 `SSTReadSnapshot`。被替换或淘汰的文件进入 retired queue，最后一个可能看到它的 epoch 结束后才物理删除。
- **SST maintenance 串行化**：`sstMaintenanceMutex` 只串行化 Sink、prepared retry、compact 与 evict，防止它们选择并发布互相冲突的 run 集合；新写入和 Flush 不因此停顿。

### 5.2 关键场景的并发控制

**并发写入 curMemTable**：底层 `ConcurrentSkipListMap` 本身线程安全；但写入提交顺序由 WALManager 分配的 `sequenceId` 确定，调用方必须保证 WAL record 与 MemTable value 使用同一个 sequence。

**Freeze（curMemTable → ImmutableMemTable）**：
- Freeze 与写入提交使用同一个 `writeMutex`，因此旧对象不会在边界发布后继续接受写入。
- 创建新的 CurMemTable，将旧对象封存为 immutable，再通过一个新的 `MemTableState` 原子发布 current + immutable 列表。
- 查询可能看到切换前或切换后的完整视图；两者都能找到该数据，不需要查询获取写锁。

**查询穿透过程中的并发**：
- MemTable 查询使用一次获取的不可变 `MemTableState`。
- 点查与 scan 使用 `readVisibleSnapshot()` 获取原子可见集合与 read epoch；Sink/compact 对明确选择的文件集合使用 `readSnapshot(...)`。
- 已成功返回的写入对之后开始的 lookup 可见；真正并发的 lookup 可观察写前或写后状态。
- V1 scan 为 weakly consistent，不承诺 batch-atomic MVCC snapshot。

**Flush/Sink/Compact 的层级迁移**：
- 目标层完整落盘后才发布，源层在同一次状态切换中或其后移除。
- 查询看到旧源、旧源 + 新目标或新目标均可得到相同最新值，不允许出现两边都不可见的窗口。
- Sink 只改变 NEW/SINKED 生命周期，不重写 SST data file；compact 输出完整发布后再替换输入 run。

**Evict 删除磁盘文件**：
- 只从可见集合移除最老 SINKED run。
- storage 记录 `retireEpoch`；活跃 read snapshot 释放后再关闭 reader。
- 物理删除按 data、meta 顺序推进；任一步失败都保留 retired entry 供下一次 reclaim 重试。删除属于可重建本地 cache 的清理路径，V1 不为 unlink 增加 directory fsync；极端掉电造成目录项持久化乱序时允许恢复校验 fatal。
- 若进程在 data 删除成功、meta 删除前崩溃，启动恢复把位于可见后缀之前且已由 `lastPersistedSequenceId` 覆盖的 meta-only run 识别为最老 evict 残留，继续清理而不注册为可见 SST。

### 5.3 内存可见性总结

| 变量 | 类型 | 写入方 | 读取方 | 可见性保证 |
|------|------|--------|--------|-----------|
| `memTables` | volatile `MemTableState` | 写入/Freeze/Flush | 写入线程 / 查询线程 / state snapshot | 整体不可变发布 |
| `runState` | volatile `RunState` | Flush/Sink/Compact/Evict | maintenance | 整体不可变发布 |
| SkipList 内部 | ConcurrentSkipListMap | 写入线程 | 查询线程 | ConcurrentMap 内部保证 |
| storage visible metas | storage-owned snapshot | Flush/Compact/Evict | 查询 / scan / sink / state snapshot | read epoch + retired queue |
| `sinkFlight` | volatile immutable snapshot | Sink/recovery | scheduler / state snapshot | 整体替换 |

### 5.4 崩溃恢复流程

```
RecoveryManager 启动
        │
        ▼
1. 加载 SinkMeta success，取得 lastPersistedSequenceId
        │
        ▼
2. 加载 flush boundary，并规划本地 SST
   - 从完整 data/meta pair 贪心选择覆盖范围最大的 compact 输出
   - 最终可见 run 必须构成 flushId 连续后缀
   - 识别 compact/oldest-evict 残留与 flush-boundary orphan
   - 按 lastPersistedSequenceId 推导 NEW/SINKED
        │
        ▼
3. 初始化 WALManager，扫描 WAL 文件
   获取 WAL 中记录的最高 sequenceId
        │
        ▼
4. 重放 DATA 记录，恢复 curMemTable
   - 本地 SST 已覆盖的数据由 lastFlushedSequenceId 跳过
   - 扫描 DATA 记录中的 sequenceId，恢复 lastSequenceId，保证后续写入继续递增
   - 传输层由 LevelDB LogReader 逐 chunk 校验 CRC32C
   - 应用层解析 PMS Payload 时校验 sequenceId、keyLen/valueLen 合法性
        │
        ▼
5. 检查是否存在 prepare 但无 success
   ┌───────────────────────────────────────────────────────┐
   │ 有 prepare，无 success                                │
   │ → 使用 SinkMeta 中保存的 prepared payload 和 fileRefs  │
   │   校验 Paimon data files 后重试 commit                 │
   ├───────────────────────────────────────────────────────┤
   │ 有 success                                            │
   │ → 使用 success.persistedSequenceId 推导 NEW/SINKED     │
   └───────────────────────────────────────────────────────┘
        │
        ▼
6. recovered prepare commit 推进 boundary 后，再次修正 SST 状态并截断 WAL
        │
        ▼
7. 恢复完毕，启动 RPC 和后台任务
```

## 6. 核心状态机流转

详见 [pms-core-bucket-director.md](pms-core-bucket-director.md) § 3。

```
curMemTable ──freeze──► ImmutableMemTable (new)
                          │
                        flush
                          ▼
                         newSST
                          │
                        sink
                          ▼
                       sinkedSST
                          │
                       evict
                          ▼
                      Paimon only
```

ImmutableMemTable 仅在等待 Flush 时参与查询；NEW SST 发布后源 MemTable 退出可见状态。NEW 与 SINKED 分别保留在独立的 local run 集合中，只允许同状态 compact。Sink 与 compact 正交：NEW Sink 后成为 SINKED，仍可继续与相邻 SINKED run 合并。
