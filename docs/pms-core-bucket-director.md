# PMS Bucket Director 设计文档

## 1. 模块定位
`PMSBucketDirector` 是 `pms-core` 的总协调器，管理一条 Paimon 表数据从写入到最终落盘的完整生命周期。它不直接持有数据，而是协调 MemTableEngine、LocalStorageManager、WALManager、SinkManager 四个子组件的协作。当前 `SinkManager` 使用 mock 实现打通状态流转，后续替换为真实 Paimon sink 实现。

## 2. 核心职责
- 管理数据单元的状态机流转。
- 编排查询路径的多层穿透。
- 协调写入、冻结、刷盘、Sink、淘汰各阶段的前后依赖。
- 向上层（pms-server RPC）暴露统一的 `put` / `get` 接口。

## 3. 数据单元状态机

PMS 中的数据单元经历以下状态流转：

```
                    ┌─────────────┐
                    │   WRITE     │
                    └──────┬──────┘
                           ▼
               ┌───────────────────────┐
               │     curMemTable       │ ◄── get() 查询命中
               │     (Active/Writable) │
               └───────────┬───────────┘
                  freeze() │ (curMemTable 达阈值 或 手动触发)
                           ▼
          ┌────────────────────────────────┐
          │   ImmutableMemTable (new)      │ ◄── get() 查询命中
          │   等待 Flush                   │
          └────────────┬───────────────────┘
                  flush() │ (异步刷盘完成)
                          ▼
          ┌────────────────────────────────┐
          │   newSST + ImmutableMemTable   │ ◄── get() 查询命中
          │   (newSSTWithMem)             │     (SST + Mem 双重保证)
          └────────────┬───────────────────┘
                  sink() │ (触发 Sink Paimon)
                         ▼
          ┌────────────────────────────────┐
          │   sinkedSST + ImmutableMemTable│ ◄── get() 查询命中
          │   (sinkedSSTWithMem)          │
          └────────────┬───────────────────┘
                mem退役 │ (ImmutableMemTable 引用计数归零 或 内存不足主动退化)
                         ▼
          ┌────────────────────────────────┐
          │   sinkedSST                    │ ◄── get() 查询命中
          │   等待淘汰                     │
          └────────────┬───────────────────┘
                evict() │ (最老优先淘汰)
                         ▼
                      [删除]
```

### 3.1 状态说明

| 状态 | 内存数据 | 磁盘数据 | 可查询 | 可写入 | 说明 |
|------|---------|---------|--------|--------|------|
| curMemTable | SkipList（可变） | 无 | 是 | 是 | 达到容量阈值或时间阈值触发 Freeze |
| ImmutableMemTable (new) | SkipList（只读） | 无 | 是 | 否 | Flush 完成后转为 newSSTWithMem |
| newSSTWithMem | SkipList（只读） | newSST | 是 | 否 | Sink 完成后转为 sinkedSSTWithMem |
| sinkedSSTWithMem | SkipList（只读） | sinkedSST | 是 | 否 | Mem 引用计数归零或内存不足时退化为 sinkedSST |
| sinkedSST | 无 | sinkedSST | 是 | 否 | 最老且无查询引用时淘汰 |

### 3.2 双持状态的性质

**newSSTWithMem 和 sinkedSSTWithMem 中的 Mem 缓存是纯性能优化，不是正确性要求。**

保留 Mem 缓存的原因：PMS 必须有基于时间阈值的 Sink 操作，在数据量较小时 MemTable 可能还没积攒足够数据就被 Freeze。如果每次 Flush 后立即退役 Mem，PMS 内存中几乎没数据，"内存缓存"功能会被严重削弱。双持保证内存中保留更多缓存数据。

**退化规则**——以下情况可将双持状态退化为不带 Mem 的状态：
- 内存不足，需要回收 Mem 缓存空间。
- Mem 缓存的引用计数已归零。
- 合并时内存不足以同时持有新旧 SkipList。

退化操作是单向的（有 Mem → 无 Mem），不可逆。

### 3.3 状态流转的原子性保证

每个状态流转操作必须是原子的，确保查询路径不会看到中间状态：

- **freeze**：`curMemTable` 引用用 volatile 修饰，原子切换为新的空 MemTable，原 MemTable 标记为 immutable 并加入 `newImmutableList`。冻结结果必须携带 `minSequenceId/maxSequenceId`，后续 Flush/Sink/WAL 截断以该边界推进。
- **flush**：SST 文件原子落盘成功后，先推进本地 `lastFlushedSequenceId`，再将对应 ImmutableMemTable 从 `newImmutableList` 移至 `newSSTWithMemList`，同时注册 newSST 的 BloomFilter。若边界推进失败，ImmutableMemTable 仍保留在内存列表中，不进入已 flush 状态。
- **sink**：Paimon prepare 成功后，`SinkMetaStore` 先将 batch、SST id 列表、sequence 范围和 prepared commit payload 写入 prepare metadata；Paimon commit 成功后，再将 batch、snapshotId、persistedSequenceId 和 SST id 列表写入 success metadata。随后内存中将对应 newSST 批量移至 sinkedSST，并更新 SST metadata 中的状态。SST 数据文件 publish 后不再 rename；是否 sinked 的可靠判断以 SinkMeta success 为准。
- **mem退役**：ImmutableMemTable 的引用计数归零后，从对应列表中移除，只保留 SST 引用。
- **evict**：淘汰最老的 sinkedSST，先从可见列表移除并放入 retired queue；待所有可能看到该 SST 的 read epoch 结束后，再物理删除 data/meta 文件。

## 4. 查询穿透

`lookup(key)` 按以下顺序穿透，命中即返回三态 `Optional<Value>`；`get(key)` 是面向旧调用方的便捷包装，会把 tombstone 转换为 `Optional.empty()`：

```
1. curMemTable                    ── 命中概率最高，延迟最低
2. newImmutableMemTables (倒序)   ── 新数据优先
3. sinkedImmutableMemTables (倒序)
4. newSSTs (BloomFilter 加速)     ── 磁盘读取
5. sinkedSSTs (BloomFilter 加速)  ── 磁盘读取
6. Paimon 穿透查询                ── 最慢，由 pms-server 在本地 miss 后发起
```

各层内部读取统一使用 `Value` 语义表达三态：

| 层级结果 | 含义 | BucketDirector 动作 |
|----------|------|---------------------|
| `null` 或 `Optional.empty()` | miss | 继续查下一层 |
| `Value.bytes() != null` | PUT 命中 | 返回该 value bytes |
| `Value.bytes() == null` | DELETE tombstone 命中 | 停止穿透，返回 `Optional.empty()` |

因此 SST 点查接口必须返回 `Optional<Value>`，不能返回 `Optional<byte[]>`。`Optional<byte[]>` 无法区分 miss 与 tombstone，会导致已删除数据从更老层或 Paimon 穿透中复活。当前实现中，SST 点查通过 `LocalStorageManager.readSnapshot(...)` 返回的 `SSTReadSnapshot.get(...)` 执行。

V1 实现决策：`pms-core` 的 `lookup(key)` 只负责本地层三态判断，不直接依赖 Paimon API；`pms-server` 调用 `lookup(key)` 后，若本地返回 PUT 或 tombstone 则停止，只有本地完全 miss 时才通过 Paimon `ReadBuilder` 主键等值过滤执行穿透点查。这样既保持 core 的 byte-oriented 边界，也保证 tombstone 能阻断 Paimon 旧值复活。

### 4.1 Range / Prefix Scan

`scan(startInclusive, endExclusive)` 在 core 层提供 byte-oriented 范围扫描能力，`prefixScan(prefix)` 通过 `PrefixNext(prefix)` 转换为 `[prefix, prefixNext)` 范围。`pms-core` 不理解 Paimon row/schema，只按 `Key` 的 unsigned lexicographical order 执行范围扫描；上层可使用 `PmsPrimaryKeyCodec.encodePrefix...` 构造主键前缀。

范围扫描覆盖以下本地层：

```text
curMemTable
immutableMemTables
newSSTs
sinkedSSTs
```

合并规则：

- 每层输出 `[start, end)` 内的有序 `Entry`。
- BucketDirector 按 key 聚合，并以 `Value.sequenceId` 最大的 entry 作为最新版本。
- 最新版本为 tombstone 时，该 key 不返回给调用方，同时阻止更老层数据复活。
- 返回结果按 key 升序排列。

该设计理由是：点查可以利用层级新旧顺序命中即返回，但 range/prefix scan 必须同时观察所有层，否则无法正确处理不同 key 在不同层上的最新 sequence，也无法在 tombstone 覆盖旧 SST/Paimon 数据时维持 Deduplicate 语义。

**查询穿透过程中的并发**：MemTable 层仍直接读取当前 volatile 引用；进入 SST 层前，BucketDirector 会在短临界区内读取当前 SST 列表并创建 `SSTReadSnapshot`。snapshot 注册 read epoch, 后续磁盘读取仍按 SST 顺序按需执行并在命中后停止。compact/evict 只会把旧 SST 放入 retired queue, 等所有活跃 snapshot 的最小 epoch 不早于旧 SST 的 `retireEpoch` 后才物理删除文件。详见 [pms-core.md](pms-core.md) § 3.2。

## 5. 接口定义

```java
interface PMSBucketDirector {

    // ── 写入 ──
    void put(byte[] key, byte[] value);
    void delete(byte[] key);  // 删除记录（写入墓碑标记）

    // ── 查询 ──
    Optional<byte[]> get(byte[] key);
    Optional<Value> lookup(byte[] key);
    List<Entry> scan(byte[] startInclusive, Optional<byte[]> endExclusive);
    List<Entry> prefixScan(byte[] prefix);

    // ── 状态流转触发 ──
    void freezeCurMemTable();
    void flushImmutableMemTable();
    void sinkToPaimon();             // 当前为 MockSinkManager，真实 Paimon sink 后续替换
    Optional<SSTMeta> evictOldestSinkedSST();

    // ── 本地 SST 合并 ──
    void compactLocalSSTs();

    // ── Mem 缓存退化 ──
    void degradeMemCache();          // TODO: 待双持状态完整实现

    // ── 状态快照（供管理接口使用）──
    BucketStateSnapshot stateSnapshot();

    // ── 生命周期 ──
    void close();
}
```

### 5.1 写入 Value 语义

`put(byte[] key, byte[] value)` 中的 `value` 不是通用 KV value，而是 Paimon `InternalRow` 的序列化结果。长期设计中，普通写入应由 RowCodec/序列化管理器把行数据编码成非空 byte payload 后再进入 BucketDirector。

- `value == null` 不表示业务层 NULL，而是内部 delete/tombstone 语义；对外删除应使用 `delete(key)`。
- 非删除写入的 `value` 应表示完整 serialized `InternalRow`。即使一行中所有业务列都是 `NULL`，编码结果也应包含格式头、字段数量、null bitmap 等元信息，设计语义上不应为空 `byte[]`。
- 当前 V1 BucketDirector 仍是底层字节接口，不负责校验 payload 是否符合未来 RowCodec 格式。RowCodec 接入后，空 payload、损坏 payload、schema 不匹配等问题应在序列化/反序列化边界被拒绝。

### 5.2 BucketStateSnapshot

```java
record BucketStateSnapshot(
    int curMemTableEstimatedEntryCount,
    long curMemTableSizeBytes,
    int immutableMemTableCount,
    long immutableMemTableTotalBytes,
    long lastAssignedSequenceId,
    long curMemTableMinSequenceId,
    long curMemTableMaxSequenceId,
    long immutableMemTableMinSequenceId,
    long immutableMemTableMaxSequenceId,
    long lastFlushedSequenceId,
    int newSSTCount,
    long newSSTTotalBytes,
    long newSSTTotalRows,
    long newSSTMinSequenceId,
    long newSSTMaxSequenceId,
    int sinkedSSTCount,
    long sinkedSSTTotalBytes,
    long sinkedSSTTotalRows,
    int withMemCount,                   // TODO: 待双持状态完整实现后补充
    long withMemTotalBytes,             // TODO: 待双持状态完整实现后补充
    long lastSinkedSnapshotId
) {}
```

> 当前实现包含 MemTable 统计、轻量 sequence 边界、`lastFlushedSequenceId`、newSST/sinkedSST 文件数、字节数、物理 entry 数和 lastSinkedSnapshotId；双持状态统计待后续补充。

## 6. 冻结与刷盘策略

### 6.1 Freeze 触发条件（满足任一即触发）

- curMemTable 条目数达到 `PMSConfig.memtableMaxEntries`（默认 100 万）。
- curMemTable 内存占用达到 `PMSConfig.memtableMaxSizeMb`（默认 256MB）。
- 流控 OVERLOADED 水位下主动触发。

### 6.2 Flush 策略

- Freeze 后立即异步提交 Flush 任务。
- Flush 任务将 ImmutableMemTable 序列化为 SST 文件（自定义行存格式，参见 [pms-core-sst-format.md](pms-core-sst-format.md)）。
- Flush 输出的 SST 元数据必须记录源 ImmutableMemTable 的 `minSequenceId/maxSequenceId`。
- SST 文件原子落盘后，BucketDirector 将 `lastFlushedSequenceId` 推进到该 SST 的 `maxSequenceId`。重启恢复时，WAL 中 `sequenceId <= lastFlushedSequenceId` 的 DATA 记录不再回放到 curMemTable，而由已加载 SST 承载。
- 如果崩溃发生在 SST 写出之后、`lastFlushedSequenceId` 推进之前，重启时该 SST 的 `maxSequenceId` 会超过恢复边界，因此被视为 orphan 并忽略，由 WAL replay 恢复对应数据，避免重复数据源。
- `lastFlushedSequenceId` 是本地 SST/WAL 恢复边界，不表示 Paimon Sink 已成功；后续 WAL 截断仍应等待 Sink 成功后的 `persistedSequenceId`。
- Flush 期间，新的写入继续进入新的 curMemTable，不阻塞。

### 6.3 并发限制

- 同一时刻最多 1 个 Flush 任务在执行（单磁盘写串行化，避免 IO 竞争）。
- 如果上一个 Flush 未完成，新 Freeze 的 ImmutableMemTable 排入等待队列。

## 7. Sink 编排

### 7.1 Sink 触发条件

- newSST 数量达到 `PMSConfig.sinkMaxPendingSsts`（默认 8）。
- 距上次 Sink 间隔超过 `PMSConfig.sinkIntervalMs`（默认 30s）。
- 流控 OVERLOADED 水位下提前触发。

### 7.2 Sink 流程（当前 MockSinkManager，后续真实 Paimon 实现）

当前实现先使用 `MockSinkManager` 打通边界，不真实写 Paimon。接口形态对齐后续真实 Paimon sink，后续可将编排逻辑从 BucketDirector 中抽出为更薄的 `SinkCoordinator`：

```java
interface SinkManager {
    PreparedSinkCommit prepare(SinkBatch batch);
    SinkCommitResult commit(PreparedSinkCommit prepared);
}

record SinkBatch(String batchId, List<SSTMeta> ssts, long minSequenceId, long maxSequenceId) {}

record PreparedSinkCommit(
    String batchId,
    long commitIdentifier,
    List<Long> sstIds,
    long minSequenceId,
    long maxSequenceId,
    byte[] payload,
    List<SinkFileRef> fileRefs,
    long inputRecordCount,
    long outputRecordCount
) {}

record SinkCommitResult(String batchId, long snapshotId, long persistedSequenceId, List<Long> sstIds) {}
```

`PreparedSinkCommit.payload` 未来承载 Paimon `CommitMessage` 序列化结果；`fileRefs` 对应 Paimon prepare 阶段生成的数据文件引用，用于恢复前校验。

当前编排顺序：

```text
PMSBucketDirector / future SinkCoordinator        SinkManager
     |                                                  |
     | 1. select newSST -> SinkBatch                    |
     |------------------------------------------------->|
     | 2. prepare(batch)                                |
     |<-------------------------------------------------|
     |    PreparedSinkCommit                            |
     |                                                  |
     | 3. SinkMetaStore save prepare metadata           |
     |                                                  |
     | 4. commit(prepared)                              |
     |------------------------------------------------->|
     |<-------------------------------------------------|
     |    SinkCommitResult                              |
     |                                                  |
     | 5. SinkMetaStore save success metadata           |
     |                                                  |
     | 6. move newSST -> sinkedSST                      |
     |    update SST metadata state                     |
```

### 7.2.1 SST 状态恢复

SST 的可靠状态不写入 SST footer。启动时：

```text
1. 扫描 storage 目录得到全部 SST
2. 扫描 SinkMeta prepare/success
3. 由 success metadata 中的 sstIds 与 persistedSequenceId 推导 sinkedSST 集合
4. 其余 SST = newSST
5. 修正 SST metadata state
```

SST 数据文件名只表达稳定的 flush range, 不表达状态：

```text
sst-000001-000001.sst
```

如果 SST metadata 中的 state 和 SinkMeta 推导状态不一致，以 SinkMeta 为准并重写 metadata。数据文件不做状态 rename。

### 7.3 Sink 失败处理

- **prepare 失败**：重试最多 3 次。仍失败则标记 Sink 异常，上报告警，不继续 commit。
- **commit 失败**：依赖 SinkMeta prepare 记录，重启后重试 commit（参见 [pms-recovery-metadata.md](pms-recovery-metadata.md)）。
- Sink 失败期间，新的 Freeze 和 Flush 仍可正常进行，写入路径不受影响。只是 newSST 会堆积，可能导致水位上升至 OVERLOADED。

## 8. 淘汰策略

### 8.1 ImmutableMemTable 退役

- 每个 ImmutableMemTable 维护引用计数 `refCount`。
- 查询时进入该层前 `refCount++`，离开时 `refCount--`。
- 退役条件：ImmutableMemTable 处于 sinkedSSTWithMem 状态，且 `refCount` 归零，且不是最新两个（避免刚 Sink 完就退役）。
- 内存不足时可通过 `degradeMemCache()` 主动退役：选择最早的双持条目，释放其 Mem 缓存，状态退化为 sinkedSST。

### 8.2 sinkedSST 淘汰

- **只淘汰最老**：保证 PMS 缓存中永远是最新数据，规避幽灵数据问题。
- 目标语义：淘汰前确认无查询引用该 SST（引用计数机制同 ImmutableMemTable）。
- 当前 V1：直接删除磁盘 SST 文件并从列表移除；SST 引用计数或延迟删除队列留待后续补齐。
- 触发条件由 `pms-server` 根据本地 SST 总大小、总文件数或总物理 entry 数判断；core 只提供"退役最老 sinkedSST"的原子能力。
- 物理 entry 数使用 `SSTMeta.entryCount` 求和，包含 tombstone 和跨 SST 的旧版本；不能用 `sequenceId` 范围推导。

### 8.3 SST 合并策略

合并的目标是减少文件数量、提升查询效率。合并不限于"淘汰前"，可以随时触发。

#### 8.3.1 合并适用范围

| 合并来源 | 合并结果 | 说明 |
|----------|---------|------|
| newSSTWithMem × N | newSSTWithMem | SST 归并 + SkipList 归并 |
| sinkedSSTWithMem × N | sinkedSSTWithMem | SST 归并 + SkipList 归并 |
| sinkedSST × N | sinkedSST | 仅 SST 归并 |

不同类别之间不合并（new 和 sinked 生命周期不同，不混并）。

#### 8.3.2 合并时的内存缓存处理

对于带 Mem 缓存的合并（newSSTWithMem / sinkedSSTWithMem）：

1. **SST 侧**：多路归并，按主键序，同一 Key 保留层级更高（更新）的值。
2. **Mem 侧**：将多个 ImmutableMemTable 的 SkipList 做归并迭代，同一 Key 保留层级更高的值，构建一个新的 SkipList 作为合并后的 Mem 缓存。
3. 合并完成后，旧的 SST meta 和旧 ImmutableMemTable 被替换。当前 V1 standalone compact 会保留旧 SST 数据文件作为 covered orphan，避免并发查询读到已删除文件；后续通过 SST 引用计数或延迟删除队列清理。

#### 8.3.3 合并期间内存约束

合并期间需要同时持有旧 SkipList + 正在构建的新 SkipList，有瞬时内存峰值。如果内存不足以完成合并：
- 优先退化最早的双持条目（释放 Mem 缓存空间），再重试合并。
- 若退化后仍不足，跳过本次合并，下次再试。

#### 8.3.4 合并触发条件

- 文件数量：同一类别（new / sinkedWithMem / sinked）的 SST 文件数超过 `PMSConfig.storageCompactMinFiles`（默认 4）。
- 小文件：连续 N 个 SST 的单文件大小 < `PMSConfig.storageCompactThresholdMb`（默认 32MB）。
- 查询效率：当同一类别的 SST 文件数量过多导致 BloomFilter 检查次数过多时，可主动触发合并。
