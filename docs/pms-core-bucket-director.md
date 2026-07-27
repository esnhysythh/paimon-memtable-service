# PMS Bucket Director 设计文档

## 1. 模块定位

`PMSBucketDirector` 是 `pms-core` 内单个 PMS 表的状态协调器。它组合 MemTable、WAL、本地 SST、SinkMeta 与 `SinkManager`，向上层提供 byte-oriented 写入、查询以及单步生命周期操作。

Bucket Director 只负责操作正确性，不决定何时为了 Paimon 可见性或本地文件数量执行操作。生产调度策略属于 `pms-server`，见 [pms-server.md](pms-server.md) § 2.4。

核心职责：

- 保证 WAL、sequence、MemTable 可见顺序与 Freeze 边界一致。
- 维护 `ACTIVE_MEM → IMMUTABLE_PENDING_FLUSH → NEW → SINKED → PAIMON_ONLY` 状态流转。
- 为点查与范围查询发布完整可见视图，使层级迁移对查询透明。
- 提供一次只完成一个确定动作的 Freeze、Flush、Sink、Compact、Evict API。
- 维护 Flush/Sink 恢复边界，并恢复未完成的 prepared Sink。

## 2. 数据状态模型

```text
write
  │
  ▼
ACTIVE_MEM
  │ Freeze：容量阈值或显式 fence
  ▼
IMMUTABLE_PENDING_FLUSH
  │ Flush：SST 与 flush boundary 完整发布
  ▼
NEW local run
  │ Sink：Paimon commit 与 SinkMeta success 完整发布
  ▼
SINKED local run
  │ Evict oldest
  ▼
PAIMON_ONLY
```

| 状态 | 内存 | 本地 SST | 已进入 Paimon | 本地查询 |
|------|------|----------|---------------|----------|
| `ACTIVE_MEM` | 可写 SkipList | 否 | 否 | 是 |
| `IMMUTABLE_PENDING_FLUSH` | 只读 SkipList | 否 | 否 | 是 |
| `NEW` | 否 | 是 | 否 | 是 |
| `SINKED` | 否 | 是 | 是 | 是 |
| `PAIMON_ONLY` | 否 | 否 | 是 | 由 `pms-lookup-paimon` 查询 |

### 2.1 ImmutableMemTable 的定位

ImmutableMemTable 是等待 Flush 的写缓冲，不是 SST 的长期内存缓存。Flush 完成时采用“目标层先发布，源层后移除”：

1. `flushToSST` 写出并校验 SST data/meta，并将 SST 原子发布到 storage 的查询可见视图。
2. 原子持久化并推进 `lastFlushedSequenceId`。
3. 从 MemTable 可见视图移除对应 ImmutableMemTable。
4. 将 NEW run 发布到 maintenance 使用的 `RunState`。

因此迁移期间查询至少能看到源层或目标层；迁移完成后只从 SST 读取该批数据。V1 不存在 `newSSTWithMem`、`sinkedSSTWithMem` 或 MemTable 主动退化调度。

### 2.2 NEW 与 SINKED

- NEW 表示尚未被 Paimon success boundary 覆盖的 local run。
- SINKED 表示已经由 SinkMeta success 确认进入 Paimon 的 local run。
- NEW 只与 NEW compact，SINKED 只与 SINKED compact；禁止跨状态合并。
- Sink 不阻止后续 compact。已经 Sink 的一批小 SST 可以在 SINKED 状态继续合并。
- 只有 SINKED 可以 Evict，并且只允许从最老 run 开始。

本地 run、`runId` 与连续 `flushId` range 的详细规则见 [pms-core-local-run-compaction.md](pms-core-local-run-compaction.md)。

## 3. 查询语义

### 3.1 点查

`lookup(key)` 按从新到旧的顺序查询，首次命中即停止：

```text
current MemTable
immutable MemTables（新到旧）
NEW runs（新到旧）
SINKED runs（新到旧）
```

core 使用 `Optional<Value>` 表达三态：

| 结果 | 含义 | 行为 |
|------|------|------|
| empty | 本地 miss | 继续查询更老层；全部 miss 后由 server 决定是否查询 Paimon |
| `Value.bytes() != null` | PUT 命中 | 返回值并停止 |
| `Value.bytes() == null` | tombstone 命中 | 返回 DELETED 并停止，禁止旧值复活 |

`get(key)` 是便捷包装，会把 tombstone 转为 `Optional.empty()`；需要区分 MISS 与 DELETED 的上层必须使用 `lookup(key)`。

### 3.2 Range / Prefix Scan

`scan(startInclusive, endExclusive)` 遍历全部本地层，按 key 聚合并选择最大 `sequenceId` 的 entry；最新 entry 为 tombstone 时不返回该 key。结果按 unsigned lexicographical key order 排序。`prefixScan(prefix)` 通过 `PrefixNext` 转换为范围查询。

V1 scan 是 weakly consistent，不提供 MVCC snapshot：与写入真正并发时，可能只观察到一个 batch 的一部分。若未来要求 batch-atomic scan，需要引入 read sequence 与多版本 key，而不是扩大全局锁。

### 3.3 查询并发契约

- 写请求已经成功返回，且 lookup 在其后发起时，该写入必须可见。
- 写入与 lookup 真正并发时，lookup 可以观察写前或写后结果，不等待写入。
- Freeze、Flush、Sink、Compact、Evict 对查询透明，不得因迁移产生瞬时 MISS。
- 查询不获取 `writeMutex`；读性能不因后台维护而被全局串行化。

MemTable 通过不可变 `MemTableState` 整体发布。点查与 scan 通过 `LocalStorageManager.readVisibleSnapshot()` 原子获取当前可见 SST 集合并进入 read epoch；指定文件集合的 Sink/compact 使用 `readSnapshot(...)`。compact/evict 先发布新集合并将旧文件放入 retired queue，活跃 epoch 释放后才物理删除旧文件。

## 4. 对外操作接口

当前核心接口的生命周期部分为：

```java
FreezeResult freezeCurMemTable();
FlushResult flushImmutableMemTable();
SinkOperationResult sinkToPaimon(SinkSelection selection);
SinkOperationResult resumeSinkFlight();
CompactionResult compactLocalSSTs(CompactionSelection selection);
EvictionResult evictOldestSinkedSST();
BucketStateSnapshot stateSnapshot();
```

这些方法均为单步操作，不包含隐式的其他维护动作。例如 Evict 不先 compact，Sink 不主动 Freeze/Flush，调度器必须根据新快照决定下一步。

结果对象通过 `OperationStatus.PROGRESSED/NOOP` 明确表示是否推进。调用方在 `PROGRESSED` 后必须重新获取快照，不应在旧快照上继续推演计划。

### 4.1 Freeze

Freeze 在 `writeMutex` 内执行短状态切换：

1. 当前 MemTable 为空时返回 fence，但不发布空 ImmutableMemTable。
2. 构造新的 CurMemTable 对象。
3. 将旧对象冻结为 ImmutableMemTable。
4. 原子发布新的 `MemTableState(current, immutables)`。

写入不会继续持有旧 CurMemTable 的可写引用，因此不需要在查询路径增加锁。写入容量达到 `maxEntries` 或 `maxSizeMb` 后，会在完整 batch apply 结束时复用同一个 `freezeCurMemTable` 内部语义自动 Freeze。

### 4.2 Flush

一次 `flushImmutableMemTable()` 只处理最老的一个 ImmutableMemTable。慢 SST IO 不持有 `writeMutex`；写入可以继续进入新的 CurMemTable。

storage 发布采用 prepare/publish 两段式：SST data、SST meta 与经过完整校验的 reader 均在 SST 可见视图 monitor 外准备，随后在短临界区内一起发布。查询看到的 SST meta 必须始终存在对应的 cached reader；缺失时按内部状态损坏记录 `PMS_STORAGE_INVARIANT` 错误并失败，不在查询线程中重新扫描文件。compact 输出和批量 NEW→SINKED 状态变化遵循同一规则，元数据 fsync 不占用查询可见视图 monitor。

`lastFlushedSequenceId` 是本地 SST/WAL replay 边界，不是 Paimon 可见性边界。若 SST 文件已写出但 flush boundary 尚未发布就崩溃，该文件作为 orphan 忽略，数据由 WAL 重放；若 boundary 已发布，则启动时必须验证承载它的 SST 完整存在。
flush boundary 使用独立的串行化边界，不与 SST 可见视图/read epoch 共用 monitor。

### 4.3 Sink 与 prepared retry

`SinkSelection(targetSequenceId, maxInputBytes)` 选择不超过目标 sequence 的最老连续 NEW 前缀：

- 批次总输入受 `maxInputBytes` 约束。
- 如果最老单个 run 已超过上限，仍允许它单独推进，避免永久卡死。
- 不用 SST 数量限制 Sink 批次；多 run 由 streaming multi-way merge 处理。
- 一次 Sink 只处理一个有界前缀。固定 Paimon fence 可以由多次 Sink 逐步达到。

Sink 按以下可靠顺序推进：

```text
select NEW prefix
  -> SinkManager.prepare
  -> durable SinkMeta prepare
  -> Paimon commit
  -> durable SinkMeta success
  -> persist every selected SSTMeta as SINKED
  -> idempotently publish NEW -> SINKED and persisted boundaries
  -> clear Sink flight
  -> best-effort truncate eligible WAL files
```

`sinkFlight` 的可恢复状态只有两种：

- `PREPARED_RETRY`：durable prepare 已存在，但 durable success 尚不存在。`resumeSinkFlight()` 只重试原 prepared commit。
- `FINALIZING`：Paimon commit 和 durable success 已存在。`resumeSinkFlight()` 按 batchId 读取原 success，只重做本地 SST metadata、RunState 和 boundary 收尾，绝不再次调用 Paimon prepare/commit。

两种状态都保持最高维护优先级，不得准备新 batch，也不得执行会改变相关 run 的 compact/evict。SST metadata 可能在多个文件之间部分写入，因此本地收尾必须幂等：重试接受目标 run 已经是 NEW 或 SINKED，并最终发布同一组 SINKED run。WAL truncate 是 success 后的空间回收；删除失败保留候选文件供后续 truncate 重试，但不让已经完成的逻辑 Sink 永久占用 flight。

### 4.4 Local compact

`CompactionSelection` 明确给出同状态、连续、从旧到新排列的 run IDs。core 在执行前重新校验：

- run 仍处于维护可见集合中；
- 状态全部与 selection 一致；
- flush range 连续；
- 至少包含两个 run；
- 不与 active/prepared Sink 的保护边界冲突。

选择已过期时返回 NOOP，而不是对另一组文件执行操作。输出完整落盘后，通过不可变 `RunState` 一次替换输入集合。

### 4.5 Evict

`evictOldestSinkedSST()` 不接受 run ID，因为 V1 只允许淘汰当前最老 SINKED run。方法不会隐式 compact；是否先 compact 由 server 调度层决定。物理文件删除受 SST read epoch 保护。

## 5. 并发与锁边界

| 同步边界 | 保护内容 | 不应承担的工作 |
|----------|----------|----------------|
| `writeMutex` | writer queue、WAL/sequence/MemTable 提交顺序、Freeze 对象切换、写入前水位复查 | Flush/Sink/Compact/Evict IO，查询 |
| `sstMaintenanceMutex` | Sink、prepared retry、Compact、Evict 的互斥与稳定选择/发布 | 普通写入、lookup、scan |
| `lifecycleLock` | close 与在途操作的生命周期互斥 | 代替数据状态发布协议 |
| `MemTableState` | current + immutable 列表的不可变聚合视图 | 长期缓存策略 |
| `RunState` | NEW + SINKED 的不可变维护视图 | SST 文件物理存活期 |
| SST read epoch | 查询已选择文件的存活期 | 调度决策 |

慢操作遵循“锁内确认/发布边界，锁外或非写锁执行 IO”的原则。`sstMaintenanceMutex` 串行化 SST 生命周期操作，避免同时启动两个 Sink 或让 compact/evict 改变 Sink 输入；它不要求阻塞写入，因为新生成的 Immutable/SST 可以在下一轮快照中被处理。

## 6. 状态快照

`BucketStateSnapshot` 是调度与管理接口的事实来源，主要包含：

- 采样时间和 Cur/Immutable 的数量、字节数、sequence 与最早写入时间。
- `lastAssignedSequenceId`、`lastFlushedSequenceId`、`lastPersistedSequenceId`。
- NEW/SINKED 数量、字节数、行数、sequence 与最早写入时间。
- 按 flush range 排序的 `LocalRunSnapshot` 列表。
- 当前 `SinkFlightSnapshot`、恢复出的未持久化数据标志、最后 Paimon snapshot ID。

快照不携带可由这些原始字段即时推导的 age 字段。调用方使用 `observedAtMillis - oldestWriteAtMillis` 计算 age。

快照对 MemTable 统计是弱一致的：可以包含或不包含真正并发的一次写入。结构迁移通过不可变 MemTable 状态与 storage 一次性 meta view 观察，不获取写入锁。调度器只依赖保守水位并在每个成功动作后重新采样，因此不要求全局瞬时一致快照。

## 7. 写入与流控

`writeBatch` 是 core 的提交边界；单条 put/delete 复用 size=1 batch。一个 batch 在 `writeMutex` 内完成 admission recheck、WAL append、连续 sequence 分配、MemTable apply 与必要的 auto-freeze。

若 Immutable 或 NEW backlog 已达到 flow-control 水位，core 在 WAL append 前抛出 `PmsWriteOverloadedException`，整批不进入 WAL。该检查不做容量预留：已被接受的一批数据可以使计数刚好达到水位，目标是阻止积压继续扩大，而非提供精确全局配额。

详细 sequence 与 WAL 边界见 [pms-sequence-and-write-boundary.md](pms-sequence-and-write-boundary.md)。

## 8. 恢复与错误边界

启动恢复顺序的关键约束：

1. 加载并校验 SST 与 `flush-boundary.meta`。
2. 加载 SinkMeta success，推导 NEW/SINKED 状态并修正 SST meta。
3. 加载未完成 prepare；V1 只允许一个 durable pending prepare。
4. replay `sequenceId > lastFlushedSequenceId` 的 WAL DATA 到 CurMemTable。
5. 恢复全局 sequence 水位，并暴露 `recoveredUnpersistedData` 供调度器立即建立可见性 fence。

启动时发现 WAL/SST/SinkMeta 损坏、边界交叉或 prepared metadata 不一致，应拒绝启动；不能将已参与恢复边界的数据静默降级为 miss。运行期后台操作的普通 `RuntimeException` 由 worker 记录并在下一周期重试，积压最终通过 flow control 限制新写入。更细的运行期 fatal 分类留待取得真实故障样本后再设计。
