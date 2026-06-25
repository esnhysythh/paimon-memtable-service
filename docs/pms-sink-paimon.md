# PMS Paimon Sink 设计文档

## 1. 模块定位

`pms-sink-paimon` 是 PMS 写回 Apache Paimon 的真实 sink 适配层。

它实现 `pms-core` 中定义的 `SinkManager` SPI，将本地 `newSST` 中的 latest-state KV 数据转换为 Paimon `InternalRow`，再通过 Paimon streaming write/commit API 写入目标 Paimon 表。

模块职责边界：

```text
pms-core        -> 选择 SinkBatch、维护 WAL sink 状态机、推进 SST new/sinked 生命周期
pms-codec       -> key/value bytes 与 Paimon InternalRow 之间的编码/解码
pms-sink-paimon -> SST iterator merge、row 转换、Paimon prepare/commit
pms-server      -> 加载 Paimon table、创建 PaimonSinkManager、调度 sink
```

`pms-sink-paimon` 不负责选择哪些 SST 需要 sink，也不直接修改本地 SST 状态；这些仍由 `pms-core` 的 `PMSBucketDirector` 和 `SinkCoordinator` 负责。

## 2. 依赖方向

```text
pms-sink-paimon
  -> pms-core
  -> pms-codec
  -> Paimon API / common / core / format
```

该模块依赖 `pms-core` 的：

- `SinkManager`
- `SinkBatch`
- `PreparedSinkCommit`
- `SinkCommitResult`
- `SinkFileRef`
- `LocalStorageManager`
- `SSTEntryIterator`

依赖 `pms-codec` 的：

- `PmsRowValueCodec`
- `PmsPrimaryKeyCodec`

## 3. 表能力约束

PMS V1 只支持 Paimon primary-key + deduplicate merge-engine 表。`PaimonSinkTableValidator` 在构造 sink 时进行校验：

- 表必须是 `FileStoreTable`。
- 表必须包含 primary key。
- 表的 `merge-engine` 必须是 `deduplicate`。

拒绝其他 merge engine 的原因是 PMS 内部存储语义是 latest-state KV：

```text
同一 primary key 只保留最新 sequence 的一条记录
最新记录为 tombstone 时删除该 key
```

该语义与 Paimon deduplicate merge engine 对齐，但无法表达 partial-update、aggregation、first-row 等需要更复杂 merge 函数的表。

## 4. Prepare 流程

`PaimonFlusher.prepare(SinkBatch)` 负责将一批 SST 预写入 Paimon，并返回可 WAL 持久化的 `PreparedSinkCommit`。

流程：

```text
SinkBatch.ssts
  -> LocalStorageManager.readSnapshot(...)
  -> SSTReadSnapshot.openIterator(...)
  -> PaimonSinkEntryMerger.mergeLatest(...)
  -> PaimonSinkRowConverter.toPaimonRow(...)
  -> StreamTableWrite.write(row)
  -> StreamTableWrite.prepareCommit(...)
  -> PaimonCommitPayloadCodec.encode(commitMessages)
  -> collect SinkFileRef
  -> PreparedSinkCommit
```

### 4.1 SST Streaming Merge

`PaimonSinkEntryMerger` 做多路有序归并：

- 输入 iterator 必须按 PMS key 有序。
- 输出仍按 key 有序。
- 多个 SST 出现相同 key 时，选择 `Value.sequenceId` 最大的 entry。
- tombstone 与普通 put 一样参与比较，若 tombstone 是最新 entry，则输出 tombstone。

这样 sink 到 Paimon 的数据仍保持 deduplicate latest-state 语义。

### 4.2 Row 转换

`PaimonSinkRowConverter` 根据 `Entry.value().isTombstone()` 分两类处理：

| PMS Entry | Paimon Row |
|-----------|------------|
| put | `PmsRowValueCodec.decode(rowType, valueBytes)`，并设置 `RowKind.INSERT` |
| tombstone | `PmsPrimaryKeyCodec.decodeKey(keyBytes)`，构造 `RowKind.DELETE` row，只填 primary key 字段 |

delete/tombstone 不写入 row value bytes；它只由 PMS KV 层表达。

### 4.3 Commit Identifier

当前 `commitIdentifier` 使用 `SinkBatch.maxSequenceId()`。

该选择依赖 PMS 全局 sequence 单调递增，并要求 sink batch 串行提交。同一个 prepared commit 在 WAL recovery 中重试时会使用相同 commit identifier，从而复用 Paimon 的幂等提交能力。

## 5. Commit 流程

`PaimonCommitter.commit(PreparedSinkCommit)` 负责提交已经 prepare 的 Paimon commit messages。

流程：

```text
PreparedSinkCommit
  -> verify SinkFileRef
  -> PaimonCommitPayloadCodec.decode(payload)
  -> StreamTableCommit.filterAndCommit(commitIdentifier -> messages)
  -> read table.latestSnapshot()
  -> SinkCommitResult
```

提交前会校验 `PreparedSinkCommit.fileRefs`：

- data file 必须存在。
- 当前文件大小必须与 prepare 阶段记录一致。

该校验用于发现 prepared data file 在 SinkMeta recovery 或 commit 前被异常删除/修改的情况。当前只校验 file size，尚未引入 checksum。

## 6. SinkMeta 与恢复协作

`pms-sink-paimon` 本身不直接写 WAL，也不直接写 SinkMeta。prepare/success metadata 由 `pms-core` 中的 `SinkCoordinator` 和 `SinkMetaStore` 编排：

```text
SinkCoordinator.sink(batch)
  -> PaimonSinkManager.prepare(batch)
  -> save prepare metadata
  -> PaimonSinkManager.commit(prepared)
  -> save success metadata
```

崩溃恢复时：

```text
SinkMetaStore load
  -> 收集未匹配 success 的 PreparedSinkCommit
  -> SinkCoordinator.recoverPrepared(prepared)
  -> PaimonSinkManager.commit(prepared)
  -> save success metadata
  -> BucketDirector 将对应 SST 标记为 sinked
```

这要求 `PreparedSinkCommit.payload` 必须完整保存 Paimon `CommitMessage` 列表，且 commit 阶段必须使用原始 `commitIdentifier`。

## 7. Delete 语义与 Paimon 已知限制

PMS tombstone 在 Paimon sink 中转换为只填 primary key 字段的 `RowKind.DELETE` row。

在 nullable 非主键字段场景下，该行为已经通过真实 Paimon 集成测试验证。

当前 Paimon 高层 `TableWrite` 路径存在一个已知限制：

```text
当非主键字段为 NOT NULL 时，key-only DELETE row 会在 prepare 阶段被整行 nullability 校验拒绝。
```

错误形式类似：

```text
Cannot write null to non-null column(...)
```

PMS 暂不通过填充占位值的方式绕过该限制，以免污染 delete 语义。该问题应优先反馈给 Paimon 社区处理。

## 8. 当前测试覆盖

当前测试覆盖包括：

- commit payload 空列表 round-trip。
- 非法 commit payload 拒绝。
- 多 SST streaming merge。
- 重复 key 取最大 sequence。
- tombstone 作为最新 entry 被保留。
- merge iterator 正常耗尽和提前关闭时释放输入 iterator。
- row value 转 INSERT row。
- tombstone 转 key-only DELETE row。
- 真实 Paimon prepare/commit。
- WAL prepared payload round-trip 后 commit。
- 重复 commit 幂等。
- nullable 非主键字段下 tombstone delete。
- 非主键 `NOT NULL` 字段下 key-only DELETE 被 Paimon 拒绝。
- 多 SST merge 后真实写入 Paimon。
- 分区表 + 复合主键 + delete。
- prepared data file 缺失时拒绝 commit。
- 非 primary-key 表拒绝。
- 非 deduplicate merge-engine 表拒绝。
- prepare 打开后续 SST iterator 失败时关闭已打开 iterator。

## 9. 已知限制与后续工作

### 9.1 表加载与配置

当前模块接收已经创建好的 Paimon `Table`。后续 `pms-server` 需要负责：

- 从配置加载 warehouse、database、table、catalog options。
- 创建 Paimon catalog 和 table。
- 构造 `PaimonSinkManager` 并注入 `PMSBucketDirectorImpl`。

### 9.2 错误分类与重试策略

当前失败会包装为 `RuntimeException`。后续 server/runtime 层应补充：

- prepare 失败与 commit 失败分类。
- recovery commit 重试退避。
- 最大重试次数或 fatal 策略。
- 文件丢失、权限错误、Paimon conflict、schema 不兼容等错误分类。

### 9.3 Prepared File 校验

当前只校验 file size。后续可以考虑：

- 记录并校验 file checksum。
- 校验 row count。
- 区分 object store eventual consistency 下的临时不可见与确定丢失。

### 9.4 Paimon API 兼容性

当前实现依赖 Paimon `CommitMessageImpl` 来收集 data file refs。该路径贴近 Paimon 内部实现，升级 Paimon 版本时需要重点回归：

- `CommitMessage` 序列化格式。
- `CommitMessageImpl` 中 new/compact/changelog file 的访问方式。
- `DataFilePathFactory` 路径生成方式。

### 9.5 观测指标

建议后续暴露：

- input/output record count。
- sink batch SST 数量。
- prepare/commit 耗时。
- Paimon snapshot id。
- persisted sequence id。
- prepared file ref 数量。
- pending prepared commit 数量。

## 10. Paimon Compaction 集成方案

PMS 需要掌控 Paimon 表的合并，因为 PMS 是该表的唯一修改者。调研 Paimon 1.4.x 代码后，结论是：不应设计成调用 `Table.compact()`，因为 `Table` 没有这个 Java Program API；Paimon 的 compact 入口在 `TableWrite` 上：

```java
write.compact(partition, bucket, fullCompaction);
messages = write.prepareCommit(true, commitIdentifier);
commit.filterAndCommit(Map.of(commitIdentifier, messages));
```

`compact(...)` 只提交或触发后台 compaction task，不保证返回时 compact 已完成；compact 结果会在后续 `prepareCommit(waitCompaction=true, ...)` 中 drain 成 `CommitMessage`。因此 PMS 必须把 compaction 当作一种可恢复的 Paimon commit，而不是 fire-and-forget 后台动作。

### 10.1 与普通 Sink 的关系

当前 `PaimonFlusher.prepare` 使用 `StreamTableWrite.prepareCommit(true, commitIdentifier)`。在 Paimon 主键表中，写入 flush 后会触发 `CompactManager.triggerCompaction(false)`，`waitCompaction=true` 还会等待结果。这意味着当前 PMS 写入路径可能已经隐式产生 compact committable，但该行为不受 PMS scheduler、metadata 和指标掌控。

推荐改成两份 table view：

| 用途 | table view | commit user | 行为 |
|------|------------|-------------|------|
| 普通 sink | `table.copy(Map.of("write-only", "true"))` | `pms-sink-${tableId}` | 只写入新 data files，不做写入端 compaction/snapshot expiration |
| 显式 compaction | `table.copy(Map.of("write-only", "false"))` | `pms-compact-${tableId}` | 枚举 partition/bucket，显式调用 Paimon compact，并提交 compact snapshot |

使用独立 commit user 的原因是 Paimon stream commit 的 `commitIdentifier` 需要在同一 commit user 下单调递增并可用于幂等过滤。普通 sink 已使用 `SinkBatch.maxSequenceId()` 作为 identifier；compaction 没有 PMS sequence 边界，应维护独立的本地单调 `compactionCommitId`。

docs: record Paimon implicit compaction cleanup note当前 PMS 初期暂不急于切换到 `write-only=true`。普通 sink 可能携带 Paimon 写入端隐式 compaction 产生的 `CompactIncrement`，但该增量已经包含在同一份成功提交 payload 中，lookup view 可按统一 delta 正确更新。以分钟级 sink 频率为目标时，隐式 compaction 的延迟和指标可先作为后续优化项。

需要注意：`write-only=true` 不只会关闭写入端隐式 compaction，也会跳过 Paimon snapshot expiration。当前未启用 `write-only=true` 时，Paimon 会在普通 commit 后按表配置清理过期 snapshot 及其不再被引用的旧文件；未来如果 PMS 接管 compaction 并启用 write-only sink，则也需要同步接管 snapshot/file 清理与对应指标。

### 10.2 Compaction Prepare/Commit 流程

建议新增 `PaimonCompactionManager`，仍放在 `pms-sink-paimon`，不让 `pms-core` 依赖 Paimon API。

```text
PaimonCompactionPlanner
  -> FileStoreTable.newSnapshotReader().bucketEntries()
  -> select partition/bucket candidates
  -> StreamTableWrite.compact(partition, bucket, fullCompaction)
  -> StreamTableWrite.prepareCommit(true, compactionCommitId)
  -> encode CommitMessage payload
  -> save prepared compaction metadata
  -> StreamTableCommit.filterAndCommit(compactionCommitId -> messages)
  -> save success compaction metadata
```

候选枚举可先使用 `SnapshotReader.bucketEntries()`，它能得到 `partition`、`bucket`、`fileCount`、`fileSizeInBytes`、`recordCount` 和最近文件创建时间。V1 先按 `fileCount >= threshold` 或手动 full compact 触发即可；后续如果要精确识别 L0 文件数或 level 分布，再读取 manifest entries 或 `DataSplit.dataFiles()`。

### 10.3 Metadata 与恢复

Compaction 不推进 PMS `persistedSequenceId`，也不改变本地 SST 状态，因此不要把 compact success 写成 `SinkMeta`。建议新增独立 metadata：

```text
compact-prepare-${compactionId}.json
  compactionId
  commitIdentifier
  fullCompaction
  partitionBuckets
  paimonCommitPayloadBase64
  outputFileRefs
  inputFileCount/outputFileCount

compact-success-${compactionId}.json
  compactionId
  commitIdentifier
  snapshotId
  committedAtMillis
```

崩溃恢复时，对存在 prepare 但没有 success 的 compaction，使用原始 `commitIdentifier` 和 payload 调用 `StreamTableCommit.filterAndCommit(...)`。这与普通 sink 的 prepared commit 恢复模型一致，可保证重复提交幂等。

### 10.4 调度与并发约束

- 同一 PMS 进程内，普通 sink commit 与 compact commit 应串行化。文件 rewrite 可以异步，但 manifest commit 要串行，以保持 snapshot 来源可解释。
- 同一 Paimon partition 的 compaction 只能有一个执行者。PMS 独占写入模型下，禁止外部 Flink/Spark dedicated compact job 同时作用于同一表。
- Compaction 失败不应回滚 PMS 本地 WAL/SST 边界；只标记 compact task 失败并重试或等待人工处理。普通 sink 仍可继续推进，但如果 compact 长期失败，点查穿透和 AP 查询会承受更多 sorted runs。
- 优雅停机应停止调度新的 compact task；已写出 prepared metadata 的 task 必须完成 commit 或在下次启动恢复提交。

### 10.5 实现优先级

1. 先调整 `PaimonSinkManager` 构造，让普通 sink 使用 `write-only=true` 的 table copy，并把 `prepareCommit` 的隐式 compaction 从写入路径剥离。
2. 新增 `PaimonCompactionManager`、`PreparedPaimonCompaction`、`PaimonCompactionMetaStore`，复用现有 `PaimonCommitPayloadCodec` 和 file ref 校验逻辑。
3. server scheduler 增加 `compactPaimon()`，V1 以 bucket file count 阈值和手动 full compact API 为触发条件。
4. 增加真实 Paimon 集成测试：write-only sink 不产生 compact snapshot；显式 compact 后文件数下降/compact snapshot 出现；prepare 后崩溃能 recovery commit；重复 recovery commit 幂等。

## 11. 与 pms-server 的交接

### 11.1 向 lookup 模块发布成功提交

`pms-lookup-paimon` 维护 Paimon data-file live view，需要在 commit **成功后**获得同一批 `CommitMessage` 的 data/compact 文件增量。为保持 `pms-core` 不依赖 Paimon 类型，`PreparedSinkCommit.payload` 继续是 opaque bytes；但 `SinkCommitResult` 应向 server 暴露同一份 commit payload，并由 server 解码后直接发布给 lookup 模块。

普通 sink、写入路径可能产生的 implicit compaction，以及未来 explicit compaction 都必须在同一张表的 commit/publish 串行约束下发布。compaction 使用独立 metadata 和 commit identifier，但提交成功后同样产出 `snapshotId + CommitMessage payload`。lookup 不重放历史 payload：发布失败或进程重启时以完整 bucket snapshot 重建。详见 [pms-lookup-paimon.md](pms-lookup-paimon.md)。

`pms-sink-paimon` 当前已经足以支撑 `pms-server` 最小闭环：

```text
server API
  -> pms-codec
  -> PMSBucketDirector.put/delete/get
  -> flush
  -> SinkCoordinator
  -> PaimonSinkManager
  -> Paimon table
```

server 第一阶段可先提供：

- 写入 row。
- 删除 primary key。
- primary key 点查。
- 手动 flush。
- 手动 sink。
- 状态快照。
- 启动恢复。

自动调度、退避、指标和后台任务可以在最小闭环跑通后逐步补齐。
