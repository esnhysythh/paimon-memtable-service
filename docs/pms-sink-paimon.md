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
  -> LocalStorageManager.openIterator(...)
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

## 10. 与 pms-server 的交接

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
