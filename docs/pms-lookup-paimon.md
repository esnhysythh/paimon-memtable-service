# PMS Paimon Lookup 模块设计

## 1. 定位与最终决定

`pms-lookup-paimon` 是 PMS 对已提交 Paimon 历史数据执行 primary-key point lookup 的模块。它引入并产品化验证过的 `paimon-local-lookup-test` demo；PMS 自己维护每个 partition-bucket 的 live `DataFileMeta` 视图，再按 Paimon merge-tree 的文件优先级查询候选文件。

该模块替代生产环境中的 Paimon `ReadBuilder` point lookup。`ReadBuilder` 只可用于测试、压测中的正确性对照，不得作为运行时 fallback。

这是一个刻意的取舍：`ReadBuilder` 的谓词读取路径不是面向 KV point lookup 的性能模型。PMS 宁可在无法证明结果正确时明确失败，也不回退到不可预测的扫描式读取。

当前实现已完成模块迁入、direct Parquet 历史点查、完整 snapshot lazy rebuild、普通 sink 成功提交后的 delta 发布，以及热点 value SST cache 的 server 集成；生产 `ReadBuilder` 路径已经删除。显式 compaction 与更完整的压测/观测仍按本文后续阶段推进。

### 1.1 边界

- `pms-core` 保持 byte-oriented，不依赖本模块或 Paimon API。
- `pms-server` 直接创建并调用本模块；不新增 adapter、事件总线或 core listener 层。
- 本模块只依赖 Paimon `1.4.1` 的 `api`、`common`、`core`、`format` artifact；版本统一使用父工程的 `${paimon.version}`。
- 当前实现面向 Paimon primary-key + deduplicate 表的最新态查询，不提供 time travel、MVCC 或完整表 prefix scan。
- Paimon Schema 变更不属于 V1，并由产品与部署约束保证不会发生。lookup 中现有的 schema/profile 校验只属于防御性正确性检查：校验失败不能解释为未命中，但不代表 PMS 会主动监控或完整阻止 Schema 变更。

## 2. 模块依赖

```text
pms-core                 -> 不依赖 Paimon / pms-lookup-paimon
pms-codec                -> Paimon
pms-sink-paimon          -> pms-core + pms-codec + Paimon
pms-lookup-paimon        -> Paimon
pms-server               -> pms-core + pms-codec + pms-sink-paimon + pms-lookup-paimon
```

模块迁入 demo 的生产代码，形成以下内部职责：

```text
lookup.PaimonKeyValueLookupService（入口）
  -> view.LiveFileIndex / view.CandidatePlanner
  -> commit.CommitMessageDeltaExtractor
  -> routing.ThresholdFileLookupRouter
      -> parquet.PaimonKeyValueParquetLookup
      -> cache.valuesst.ValueSstCacheBuilder（热点文件的本地 value SST）
```

`view` 是 PMS 自己拥有的控制面状态；Paimon 不会替 PMS 自动刷新该 view。`commit` 只负责把已成功提交的 Paimon `CommitMessage` 解释为 data-file delta。`routing` 负责热度准入、异步 build、超时、退避和本地 cache 的磁盘预算；cache 只是性能层，`parquet` 单文件点查仍是其正确性基础。

## 3. 查询语义

完整点查的顺序如下：

```text
PMS local memtable / SST
  HIT       -> 返回行
  DELETED   -> 返回 not found，禁止继续查询 Paimon
  MISS      -> pms-lookup-paimon
                 HIT       -> 返回行
                 DELETED   -> 返回 not found
                 MISS      -> 返回 not found
                 UNKNOWN   -> 失败（可重试），绝不返回 MISS
```

lookup 模块保留 demo 的四态 `LookupResult`：

| 状态 | 含义 | server 行为 |
|------|------|-------------|
| `HIT` | 当前完整 view 中找到最新 PUT | 返回行 |
| `DELETED` | 当前完整 view 中首先命中 retract/delete | 返回 not found |
| `MISS` | 当前完整且有效 view 中没有候选文件命中 key | 返回 not found |
| `UNKNOWN` | view 未安装/失效，或文件读取、schema/profile 校验无法保证正确性 | 抛出 `PmsLookupUnavailableException`（可重试） |

`UNKNOWN` 是“无法证明不存在”，不是 negative lookup result。HTTP/RPC 层应将其映射为显式可重试错误（建议 HTTP 503 或等价 RPC status），而不是 `found=false`。

### 3.1 key、partition 与 bucket

`PaimonKeyValueLookupService` 是 bucket-scoped primitive：

```java
LookupResult lookup(BinaryRow partition, int bucket, LookupRequest request)
```

其中 `LookupRequest.key()` 必须是 Paimon **trimmed primary key**，而不是 PMS `PmsPrimaryKeyCodec` 生成的 bytes，也不是简单的主键 tuple。

`PmsTableService` 直接使用 `FileStoreTable.createRowKeyExtractor()` 完成路由：

1. 根据请求构造与表 `rowType` 字段位置一致的 full-row layout `GenericRow`，仅填入 primary-key 字段；非主键字段可为 null。
2. 调用 `extractor.setRecord(row)`。
3. 使用 `extractor.partition()`、`extractor.bucket()`、`extractor.trimmedPrimaryKey()` 调用 lookup primitive。

这个流程与 Paimon `PrimaryKeyPartialLookupTable.get` 的 partition/bucket/key 拆分一致。初期只支持 `HASH_FIXED` bucket；dynamic、postpone 或无法由请求 key 确定 bucket 的表在启动 profile 校验时拒绝启用，不存在 `ReadBuilder` 降级路径。

### 3.2 data file 解析

每次单文件查询必须使用与 bucket 绑定的 `FileLookupContext(partition, bucket)`。server 提供给模块的 `DataFileResolver` 必须通过以下路径解析：

```java
table.store().pathFactory()
    .createDataFilePathFactory(context.partition(), context.bucket())
    .toPath(file);
```

不得按 table root 或文件名独立拼接路径；这会错误处理分区、bucket 或 external path 文件。

### 3.3 Direct reader 与 footer 复用

direct 保留“索引筛选 → 主键列定位行号 → 读取完整 KeyValue”的两阶段数据读取路径。
同一次文件查询取得的 `ParquetLookupMetadata` 贯穿三个阶段；缓存命中后，key reader 与
整行 reader 都通过带 footer 的 `ParquetFileReader` 构造入口创建，不再重复读取、解析 footer。

Paimon 1.4.1 的 `ParquetReaderFactory.createReader(context)` 未提供 footer 注入入口，且内部
装配方法为 private。因此 PMS 在 `ParquetReaderSupport` 中做最小装配：从文件 schema
选取完整顶层字段，保留原物理类型和嵌套结构，再调用 Paimon 的公开字段/列向量工具与
`VectorizedParquetRecordReader`。数据页读取、解压和类型解码继续复用 Paimon 实现。
该路径依赖 V1 固定 Schema 和完整顶层字段读取约束，不实现 Schema 演进或嵌套字段裁剪；
缺少预期字段时失败，不能将缺失数据解释为 MISS。

仅 footer 元数据按只读方式共享；输入流、selection、列向量与 reader 游标均由每次读取独立
持有。装配失败时关闭已打开 reader，避免泄漏输入流。文件失效与 LRU 淘汰仍沿用现有缓存规则。
这一步不缓存 ColumnIndex/OffsetIndex，不共享打开的 reader，也不改变候选顺序、DELETE
或 UNKNOWN 语义。Paimon 升级时需回归公开装配接口、复杂值类型、并发读取和异常资源释放。

## 4. live 文件视图

`LiveFileIndex` 保存 `(partition, bucket) -> immutable Levels view`。`CandidatePlanner` 按 Paimon 的 level 语义选择候选：L0 查询所有 key-range 覆盖文件，L1+ 每层最多选择一个 sorted-run 文件；命中 PUT 或 DELETE 后立即停止。

### 4.1 完整 snapshot 安装

增量前必须先安装一个 bucket 的完整 committed snapshot：

```text
FileStoreTable.store().newScan()
  .withPartitionBucket(partition, bucket)
  .plan().files(FileKind.ADD)
  -> ManifestEntry.file()
  -> installSnapshot(partition, bucket, liveFiles)
```

启动后不持久化或恢复 `LiveFileIndex`。view 初始为空；首次访问一个 bucket 时，server 在表级 commit/publish lock 下读取该 bucket 的完整最新 snapshot 并调用 `installSnapshot`，成功后才允许确定性 lookup。该 lazy rebuild 避免启动时扫描全表，但每个已启用 bucket 的 view 都来自完整 snapshot，而不是历史 delta 回放。

`installSnapshot` 会使该 bucket 的本地 cache 失效。`LocalCacheDirectory` 在启动时取得独占锁并清理遗留 cache 文件；本地 cache 文件不是恢复数据源。

### 4.2 已提交 delta

仅在 Paimon commit 成功后，并且按 snapshot 提交顺序、每个成功提交恰好一次地调用：

```java
applyDelta(partition, bucket, beforeFiles, afterFiles)
```

同一 `CommitMessageImpl` 中的 data-file 变更映射为：

| Paimon 增量 | `beforeFiles` | `afterFiles` |
|-------------|---------------|--------------|
| 普通写入 | `DataIncrement.deletedFiles()` | `DataIncrement.newFiles()` |
| compaction | `CompactIncrement.compactBefore()` | `CompactIncrement.compactAfter()` |

同一 `(partition, bucket)` 的 data 与 compact 文件在一次提交中合并为一次 `applyDelta`。`changelogFiles` 不属于最新态 data-file view，不参与该索引。

非法 delta（未安装 snapshot、移除非 live 文件、重复或乱序）会使该 bucket invalid 并抛出异常。invalid bucket 的查询只能返回 `UNKNOWN`；必须重新读取完整 snapshot 后 `installSnapshot`，不能尝试重放不确定 delta。

## 5. sink 与 compaction 的统一发布

`PreparedSinkCommit.payload` 已经持有 Paimon `CommitMessage` 编码，`SinkCommitResult` 已把它暴露到 server，当前接口为：

```java
record SinkCommitResult(
    String batchId,
    long snapshotId,
    long persistedSequenceId,
    List<Long> sstIds,
    byte[] commitPayload
) {}
```

`PaimonCommitter` 返回 `prepared.payload()`；`SinkMetaPayloadCodec` 同步持久化该值。`PMSBucketDirector.sinkToPaimon()` 返回 `Optional<SinkCommitResult>`，没有待提交 SST 时返回空。

server 直接解码 payload 并调用 lookup 模块发布，不增加 adapter：

```text
Paimon commit success
  -> snapshotId + CommitMessage payload
  -> PmsTableService 解码 CommitMessage
  -> pms-lookup-paimon 按 partition/bucket applyDelta
```

未来显式 compaction 不推进 `persistedSequenceId`，也不复用 `SinkMeta` 的 SST/WAL 语义；它仍需要独立的 prepare/success metadata。但它完成提交后必须产出同样的 `snapshotId + CommitMessage payload`，并走上面的同一 server 发布入口。这样普通 sink、当前写入可能产生的 implicit compact committable，以及未来 explicit compaction 都维护同一 live view。

同一张表的普通 sink commit、compaction commit、snapshot rebuild 与 delta publish 必须由同一把 table-scoped `paimonCommitPublishLock` 串行化。PMS 单表唯一写入者模型是该顺序保证的前提。

## 6. 故障与恢复

| 事件 | 行为 |
|------|------|
| Paimon prepare/commit 失败 | 不发布 delta；维持现有 sink/compaction 恢复语义 |
| commit 成功、delta 发布失败 | Paimon 数据已提交；使相关 bucket invalid，查询报可重试错误，随后完整 snapshot 重建 |
| 进程在 commit 后、发布前崩溃 | 重启后 view 为空；不重放历史 payload，按完整 snapshot 重建 |
| direct Parquet lookup 或 metadata 读取失败 | 返回 `UNKNOWN`，server 返回可重试错误 |
| Value SST build/evict/close 失败 | 若 direct Parquet lookup 仍可正确执行，降级到 direct path；否则 `UNKNOWN` |
| schema/profile 校验失败 | 返回 lookup unavailable 并要求人工检查部署约束，不能转为 `MISS` |

`LiveFileIndex` 使用 copy-on-write 发布 view，查询可观察到更新前或更新后的完整 view，不观察原地修改。router/cache 的失效与 view 更新遵循 demo 的并发约束。

## 7. 首期 profile

启用模块前，server 必须 fail-fast 校验：

- Paimon `FileStoreTable`，有 primary key，merge engine 为 `deduplicate`；
- `HASH_FIXED` bucket；
- Parquet data file；
- 表 schema 固定；启动时固定预期 schema，snapshot 安装、delta 发布和文件查询时校验每个 live data file 的 `schemaId`；
- 主键类型属于 demo 已验证/支持的范围：非 null `INT`、`BIGINT`、`DATE`、`STRING`、`TIMESTAMP(P <= 6, non-LTZ)` 及已验证的多列组合；
- 不支持 `BINARY`、`VARBINARY`、`DECIMAL`、`TIMESTAMP_LTZ`、`TIMESTAMP(P > 6)`，以及 dynamic/postpone bucket。

不支持的 profile 不能启动为带完整历史点查能力的 PMS 服务，直至实现对应 lookup profile；不能隐式改走 `ReadBuilder`。

## 8. 配置、资源与观测

当前 PMS 是单表项目，暂不引入多表 feature flag、跨表预算或配置归属模型。`pms-server` 拥有单表 `PmsLookupConfig`：

- `pms.lookup.cache.enabled`：默认 `true`。关闭后仍使用 direct Parquet lookup，不启用本地 value SST。
- `pms.lookup.cache.dir`：默认 `${java.io.tmpdir}/pms-lookup-cache/<database>.<table>`；测试/开发环境建议覆盖到 `target/lookup-cache`，便于人工检查并随 `mvn clean` 清理。
- `pms.lookup.cache.max_bytes`：默认 `3gb`。这是本表 lookup cache 的本地磁盘预算。
- `pms.lookup.cache.build_threshold`：默认 `3`。同一 data file 被查询达到阈值后异步构建本地 value SST。
- `pms.lookup.cache.build_threads`：默认 `2`，同时也作为当前最大在途 build 数。
- `pms.lookup.cache.build_timeout_ms`：默认 `30000`。
- `pms.lookup.cache.retry_backoff_ms`：默认 `60000`。
- `pms.lookup.direct.metadata_cache_entries`：默认 `1024`。

cache directory 是纯性能层，不是恢复数据源。`ConfigManager` 会拒绝将它放在 WAL、本地 SST storage 或本地 Paimon warehouse 下，避免 cache 清理、锁文件或磁盘预算与 durable state 互相干扰。

当前 `/state` 暴露 lookup cache 开关、direct/local lookup 次数、build 成功/失败/超时/拒绝、ready entry 数、cache bytes 和在途 build 数；日志记录 snapshot install、delta apply、direct/local 路由、build/evict/failure 等关键信息。后续仍需补充四态查询计数、`UNKNOWN`/服务错误数、direct/local lookup 延迟和压测视图。多表隔离与每表配置在 PMS 多表化时设计。

## 9. 实施顺序与测试

1. 已完成：新建模块并迁入 demo 生产代码，固定 Paimon 1.4.1，保留真实 Paimon 测试。
2. 已完成：接入 server 的 key 路由、snapshot install 与四态错误语义；删除生产 `ReadBuilder` 路径。
3. 已完成：暴露成功 commit payload，接入普通 sink 的严格有序 delta 发布。
4. 已完成：加入热点 value SST cache 的 server 配置、资源预算、基础指标与关键日志。
5. 已实现本地 direct/cached 查询基准，见 [本地 Benchmark](pms-benchmark.md)；远端、混合负载与更细粒度指标后续补充。
6. MVP 之后：接入 explicit compaction 的独立恢复 metadata 和统一发布；当前保留普通 sink 的隐式 compaction。

必须覆盖的集成测试包括：分区/多 bucket、复合 key、PUT/DELETE/MISS、L0 与 compaction、snapshot lazy rebuild、非法/重复/乱序 delta、commit 成功后 publish 失败、进程重启后不回放 delta、direct lookup 与 `ReadBuilder` 对照、cache build/evict 以及 schema/profile 拒绝。
