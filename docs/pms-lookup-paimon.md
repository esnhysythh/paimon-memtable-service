# PMS Paimon Lookup 模块设计

## 1. 定位与设计原则

`pms-lookup-paimon` 是 PMS 对已提交 Paimon 历史数据执行 primary-key point lookup 的模块。PMS 维护每个 partition-bucket 的 live `DataFileMeta` 视图，按 Paimon merge-tree 的文件优先级查询候选文件。

生产点查使用 direct Parquet reader 和热点文件的本地 value SST cache。Paimon `ReadBuilder` 仅用于测试、压测中的正确性对照，不作为运行时 fallback。

`ReadBuilder` 的谓词读取路径具有扫描式读取的成本特征。PMS 在无法证明结果正确时返回 unavailable，避免将点查隐式转为成本不可预测的扫描。

模块通过完整 snapshot 按需建立文件视图，并通过普通 sink 成功提交后的 delta 维护视图。显式 compaction 调度不属于当前模块的运行职责。

### 1.1 边界

- `pms-core` 保持 byte-oriented，不依赖本模块或 Paimon API。
- `pms-server` 直接创建并调用本模块。
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

模块内部职责如下：

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
                 UNKNOWN   -> 完整 snapshot 重建后重试查询
                                仍无法确定结果 -> lookup unavailable
```

lookup 模块使用四态 `LookupResult`：

| 状态 | 含义 | server 行为 |
|------|------|-------------|
| `HIT` | 当前完整 view 中找到最新 PUT | 返回行 |
| `DELETED` | 当前完整 view 中首先命中 retract/delete | 返回 not found |
| `MISS` | 当前完整且有效 view 中没有候选文件命中 key | 返回 not found |
| `UNKNOWN` | view 未安装/失效，或候选文件读取无法确定结果 | 尝试完整 snapshot 重建并重查；恢复失败或仍为 UNKNOWN 时抛出 `PmsLookupUnavailableException` |

`UNKNOWN` 表示无法确定查询结果，不能作为 negative lookup result 返回。server 在同一次请求内尝试重建；成功后直接返回查询结果。无法恢复时，JSON 接口返回 HTTP 503 和 `LOOKUP_UNAVAILABLE`，binary 协议返回相应失败状态，而不是 `found=false`。Schema/profile 不匹配同样以 unavailable 表达，不解释为 MISS。

JSON/raw 历史查询共用 `lookupPaimonResult` 完成路由、查询、snapshot 重建与异常处理，只在各自入口转换返回格式。该方法只返回 HIT / DELETED / MISS，无法消除的 UNKNOWN 转为 lookup unavailable；raw 保留 DELETED 与 MISS 的区别。

无法按 bucket 隔离的发布异常通过清空全部文件视图转入按需重建，查询是否可用由当前视图及重建结果决定。发布期间的 Error 在清空视图后原样重抛。本地 HIT/DELETED 不进入历史查询路径。具体恢复语义见第 6 节。

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

footer 与已解析的 Page Index 按只读方式共享；输入流、selection、列向量与 reader 游标
由每次读取独立持有。装配失败时关闭已打开 reader，避免泄漏输入流。
Paimon 升级时需回归公开装配接口、复杂值类型、并发读取和异常资源释放。

### 3.4 Page Index 缓存

`ParquetLookupMetadata` 按文件持有 `ParquetPageIndexCache`，供筛选、主键定位和整行读取
复用已解析的索引，减少重复读取与解析。

- **ColumnIndex**：按主键筛选需要加载。
- **OffsetIndex**：按 RowGroup 懒加载，首次访问时加载组内所有列的索引。命中查询需要完整
  KeyValue，因此优先合并读取连续索引；布局分散、超过合并缓冲上限或涉及加密列时沿用逐列读取。
  冷 MISS 同样会加载该组的 value 索引。

同一组或条目的首次加载通过同步合并，完整成功后发布只读结果，失败可重试；不同组或条目
可以并发加载，热查询无需加锁。索引读取通过 `ParquetFileReader` 的内部扩展入口接入，
升级 Paimon 时需验证接口及索引只读共享的安全性。

索引缓存与 footer 共用文件身份、LRU 淘汰和失效生命周期，容量按文件数限制（默认 1024），
实际内存随列数和页数增长。文件失效后，已开始的查询可以完成，但旧加载结果不会重新写入缓存。

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

批量发布以 `(partition, bucket)` 为故障隔离单位。发布结束后，相关视图必须完整包含该提交的变更，或保持 missing/invalid；不完整的视图不能继续以 valid 状态参与查询。

仅在 Paimon commit 成功后，并且按 snapshot 提交顺序、每个成功提交恰好一次地调用：

```java
int failedBucketCount = applyCommittedMessages(messages);
```

同一 `CommitMessageImpl` 中的 data-file 变更映射为：

| Paimon 增量 | `beforeFiles` | `afterFiles` |
|-------------|---------------|--------------|
| 普通写入 | `DataIncrement.deletedFiles()` | `DataIncrement.newFiles()` |
| compaction | `CompactIncrement.compactBefore()` | `CompactIncrement.compactAfter()` |

同一 `(partition, bucket)` 的 data 与 compact 文件在一次提交中合并为一次 `applyDelta`。`changelogFiles` 不属于最新态 data-file view，不参与该索引。

missing 和 invalid bucket 跳过 delta，保留按最新完整 snapshot 重建的语义；不得用后续增量将 invalid bucket 恢复为 valid。

对 valid bucket，文件视图更新或旧文件 cache 清理抛 RuntimeException 时，先调用 `LiveFileIndex.invalidate`，再尽力清理 bucket cache，然后继续发布其余 bucket。cache 清理二次失败单独记录，不覆盖原始失败，也不打断后续 bucket。返回值只统计本次发布新发生的局部失败；server 记录对应 batch/snapshot，保持已成功 sink 的结果。

payload 解码、delta 提取或 bucket 逻辑失效抛出未隔离的 RuntimeException，或发布期间出现 Error 时，server 在同一提交锁内调用 `invalidateAllViews`，通过 `LiveFileIndex.clear` 清空全部视图，包括已经更新和尚未更新的 bucket。清空完成后，RuntimeException 记录日志并保留已成功 sink 的结果，Error 原样重抛；两者都不重试已经成功的 Paimon commit。服务继续运行时，后续查询按 bucket 重建最新完整 snapshot。

移除非 live 文件等校验失败会使 bucket invalid。提交顺序和恰好一次交付仍由 server 保证，文件索引不承担完整的 snapshot 去重或乱序检测。invalid bucket 的查询只能返回 `UNKNOWN`；必须重新读取完整 snapshot 后 `installSnapshot`，不能尝试重放不确定 delta。低层 `applyDelta` 保留异常传播，生产 commit 统一走上述批量入口。

## 5. sink 与 compaction 的统一发布

`PreparedSinkCommit.payload` 保存 Paimon `CommitMessage` 编码，`SinkCommitResult` 将提交结果和 payload 传递给 server：

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

server 解码 payload 后调用 lookup 模块发布：

```text
Paimon commit success
  -> snapshotId + CommitMessage payload
  -> PmsTableService 解码 CommitMessage
  -> pms-lookup-paimon applyCommittedMessages，按 partition/bucket 隔离发布
```

未来显式 compaction 不推进 `persistedSequenceId`，也不复用 `SinkMeta` 的 SST/WAL 语义；它仍需要独立的 prepare/success metadata。但它完成提交后必须产出同样的 `snapshotId + CommitMessage payload`，并走上面的同一 server 发布入口。这样普通 sink、当前写入可能产生的 implicit compact committable，以及未来 explicit compaction 都维护同一 live view。

同一张表的普通 sink commit、compaction commit、snapshot rebuild 与 delta publish 必须由同一把 table-scoped `paimonCommitPublishLock` 串行化。PMS 单表唯一写入者模型是该顺序保证的前提。

普通健康点查不获取该表级锁；只有 UNKNOWN 恢复路径需要它。现有单线程 maintenance worker 在 sink（包含发布）返回后才能淘汰 SINKED SST；未来增加独立 eviction 执行者时必须继续保证这个先后关系。

## 6. 故障与恢复

| 事件 | 行为 |
|------|------|
| Paimon prepare/commit 失败 | 不发布 delta；维持现有 sink/compaction 恢复语义 |
| commit 成功、单个 bucket delta 发布失败 | 该 bucket invalid，其余 bucket 继续发布；不重试已成功 commit，查询按需重建完整 snapshot，重建失败才报 unavailable |
| payload decode/extract 失败，或无法完成 bucket 逻辑失效（RuntimeException） | 在提交锁内清空全部 LiveFileIndex 视图；清空成功后保留 sink 成功结果，JSON/raw 历史查询按需重建最新完整 snapshot，无需重启 |
| 发布期间出现 Error | 在提交锁内清空全部 LiveFileIndex 视图，然后原样重抛；服务继续运行时，查询按需重建；完整 runtime fatal 处理独立维护 |
| 进程在 commit 后、发布前崩溃 | 重启后 view 为空；不重放历史 payload，按完整 snapshot 重建 |
| direct Parquet lookup 或 metadata 读取失败 | 返回 `UNKNOWN`，server 尝试完整 snapshot 重建，仍无法查询则返回可重试错误 |
| Value SST build/evict/close 失败 | 若 direct Parquet lookup 仍可正确执行，降级到 direct path；否则 `UNKNOWN` |
| schema/profile 校验失败 | 返回 lookup unavailable 并要求人工检查部署约束，不能转为 `MISS` |

`LiveFileIndex` 使用 copy-on-write 发布 view，查询可观察到更新前或更新后的完整 view，不观察原地修改。文件视图失效先于可失败的 cache 清理；missing/invalid view 阻止新查询使用残留 cache 判定结果。

UNKNOWN 查询在表级锁内读取最新完整 snapshot 并安装视图，等待期间发生的提交和视图清空均先于重建完成。snapshot 扫描、cache 清理、安装过程中的 IOException/RuntimeException 转为 lookup unavailable；missing/invalid view 仅在完整安装成功后恢复。逻辑失效不会撤回已交给在途查询的旧 view，不提供跨 bucket 原子快照。

全部视图清空只操作内存索引，不执行 Paimon 扫描或 Value SST cache 清理。每个 bucket 在重新安装 snapshot 前清理对应 cache，清理失败时仍保持 missing/invalid。清空后到重建前的新提交跳过这些 missing bucket，重建读取的最新完整 snapshot 包含期间所有已成功提交；重建完成后的提交继续按 delta 更新。

该恢复方式复用启动时的按需重建路径，不需要后台全表扫描、delta 重放或新的查询锁。索引清空使用现有索引 monitor，提交、清空和 snapshot 重建由现有表级锁串行化；正常历史查询不获取表级锁，前台写入不参与此恢复锁。代价是恢复后的首次 bucket 查询需要读取元数据并重建视图，重建期间 sink 等待表级锁。按需重建避免为未访问的 bucket 建立索引。

missing 与 invalid 在历史查询入口具有相同的恢复行为：读取该 bucket 的最新完整 committed snapshot，再安装有效视图。`LiveFileIndex` 描述参与查询的完整文件集合；它不同于用于加速单文件读取的 ValueSST cache，后者不可用时可以降级到 direct Parquet 查询。

重建由查询同步触发，没有独立的后台视图重建任务或持久的发布失败标志。重建失败时，本次请求返回 lookup unavailable，后续请求可以再次尝试。内存索引清空不涉及 IO 或业务回调，不维护清空失败的独立恢复状态；该操作自身的异常直接传播，无法完成内存清空或严重 JVM 故障后的恢复不属于此机制的保证范围。

## 7. 首期 profile

启用模块前，server 必须 fail-fast 校验：

- Paimon `FileStoreTable`，有 primary key，merge engine 为 `deduplicate`；
- `HASH_FIXED` bucket；
- Parquet data file；
- 表 schema 固定；启动时确定预期 schema，候选文件查询时校验其 `schemaId`；
- 主键类型属于 demo 已验证/支持的范围：非 null `INT`、`BIGINT`、`DATE`、`STRING`、`TIMESTAMP(P <= 6, non-LTZ)` 及已验证的多列组合；
- 不支持 `BINARY`、`VARBINARY`、`DECIMAL`、`TIMESTAMP_LTZ`、`TIMESTAMP(P > 6)`，以及 dynamic/postpone bucket。

不支持的 profile 不能启动为带完整历史点查能力的 PMS 服务，直至实现对应 lookup profile；不能隐式改走 `ReadBuilder`。

## 8. 配置、资源与观测

PMS 为单表服务，lookup 配置和资源预算按绑定表归属。`pms-server` 持有单表 `PmsLookupConfig`：

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

## 9. 测试范围

测试使用 Paimon 1.4.1 的真实本地表验证查询结果，以小型 fake 和受控线程交错验证故障与并发边界。本地 direct/cached 查询基准的负载和测量口径见 [本地 Benchmark](pms-benchmark.md)。

必须覆盖的集成测试包括：分区/多 bucket、复合 key、PUT/DELETE/MISS、L0 与 compaction、snapshot lazy rebuild、非法/重复/乱序 delta、commit 成功后 publish 失败、进程重启后不回放 delta、direct lookup 与 `ReadBuilder` 对照、cache build/evict 以及 schema/profile 拒绝。

发布故障测试覆盖 missing/invalid 跳过、失败 bucket 隔离、cache 清理二次失败、全部视图清空后按需恢复、恢复前后的后续提交、Error 清空视图后原样传播、snapshot 恢复与提交排序，以及本地 SINKED SST 淘汰后的新增、更新和删除查询。正常历史查询不等待表级 commit 锁；等待重建的查询在发布异常导致视图清空后仍能取得锁并重建。清空不修改在途查询已经取得的只读 view，重建失败后的查询仍可重试。
