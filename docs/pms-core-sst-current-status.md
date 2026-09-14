# PMS SST 当前状态与后续交接

## 1. 当前阶段定位

本文档汇总 SST 相关模块的当前实现状态。规范性状态机、并发与调度语义分别以 [pms-core-bucket-director.md](pms-core-bucket-director.md)、[pms-core-local-run-compaction.md](pms-core-local-run-compaction.md) 和 [pms-server.md](pms-server.md) 为准。

## 2. 已完成能力

### 2.1 SST 文件格式与本地读写

- `ImmutableMemTable` 可以 flush 为本地 SST 文件。
- SST 文件采用参考 LevelDB/RocksDB Block Based Table 的自定义格式：Data Block、BloomFilter Block、Index Block、Properties Block、Footer。
- Data Block 支持 prefix compression 和 restart points。
- SST Footer 记录 Bloom/Index/Properties 的 `BlockHandle` 和全文件 CRC32。
- SST 注册时会校验全文件 CRC32 并缓存 reader；点查路径通过缓存的 BloomFilter + Index Block 定位 Data Block，不重复扫描全文件 CRC。
- SST 查询接口返回 `Optional<Value>`，可区分 miss、PUT 命中和 DELETE tombstone 命中。
- SST 已暴露 ordered iterator，可按 key 顺序扫描 `Entry<Key, Value>`，供真实 Paimon sink 做多 SST streaming merge。
- Tombstone 在磁盘层由 `valueLen = -1` 表达，与 WAL DATA 记录一致。

### 2.2 BucketDirector 与本地恢复边界

- BucketDirector 查询路径已包含 `curMemTable -> immutableMemTables -> newSSTs -> sinkedSSTs`。
- Flush 成功后会推进 `lastFlushedSequenceId`。
- Flush 成功后会写独立 `sst-*.meta.json`，记录 SST 文件名、sequence 边界、key 边界、entryCount、状态和校验字段。
- `lastFlushedSequenceId` 持久化在 storage 目录下的 `flush-boundary.meta`。
- 重启恢复时，WAL 中 `sequenceId <= lastFlushedSequenceId` 的 DATA 记录不再回放到 curMemTable，而由 SST 承载。
- 启动扫描由 `SSTRecoveryPlanner` 构建 flushId 连续的本地 run 后缀；`minSequenceId > lastFlushedSequenceId` 的单 flush SST 视为可复用 orphan，不注册、不清理、不推进 allocator，由 WAL replay 后的下一次 Flush 原位覆盖。
- 恢复自动清理被 compact output 覆盖的旧输入，以及位于保留后缀之前、已由 Paimon 覆盖的 meta-only 最老 evict 残留；保留后缀内部损坏或缺口仍启动失败。
- `NEW/SINKED` 使用 SinkMeta success 的 `lastPersistedSequenceId` 推导，不信任 metadata 中可能过时的旧 state。
- retired 文件按 data-first 顺序删除，不额外 force 目录；只有 data/meta 均成功后才移除 cleanup entry。
- SST 文件、`sst-*.meta.json` 和 `flush-boundary.meta` 写入都采用 temp file + force/fsync + atomic rename + directory force 的持久化顺序。

### 2.3 Sink 边界与实现

当前已经定义了 PMS 内部 sink 边界：

```java
interface SinkManager {
    PreparedSinkCommit prepare(SinkBatch batch);
    SinkCommitResult commit(PreparedSinkCommit prepared);
}
```

相关模型：

- `SinkBatch`：记录本次 sink 覆盖的 SST 列表、batchId 和 sequence 范围。
- `PreparedSinkCommit`：记录 prepare 阶段产物，包括 sstIds、sequence 范围、payload、fileRefs 和行数统计。
- `SinkCommitResult`：记录 commit 成功后的 batchId、snapshotId、persistedSequenceId、sstIds 和 commitPayload。
- `SinkMetaStore`：负责将 prepare/success 信息写入独立可读 metadata，并在恢复时加载未完成 prepared commit。
- `SinkMetaPayloadCodec`：作为 `SinkMetaStore` 内部二进制 payload 编码工具复用，便于 prepare/success round-trip。
- `PaimonSinkManager`：生产环境由 `pms-server` 通过 `pms-sink-paimon` 注入真实实现。
- fake/mock `SinkManager`：仅用于 core 与 scheduler 测试，不属于生产路径。

### 2.4 SST sinked 状态来源

SST 的可靠 sinked 状态来源是 SinkMeta success，而不是文件名或 SST footer。

恢复时：

```text
1. 扫描 storage 目录的 `sst-*.meta.json` 并校验对应 SST 文件
2. 扫描 SinkMeta prepare/success
3. 从 success metadata 中读取 sstIds 和 persistedSequenceId
4. sstIds 命中的 SST，或 maxSequenceId 不超过 persistedSequenceId 的 SST 视为 sinkedSST
5. 其余 SST 视为 newSST
6. 将 SSTMeta state 修正为 SINKED
```

SST 数据文件名仅表达稳定 flush range：

```text
sst-000001-000001.sst
```

如果 SST metadata state 和 SinkMeta 推导状态不一致，以 SinkMeta 为准并重写 metadata。数据文件不做状态 rename。

## 3. 当前生产边界

- WAL 只记录数据变更；SSTMeta/SinkMeta 独立记录 Flush/Sink 进度，详见 [pms-recovery-metadata.md](pms-recovery-metadata.md)。
- RowCodec 与 PrimaryKeyCodec 已在 `pms-codec` 落地，并由 server 和真实 Paimon sink 使用。
- `pms-sink-paimon` 支持多 SST streaming merge、prepare/commit、payload round-trip、重复 commit 幂等、delete、prepared file ref 校验和表能力校验。
- `SinkCoordinator` 负责 prepare → durable prepare metadata → commit → durable success metadata；commit 临时失败后可在进程内通过同一 prepared payload 重试。
- BucketDirector 根据 `SinkSelection` 选择有界 NEW 前缀，并在 success 后发布 NEW → SINKED 与 WAL truncate；`PmsTableService` 根据 commit result 发布 lookup delta。
- 本地 compact 支持 NEW/NEW 与 SINKED/SINKED 的连续 run 合并；sinked eviction 只淘汰最老 run；查询通过 read epoch 保护 retired SST 文件。
- ImmutableMemTable 只在等待 Flush 时存在；SST 发布后立即退出查询状态，不实现双持 Mem cache。

## 4. RowCodec 与数据语义

PMS 不是通用 KV 系统。当前语义为：

```text
Key          = Paimon primary key 的稳定有序编码
Value.bytes  = serialized Paimon InternalRow
Value null   = delete tombstone
```

`pms-codec` 的 `PmsRowValueCodec` 负责完整行编码/解码，`RowValueView` 提供按字段读取的
视图，`PmsPrimaryKeyCodec` 负责主键有序编码。core 只保存 opaque bytes，不依赖这些类型。

约束：

- Tombstone 不通过 RowCodec 表达，仍由 `Value.bytes == null` 表达。
- `decode(null)` 不应表示 delete。
- `INSERT/UPDATE_AFTER` 归一化为 `put(key, valueBytes)`；`DELETE/UPDATE_BEFORE` 归一化为 `delete(key)`。
- `pms-codec` 不反向依赖 `pms-core`，因此 primary key codec 返回 `byte[]`，由调用方构造 core 层的 `Key` 或调用 bucket 接口。
- V1 绑定单表，并由产品与部署约束保证该 PMS 本地状态生命周期内的 RowType/Schema 不变；core 与 codec 不负责主动监控或阻止 Schema 变更。
- RowCodec 应保持与启动时固定的 Paimon `InternalRow` / `RowType` 稳定映射；格式版本和 handshake schema fingerprint 用于校验各自的编码/协议边界，不构成运行期 Schema 变更检测机制。
- Mock codec 可以用于打通测试，但类名应明确标记 mock/test，避免误认为生产编码。

## 5. 后续验证

当前后续重点不是再扩展一套 SST 状态，而是验证既有边界：

1. 对 Flush/Sink/compact publish 与 read epoch retire 做进程级故障注入。
2. 通过长时间混合负载校准 1 GiB Sink/compact 操作上限和 NEW/SINKED 数量默认值。
3. 根据真实查询放大和恢复时间决定是否增加新的调度信号。
