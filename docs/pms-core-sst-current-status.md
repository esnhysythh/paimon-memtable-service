# PMS SST 当前状态与后续交接

## 1. 当前阶段定位

本文档用于记录 SST 相关模块在 Phase 5 收束时的状态。当前目标不是继续扩展功能，而是明确已经完成的存储语义、mock 边界，以及后续 RowCodec 和真实 Paimon sink 需要对接的位置。

## 2. 已完成能力

### 2.1 SST 文件格式与本地读写

- `ImmutableMemTable` 可以 flush 为本地 SST 文件。
- SST 文件采用参考 LevelDB/RocksDB Block Based Table 的自定义格式：Data Block、BloomFilter Block、Index Block、Properties Block、Footer。
- Data Block 支持 prefix compression 和 restart points。
- SST Footer 记录 Bloom/Index/Properties 的 `BlockHandle` 和全文件 CRC32。
- SST 注册时会校验全文件 CRC32 并缓存 reader；点查路径通过缓存的 BloomFilter + Index Block 定位 Data Block，不重复扫描全文件 CRC。
- SST 查询接口返回 `Optional<Value>`，可区分 miss、PUT 命中和 DELETE tombstone 命中。
- Tombstone 在磁盘层由 `valueLen = -1` 表达，与 WAL DATA 记录一致。

### 2.2 BucketDirector 与本地恢复边界

- BucketDirector 查询路径已包含 `curMemTable -> immutableMemTables -> newSSTs -> sinkedSSTs`。
- Flush 成功后会推进 `lastFlushedSequenceId`。
- `lastFlushedSequenceId` 持久化在 storage 目录下的 `flush-boundary.meta`。
- 重启恢复时，WAL 中 `sequenceId <= lastFlushedSequenceId` 的 DATA 记录不再回放到 curMemTable，而由 SST 承载。
- 启动扫描 SST 时，只有 `maxSequenceId <= lastFlushedSequenceId` 的 SST 会注册为有效 SST；`maxSequenceId` 超过边界的 SST 视为 orphan，由 WAL replay 恢复对应数据，避免重复数据源。
- 如果 flush boundary 已经持久化，但启动时发现相关 SST 损坏，当前策略是启动失败，避免 WAL 被跳过后数据丢失。
- SST 文件和 `flush-boundary.meta` 写入都采用 temp file + force/fsync + atomic rename + directory force 的持久化顺序。

### 2.3 Sink 边界与 mock 实现

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
- `SinkCommitResult`：记录 commit 成功后的 batchId、snapshotId、persistedSequenceId 和 sstIds。
- `SinkWalCodec`：负责将 prepare/success 信息编码进 WAL payload。
- `MockSinkManager`：当前只用于打通 PMS 内部状态流转，不真实写 Paimon。

### 2.4 SST sinked 状态来源

SST 的可靠状态来源是 WAL，而不是文件名、manifest 或 SST footer。

恢复时：

```text
1. 扫描 storage 目录得到全部 SST
2. replay WAL 中的 SINK_PREPARE / SINK_SUCCESS
3. 从成功的 SINK_SUCCESS metadata 中读取 sstIds
4. sstIds 中的 SST 视为 sinkedSST
5. 其余 SST 视为 newSST
6. best-effort 修正 SST 文件名标签
```

SST 文件名仅作为人工可观察标签：

```text
sst-000001.new.sst
sst-000001.sinked.sst
```

如果文件名和 WAL 推导状态不一致，以 WAL 为准；rename 失败只记录 warning，不影响正确性。

## 3. 当前 mock 与未完成边界

### 3.1 已有 mock

- `MockSinkManager` 不写 Paimon，只生成 fake prepared payload 和递增 snapshotId。
- `PreparedSinkCommit.payload` 当前可承载 mock payload；未来应承载 Paimon `CommitMessage` 序列化结果。
- `SinkFileRef` 已作为对接 Paimon prepare 阶段文件引用的结构预留。

### 3.2 尚未完成

- SST full scan iterator / ordered iterator 尚未实现。
- 多 SST 按 key 归并、同 key 取最大 sequence 的 merge 层尚未实现。
- RowCodec 尚未实现，当前 `Value.bytes` 只是底层 byte payload。
- `InternalRow -> byte[]` 和 `byte[] -> InternalRow` 尚未打通。
- `InternalRow -> Key` 的 primary key codec 尚未实现。
- 真实 Paimon sink 尚未接入。
- `SinkCoordinator` 尚未抽出，目前 prepare/WAL/commit/WAL/state update 流程仍在 BucketDirector 内。
- 本地 SST compact、sinkedSST evict、双持 Mem 缓存退化仍未实现。
- WAL truncate 仍未切换到 `persistedSequenceId` 主导。

## 4. 后续 RowCodec 对接要求

PMS 不是通用 KV 系统。长期语义中：

```text
Key          = Paimon primary key 的稳定有序编码
Value.bytes  = serialized Paimon InternalRow
Value null   = delete tombstone
```

建议下一阶段引入独立 codec 边界：

```java
interface RowCodec {
    byte[] encode(InternalRow row);
    InternalRow decode(byte[] bytes);
}

interface PrimaryKeyCodec {
    Key encodeKey(InternalRow row);
}

interface RowCodecFactory {
    RowCodec create(RowType rowType);
    PrimaryKeyCodec createPrimaryKeyCodec(RowType rowType, List<String> primaryKeys);
}
```

约束：

- Tombstone 不通过 RowCodec 表达，仍由 `Value.bytes == null` 表达。
- `decode(null)` 不应表示 delete。
- V1 绑定单表，运行期间 RowType/Schema 不变；检测到 schema 变更应视为 fatal。
- 真实 RowCodec 应优先复用 Paimon 自身的 `InternalRow` / `RowType` / serializer 能力，避免手写不兼容格式。
- Mock codec 可以用于打通测试，但类名应明确标记 mock/test，避免误认为生产编码。

## 5. 推荐后续阶段

建议后续工作拆为：

1. **Codec 阶段**：实现 RowCodec、PrimaryKeyCodec、mock codec 和基础往返测试。
2. **SST Iterator 阶段**：实现 SST 顺序读取和多 SST ordered merge。
3. **Sink 适配阶段**：将 SST iterator + RowCodec 适配到 Paimon-sink-demo 的 ordered iterator 输入。
4. **真实 Paimon sink 阶段**：用 demo 中的 PaimonFlusher/PaimonCommitter 替换 MockSinkManager。
5. **Coordinator 阶段**：抽出薄的 SinkCoordinator，减轻 BucketDirector 职责。
