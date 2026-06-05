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
- SST 已暴露 ordered iterator，可按 key 顺序扫描 `Entry<Key, Value>`，供后续 sink 模块做多 SST streaming merge。
- Tombstone 在磁盘层由 `valueLen = -1` 表达，与 WAL DATA 记录一致。

### 2.2 BucketDirector 与本地恢复边界

- BucketDirector 查询路径已包含 `curMemTable -> immutableMemTables -> newSSTs -> sinkedSSTs`。
- Flush 成功后会推进 `lastFlushedSequenceId`。
- Flush 成功后会写独立 `sst-*.meta.json`，记录 SST 文件名、sequence 边界、key 边界、entryCount、状态和校验字段。
- `lastFlushedSequenceId` 持久化在 storage 目录下的 `flush-boundary.meta`。
- 重启恢复时，WAL 中 `sequenceId <= lastFlushedSequenceId` 的 DATA 记录不再回放到 curMemTable，而由 SST 承载。
- 启动扫描 SST 时，只有 `maxSequenceId <= lastFlushedSequenceId` 的 SST 会注册为有效 SST；`maxSequenceId` 超过边界的 SST 视为 orphan，由 WAL replay 恢复对应数据，避免重复数据源。
- 如果 flush boundary 已经持久化，但启动时发现相关 SST 损坏，当前策略是启动失败，避免 WAL 被跳过后数据丢失。
- SST 文件、`sst-*.meta.json` 和 `flush-boundary.meta` 写入都采用 temp file + force/fsync + atomic rename + directory force 的持久化顺序。

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
- `SinkMetaStore`：负责将 prepare/success 信息写入独立可读 metadata，并在恢复时加载未完成 prepared commit。
- `SinkMetaPayloadCodec`：作为 `SinkMetaStore` 内部二进制 payload 编码工具复用，便于 prepare/success round-trip。
- `MockSinkManager`：当前只用于打通 PMS 内部状态流转，不真实写 Paimon。

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

## 3. 当前 mock 与未完成边界

### 3.1 已有 mock

- 恢复元信息已整理为 WAL 只记录数据变更，SSTMeta/SinkMeta 独立记录 flush/sink 进度；详见 [pms-recovery-metadata.md](pms-recovery-metadata.md)。
- `MockSinkManager` 不写 Paimon，只生成 fake prepared payload 和递增 snapshotId。
- `PreparedSinkCommit.payload` 当前可承载 mock payload；未来应承载 Paimon `CommitMessage` 序列化结果。
- `SinkFileRef` 已作为对接 Paimon prepare 阶段文件引用的结构预留。

### 3.2 尚未完成

- 多 SST 按 key streaming merge 已在 `pms-sink-paimon` 初步落地。
- RowCodec 已在 `pms-codec` 落地，并已由 `pms-sink-paimon` 在 sink 路径使用。
- `InternalRow -> byte[]`、`byte[] -> InternalRow`、`InternalRow -> Key` 已具备基础实现。
- 真实 Paimon sink 已初步接入，支持 prepare/commit、SinkMeta payload round-trip、重复 commit 幂等、nullable 非主键场景下的 tombstone delete、多 SST streaming merge 后写入 Paimon、分区表 + 复合主键 delete、prepared file ref 校验失败拒绝 commit，以及非法表能力拒绝；非主键 `NOT NULL` 场景下，Paimon 高层 `TableWrite` 当前会先做整行 nullability 校验，因此 key-only DELETE row 会被拒绝。
- `SinkCoordinator` 已抽出，负责 prepare/SinkMeta/commit/SinkMeta 编排；BucketDirector 仍负责选择待 sink SST、推进本地 SST 状态和刷新内存视图。`SinkMetaStore` 已能识别没有匹配 success 的 prepared commit，并在重启初始化时重试 commit。
- 本地 SST compact 和 sinkedSST evict 已有第一阶段实现；双持 Mem 缓存退化仍未实现。
- WAL truncate 已切换到 sequence 维度接口；定期调度与最新 `persistedSequenceId` 的完整串联仍需在 server/runtime 层补齐。

## 4. 后续 RowCodec 对接要求

PMS 不是通用 KV 系统。长期语义中：

```text
Key          = Paimon primary key 的稳定有序编码
Value.bytes  = serialized Paimon InternalRow
Value null   = delete tombstone
```

建议下一阶段引入独立 `pms-codec` 模块：

```java
interface RowCodec {
    byte[] encode(InternalRow row);
    InternalRow decode(byte[] bytes);
}

interface PrimaryKeyCodec {
    byte[] encodeKey(InternalRow row);
}

interface RowCodecFactory {
    RowCodec create(RowType rowType);
    PrimaryKeyCodec createPrimaryKeyCodec(RowType rowType, List<String> primaryKeys);
}
```

约束：

- Tombstone 不通过 RowCodec 表达，仍由 `Value.bytes == null` 表达。
- `decode(null)` 不应表示 delete。
- `INSERT/UPDATE_AFTER` 归一化为 `put(key, valueBytes)`；`DELETE/UPDATE_BEFORE` 归一化为 `delete(key)`。
- `pms-codec` 不反向依赖 `pms-core`，因此 primary key codec 返回 `byte[]`，由调用方构造 core 层的 `Key` 或调用 bucket 接口。
- V1 绑定单表，运行期间 RowType/Schema 不变；检测到 schema 变更应视为 fatal。
- 真实 RowCodec 应优先复用 Paimon 自身的 `InternalRow` / `RowType` / serializer 能力，避免手写不兼容格式。
- Mock codec 可以用于打通测试，但类名应明确标记 mock/test，避免误认为生产编码。

## 5. 推荐后续阶段

建议后续工作拆为：

1. **Codec 阶段**：实现 RowCodec、PrimaryKeyCodec、mock codec 和基础往返测试。
2. **SST Iterator 阶段**：SST 顺序读取已在 `pms-core` 落地；多 SST ordered merge 在 `pms-sink-paimon` 中推进。
3. **Sink 适配阶段**：将 SST iterator + RowCodec 适配到 Paimon-sink-demo 的 ordered iterator 输入。
4. **真实 Paimon sink 阶段**：用 demo 中的 PaimonFlusher/PaimonCommitter 替换 MockSinkManager。
5. **Coordinator 阶段**：薄 `SinkCoordinator` 已抽出；后续继续收敛更完整的 sink 调度、失败退避和指标暴露。
