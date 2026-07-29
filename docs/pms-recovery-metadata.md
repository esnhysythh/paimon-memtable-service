# PMS Recovery Metadata 设计文档

## 1. 背景

旧实现中，PMS 的恢复元信息分散在不同位置：

- 数据变更写入 WAL 的 `DATA` 记录。
- Sink Paimon 的 prepare/success 也写入 WAL。
- Flush SST 的本地恢复边界写入 `storage/flush-boundary.meta`。
- SST 是否已经 sinked 由 success metadata 推导，并写回 SST metadata state。

这套实现可以工作，但职责边界不够清晰：WAL 既是数据日志，又承载部分后台状态机；Flush 和 Sink 同样是后台物化进度，却分别使用不同持久化方式。V1 的新恢复模型把这些信息整理为更容易解释、测试和人工排障的结构。

## 2. 目标决策

PMS V1 采用以下恢复元信息模型：

```text
Data WAL  = 用户数据变更事实
SSTMeta   = 本地 SST 物化进度与文件元信息
SinkMeta  = Paimon prepare/commit 进度与外部持久化边界
```

最终决策：

- WAL 只记录数据变更，不再记录 flush/sink 控制事件。
- Flush SST 与 Sink Paimon 都通过独立、可读、可校验、原子更新的 metadata 文件记录恢复所需信息。
- WAL 截断以 `SinkMeta.success.persistedSequenceId` 为主边界，而不是以 Paimon `snapshotId` 为主边界。
- SST 数据文件名 publish 后保持稳定，不包含 `NEW` / `SINKED` 状态；metadata 保存 run 结构，启动生命周期状态由 SinkMeta success 的 `persistedSequenceId` 推导，而不是依赖文件名或历史 runId。

该选择的理由：

- WAL 重新变成单一职责的数据日志，恢复时只负责重放尚未被本地 SST 覆盖的数据。
- Flush 和 Sink 都是低频后台操作，用人类可读 metadata 不会形成性能瓶颈。
- 独立 metadata 更利于现场排障，可以直接查看某个 SST 或 sink batch 的边界、文件引用和提交状态。
- 未来如果要迁移 WAL 格式，可以减少控制事件兼容负担。

## 3. Data WAL

### 3.1 职责

WAL 只回答一个问题：

```text
有哪些用户数据变更在崩溃后必须重放？
```

WAL 不再表达：

- 某个 SST 是否 flush 成功。
- 某个 SST 是否已经 sink 到 Paimon。
- 某次 Paimon prepare/commit 是否完成。
- 某个 Paimon snapshot 是否存在。

### 3.2 应用层记录格式

WAL 中只有一种应用层数据记录，因此 PMS 应用层不再需要 `type` 字段：

```text
DATA:
┌────────────┬──────────┬──────────┬────────────┬──────────┐
│ sequenceId │ keyLen   │ key      │ valueLen   │ value    │
│ (8 byte)   │ (4 byte) │ (N byte) │ (4 byte)   │ (M byte) │
└────────────┴──────────┴──────────┴────────────┴──────────┘

valueLen >= 0  -> put/upsert, value 为 serialized InternalRow bytes
valueLen = -1  -> delete/tombstone, 无 value bytes
```

说明：

- `sequenceId` 仍是 PMS 内部恢复和截断的主边界坐标。
- `valueLen = -1` 继续表达 delete/tombstone，与 MemTable/SST 的 tombstone 语义保持一致。
- `valueLen = 0` 不表达 tombstone，也不表达业务 NULL；接入 RowCodec 后应由 codec 边界决定是否允许空 payload。
- 传输层仍可以继续使用 LevelDB WAL chunk 格式及 CRC32C 校验。
- WAL 文件头仍应保留 magic/version/lastSequenceId 等文件级信息，以支持格式演进和旧 WAL 截断后恢复 sequence 水位。

### 3.3 Replay 规则

启动恢复时：

```text
load SSTMeta + flush boundary
replay WAL DATA where sequenceId > lastFlushedSequenceId
```

`sequenceId <= lastFlushedSequenceId` 的 DATA 记录由本地 SST 承载，不再回放到 `curMemTable`。

## 4. SSTMeta

### 4.1 职责

SSTMeta 负责回答：

```text
哪些数据已经成功物化到本地 SST？
这些 SST 覆盖了哪些 sequence 边界？
这些 SST 当前是否已经被 Paimon sink 持久化？
```

### 4.2 文件组织

建议 storage 目录组织如下：

```text
storage/
  sst-000001-000001.sst
  sst-000001-000001.meta.json
  sst-000002-000004.sst
  sst-000002-000004.meta.json
  flush-boundary.meta
```

其中：

- `.sst` 是不可变数据文件。
- `.meta.json` 是该 SST 的人类可读元信息。
- `flush-boundary.meta` 记录本地 WAL replay 边界，也可以在后续演进为 `sst-manifest.json` 中的字段。

### 4.3 SSTMeta 字段

当前 `sst-*.meta.json` 至少包含：

```json
{
  "version": 1,
  "runId": 1,
  "minFlushId": 1,
  "maxFlushId": 1,
  "sstFile": "sst-000001-000001.sst",
  "state": "NEW",
  "fileSize": 12345,
  "entryCount": 1000,
  "minKeyBase64": "...",
  "maxKeyBase64": "...",
  "minSequenceId": 1,
  "maxSequenceId": 1000,
  "oldestWriteAtMillis": 1709999999000,
  "createdAtMillis": 1710000000000,
  "metaCrc32": 987654321
}
```

字段说明：

- `state` 初期包含 `NEW` / `SINKED`。
- `runId` 是物理唯一标识，只用于 reader cache、删除和排障；逻辑新旧顺序由 `minFlushId/maxFlushId` 表达。
- `sstFile` 是稳定 SST 数据文件名；恢复时按 `minFlushId/maxFlushId` 查找实际存在的 SST 文件。metadata 中的旧 `state` 只用于观测，运行状态由 SinkMeta success 的 `lastPersistedSequenceId` 重新推导。
- `minKeyBase64/maxKeyBase64` 保持 JSON 可读结构，同时避免二进制 key 破坏文本格式。
- `oldestWriteAtMillis` 随 Flush 与 compact 保留该 run 中最早写入时间，供 server 计算 Paimon 可见性 lag。
- SST 数据文件完整性仍由 SST footer 中的 full-file CRC 校验；启动时还会对比 `.meta.json` 与 SST properties 中的关键字段。
- `metaCrc32` 覆盖 metadata 中除自身外的稳定字段，用于发现半写或人工误改。

### 4.4 Flush 持久化顺序

Flush 必须保持以下顺序：

```text
1. 写 sst-000001-000001.sst.tmp
2. force SST 文件内容
3. atomic rename -> sst-000001-000001.sst
4. force storage directory
5. 写 sst-000001-000001.meta.json.tmp
6. force meta 文件内容
7. atomic rename -> sst-000001-000001.meta.json
8. force storage directory
9. 推进 flush-boundary.meta(lastFlushedSequenceId = maxSequenceId)
```

如果崩溃发生在 boundary 推进前，恢复时忽略该 SST，并通过 WAL 重放恢复数据。该单 flush orphan 不进入普通 cleanup，也不推进 `nextFlushId`；下一次 Flush 使用相同 flushId 原位覆盖它。

如果 boundary 已经推进且尚未被 `lastPersistedSequenceId` 覆盖，恢复出的可见 SST 后缀必须承载到该 boundary；若缺失或损坏，应启动失败，避免跳过 WAL 后丢失数据。已经进入 Paimon 的最老 SINKED cache 可以正常淘汰，不要求本地 SST 从 flushId 1 开始。

### 4.5 Flush Boundary

`flush-boundary.meta` 继续记录：

```text
version=1
lastFlushedSequenceId=1000
crc32=...
```

它只表示本地 SST 已覆盖的 WAL replay 边界，不表示 Paimon 已 commit，也不能作为 WAL 最终截断依据。

## 5. SinkMeta

### 5.1 职责

SinkMeta 负责回答：

```text
哪些 SST 已经进入 Paimon prepare？
哪些 prepared batch 已经 commit 成功？
Paimon 已经持久化到哪个 PMS sequence 边界？
```

SinkMeta 必须同时记录 prepare 和 success。只记录 success 不够，因为崩溃可能发生在 Paimon prepare 完成后、commit 成功前；恢复时需要原始 prepared payload 重试 commit。

### 5.2 文件组织

建议 sink metadata 目录组织如下：

```text
sink/
  batch-000001.prepare.json
  batch-000001.success.json
  batch-000002.prepare.json
  sink-current.meta
```

其中：

- `*.prepare.json` 是 Paimon prepare 成功后的可恢复提交信息。
- `*.success.json` 是 Paimon commit 成功后的确认信息。
- `sink-current.meta` 是可选聚合视图，用于快速查看最新 `snapshotId`、`persistedSequenceId` 和成功 batch；恢复正确性不应只依赖它。

### 5.3 Prepare Metadata

`batch-*.prepare.json` 至少包含：

```json
{
  "version": 1,
  "batchId": "sink-1000-4",
  "commitIdentifier": 1000,
  "sstIds": [1, 2, 3, 4],
  "minSequenceId": 1,
  "maxSequenceId": 1000,
  "paimonCommitPayloadBase64": "...",
  "fileRefs": [
    {
      "fileName": "data-xxx.orc",
      "path": "file:/warehouse/db/table/...",
      "fileSize": 123456,
      "rowCount": 1000,
      "partition": "{}",
      "bucket": 0
    }
  ],
  "inputRecordCount": 1200,
  "outputRecordCount": 1000,
  "createdAtMillis": 1710000000000,
  "metaCrc32": 123456789
}
```

说明：

- `paimonCommitPayloadBase64` 保存 Paimon `CommitMessage` 序列化结果。外围字段保持 JSON 可读，二进制 payload 使用 Base64。
- `commitIdentifier` 应保持确定性，当前建议继续使用 `maxSequenceId`。
- `fileRefs` 用于恢复 commit 前校验 prepared data files 是否仍存在且未被明显篡改。

### 5.4 Success Metadata

`batch-*.success.json` 至少包含：

```json
{
  "version": 1,
  "batchId": "sink-1000-4",
  "snapshotId": 42,
  "persistedSequenceId": 1000,
  "sstIds": [1, 2, 3, 4],
  "committedAtMillis": 1710000001000,
  "metaCrc32": 987654321
}
```

说明：

- `persistedSequenceId` 是 WAL 安全截断的主边界。
- `snapshotId` 表示 Paimon 外部提交结果，只能作为排障和外部一致性校验信息，不能单独决定 WAL 截断。
- `sstIds` 用于恢复 exact prepared/finalizing batch；它不是长期 SST 状态边界，避免本地 cache 清空、runId 重新分配后与历史 ID 碰撞。启动时使用 `persistedSequenceId` 判定状态：run 的 max sequence 不超过它则为 SINKED，run 的 min sequence大于它则为 NEW，跨越边界则拒绝启动。

### 5.5 Sink 持久化顺序

Sink 必须保持以下顺序：

```text
1. 选择待 sink 的 NEW SST，形成 SinkBatch
2. Paimon prepareCommit，生成 CommitMessage 和 data file refs
3. 写 batch.prepare.json.tmp
4. force prepare meta 文件内容
5. atomic rename -> batch.prepare.json
6. force sink directory
7. Paimon commit，生成 snapshotId
8. 写 batch.success.json.tmp
9. force success meta 文件内容
10. atomic rename -> batch.success.json
11. force sink directory
12. 将对应 SSTMeta state 更新为 SINKED
13. 可选更新 sink-current.meta
```

如果崩溃发生在第 2 步之后、第 6 步之前，恢复时看不到 prepare meta，因此不会重试 commit；数据仍在本地 SST/WAL 中，后续可以重新 prepare/sink。可能遗留 Paimon prepared files，需要依赖 Paimon 清理策略或后续运维工具处理。

如果崩溃发生在第 6 步之后、第 10 步之前，恢复时必须加载 `prepare.json` 并重试 commit。

如果崩溃发生在第 10 步之后、第 12 步之前，恢复时通过 `success.json` 将对应 SSTMeta 修正为 `SINKED`。

## 6. 恢复流程

目标恢复流程：

```text
1. 初始化 sink metadata
   - 扫描 batch-*.prepare.json 和 batch-*.success.json
   - 取得 durable lastPersistedSequenceId
   - V1 校验最多只有一个 pending prepare

2. 初始化 storage
   - 加载 flush-boundary.meta
   - 扫描并校验全部 sst-*.meta.json / sst-*.sst 文件
   - 用单 candidate 贪心选择范围更大的 compact output，最终构建 flushId 连续的本地后缀
   - 识别 compact 旧输入、data-first 最老 evict 残留和 flush-boundary orphan
   - 按 lastPersistedSequenceId 推导 NEW/SINKED；内部缺口或无法解释的损坏 fatal

3. 初始化 WAL
   - 扫描 WAL 文件头和 DATA record，恢复 lastSequenceId

4. Replay WAL DATA
   - sequenceId <= lastFlushedSequenceId: 跳过
   - sequenceId > lastFlushedSequenceId: 回放到 curMemTable

5. 恢复未完成 sink
   - 对 prepare 存在但 success 不存在的 batch，使用原始 prepared payload 重试 commit
   - commit 成功后写 success metadata

6. 修正本地 SST 状态
   - recovered commit 推进 persistedSequenceId 后重新推导 SST state
   - SST 数据文件名保持不变

7. best-effort 清理恢复规划识别出的 compact/evict 垃圾；失败保留文件，由下次启动重新识别并重试

8. 启动服务和后台任务
```

## 7. WAL 截断

WAL 截断应以 sink success metadata 中的 `persistedSequenceId` 为主：

```text
walFile.maxSequenceId <= latestPersistedSequenceId
```

附加约束：

- 当前正在写入的 WAL 文件永不删除。
- 若 WAL 文件只包含头部或无 DATA，可以依赖文件头中的 sequence 水位判断是否保留。
- `snapshotId` 可用于确认外部 Paimon 状态，但不应替代 PMS 内部 sequence 边界。

## 8. 崩溃矩阵

| 场景 | 持久化结果 | 恢复行为 |
|------|------------|----------|
| DATA WAL 写入前崩溃 | 无 WAL DATA | 写入未成功，不恢复 |
| DATA WAL 写入后、MemTable 可见前崩溃 | 有 WAL DATA | replay 到 curMemTable |
| SST 文件写完、SSTMeta 未写 | 有 orphan SST 文件 | 不注册、不清理、不推进 nextFlushId；由 WAL replay 后的下一次 Flush 原位覆盖 |
| SSTMeta 写完、flush boundary 未推进 | 有 SST/SSTMeta，但 boundary 未覆盖 | 同上，作为可复用 orphan |
| compact 输出已发布、旧输入只删除一部分 | 大范围完整输出覆盖旧输入 | 选择大范围输出，旧输入进入 best-effort cleanup |
| 最老 SINKED data 已删、meta 删除失败 | meta-only 前缀，sequence 已由 Paimon 覆盖 | 不注册，作为已开始 evict 的垃圾继续清理 |
| 保留后缀内部 SST/SSTMeta 缺失 | flushId 出现内部缺口 | 启动失败，人工介入 |
| flush boundary 未被 Paimon 覆盖且本地尾部缺失 | boundary 指向无承载数据 | 启动失败，避免丢数据 |
| Paimon prepare 成功、prepare meta 未写 | Paimon 可能有临时 data files | 不恢复 commit，后续重新 sink；可能遗留外部垃圾 |
| prepare meta 已写、success meta 未写 | 可恢复 prepared commit | 重试 commit，成功后写 success |
| success meta 已写、部分或全部 SSTMeta 未标记 SINKED | commit 已确认 | 进入 `FINALIZING`，按 exact batch success 幂等修正 SSTMeta、RunState 和 boundary，不再次 commit |
| success meta 已写、WAL 未截断 | 数据可能重复存在于 WAL/SST/Paimon | 逻辑 Sink 保持完成；通过 flush boundary 和 sink meta 避免 replay 重复，并在后续 truncate 再次尝试删除 |

## 9. 当前实现状态

当前实现已经完成核心职责拆分：

- WAL 只写 DATA 记录，应用层记录格式已移除 `type` 字段。
- SST 文件写出后会同步写独立、人工可读的 `sst-*.meta.json`，并在启动时与 SST properties 交叉校验。
- Sink prepare/success 写入独立 `SinkMetaStore`，文件为可读 metadata，并带校验字段。
- `SinkCoordinator` 的持久化依赖已经从 `WALManager` 切换到 `SinkMetaStore`。
- 启动恢复通过扫描 SinkMeta 恢复 pending prepare，并用 success meta 推导 sinked SST。
- 运行期 durable success 之后的本地收尾失败会进入 `FINALIZING`，由 Maintenance 最高优先级重做；该路径只读取原 success metadata，不创建新 Paimon commit。
- Flush boundary 已使用独立 `flush-boundary.meta`。
- 运行期 flush boundary 写入失败时，非持久化 FlushFlight 会复用已经发布的 SST 完成重试；进程崩溃后仍按 orphan SST + WAL replay 恢复。
- `SSTRecoveryPlanner` 已从 storage manager 中抽离目录扫描与恢复判断；可见集合必须构成 flushId 连续后缀，只自动修复 compact、最老 evict 和 boundary orphan 三类正常崩溃状态。
- retired SST 使用 data-first 删除且不额外 force 目录，data/meta 均成功前不移除 cleanup entry；启动可识别并清理 data 已删、meta 尚存的最老 evict 残留。
- WAL truncate 已按 `persistedSequenceId` / `maxSequenceId` 维度实现，并在 sink success 或 recovered prepare commit 后触发；删除失败的 WAL 仍保留在候选集合中，后续 truncate 可以重试。
- `SinkMetaPayloadCodec` 作为 SinkMeta 中 prepared/success payload 的内部二进制编解码器使用。

仍需后续补齐的部分：

- 服务层尚未形成定期补偿式 WAL truncate 调度；当前依赖 sink success / recovered prepare commit 后的即时触发。

## 10. 测试要求

实现该设计时至少补充以下测试：

- WAL 只 replay DATA，且 delete tombstone 正确恢复。
- flush boundary 未推进时 orphan SST 不注册。
- flush boundary fail-once 后在线重试复用同一个 SST，最终只存在一个 local run。
- flush boundary 已推进但 SST/SSTMeta 损坏时启动失败。
- SST 写入失败和 boundary orphan 重启都不消费 flushId。
- retired meta 删除 fail-once 后保留 cleanup entry 并可重试。
- data 已删、meta 尚存的最老 evict 残留可重启，保留后缀内部出现同类缺口则启动失败。
- compact output 完整而旧输入删除一半时选择 output 并清理输入垃圾。
- prepare meta 存在、success meta 不存在时恢复 commit。
- success meta 存在但 SSTMeta 仍为 NEW 时恢复为 SINKED。
- durable success 后本地 metadata fail-once 时进入 `FINALIZING`，在线重试后只存在原 Paimon commit。
- 多个 SSTMeta 标记中途失败时，已经写成 SINKED 的文件和仍为 NEW 的文件可由同一 finalization 幂等收敛。
- WAL truncate 只删除 `maxSequenceId <= persistedSequenceId` 的非当前 WAL 文件。
- 人工改坏 metadata checksum 时拒绝加载或降级为 orphan，行为需要按是否越过 boundary 区分。
