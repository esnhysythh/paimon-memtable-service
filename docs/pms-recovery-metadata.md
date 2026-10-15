# PMS Recovery Metadata 设计文档

## 1. 职责划分

PMS V1 使用独立的 DATA WAL、SSTMeta、flush boundary 和 SinkMeta 恢复本地状态及 Paimon 提交进度：

```text
Data WAL  = 用户数据变更事实
SSTMeta   = 本地 SST 物化进度与文件元信息
SinkMeta  = Paimon prepare/commit 进度与外部持久化边界
```

WAL 保持顺序追加的数据日志；低频的 Flush/Sink 进度通过可读、可校验、原子更新的 metadata 文件保存，便于独立验证恢复边界与排障。

## 2. 恢复边界

- WAL 只记录数据变更，不记录 flush/sink 控制事件。
- `lastFlushedSequenceId` 决定哪些 WAL DATA 已由本地 SST 承载，无需再次回放。
- WAL 回收使用 `SinkMeta.success.persistedSequenceId`，不使用 Paimon `snapshotId`。
- SST 数据文件名发布后保持稳定，不包含 `NEW` / `SINKED` 状态；metadata 保存 run 结构，启动时按 `persistedSequenceId` 推导状态。

server 在打开这些组件前加载或创建 `storage/commit-user`，持久化本地状态对应的 Paimon writer 身份。恢复 prepared Sink 沿用该身份；全新本地状态使用新身份以隔离重置后的 sequence。此文件由 server 管理，不进入 core 恢复协议，详见 [pms-server.md](pms-server.md) § 2.2。

## 3. Data WAL

### 3.1 职责

WAL 保存用户数据变更，恢复时用于重放尚未被本地 SST 覆盖的 DATA。SST 物化状态、Paimon prepare/commit 进度和 snapshot 信息由对应的 metadata 管理，不写入 DATA WAL。

### 3.2 应用层记录格式

每个逻辑记录包含一个 DATA batch，整数采用 little-endian：

```text
recordType(1, 0x42) + sequenceBegin(8) + count(4)
重复 count 次：keyLen(4) + key(N) + valueLen(4) + value(M)

valueLen >= 0 -> put/upsert，value 为 opaque bytes
valueLen = -1 -> delete/tombstone，无 value bytes
```

batch 内 sequence 连续分配，batch 之间只要求单调递增。恢复先验证完整 batch 的类型、count、长度、sequence 和剩余字节，再交给回调；不兼容旧的无类型单条 DATA 编码。core 不解析 Paimon 行内容。

物理层使用 32 KiB block 和 CRC32C，校验覆盖 chunk type 与 payload。每文件的首条 FULL 记录为 `PMS\0(4) + reserved(8, 必须为 0) + lastSequenceId(8)`，共 20 字节；用于承接已回收文件的 sequence 水位。

### 3.3 Replay 规则

启动恢复时：

```text
load SSTMeta + flush boundary
replay WAL DATA where sequenceId > lastFlushedSequenceId
```

`sequenceId <= lastFlushedSequenceId` 的 DATA 记录不再回放到 `curMemTable`，但仍须通过 WAL 结构与 CRC 校验。

只有最新 WAL 在有效文件头之后的未完成 DATA 尾部、以及符合格式的全零预分配后缀可被丢弃；其余已检测到的损坏拒绝启动。完整记录 CRC 错误没有尾部豁免。接受的尾部先截到最后完整逻辑记录并 force，再创建后继 writer，使再次重启仍能恢复。

零字节/不完整文件头统一失败，V1 不自动推断创建中断、不实现跳坏记录或时间点抢救。不逐次 fsync 的 DATA 仅提供既定进程恢复能力；完整文件或有效后缀被删除可能无法检测。详细规则见 [pms-core.md](pms-core.md) § 3.3。

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

启动恢复流程：

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

3. 校验并恢复 WAL（单次顺序扫描）
   - 检查文件名、连续编号、CRC、文件头、完整 DATA batch 与跨文件水位
   - 最老 header 水位 <= max(flushed, persisted) <= WAL 恢复水位
   - 暂存 sequenceId > lastFlushedSequenceId 的 DATA；其他 DATA 仍执行完整校验
   - 只容忍最新有效文件头后的未完成 DATA/合法零后缀
   - 全部输入通过验证后，截断允许的尾部并 force；失败不创建后继 WAL
   - 设置 allocator 并创建带可信水位的新 writer

4. 将暂存 DATA 回放到 curMemTable
   - sequenceId <= lastFlushedSequenceId 的记录不重复应用

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

WAL 回收使用 sink success metadata 中的 `persistedSequenceId`：

```text
walFile.highWatermark <= latestPersistedSequenceId
```

附加约束：

- 按编号升序只删除安全前缀；遇 current、首个不可回收文件或首个删除失败停止。
- `highWatermark` 为文件头水位与完整 DATA sequence 的最大值，合法 header-only/水位 0 的文件也参与回收。
- current 文件永不删除，保留 sequence 水位；失败候选留待后续重试。
- `snapshotId` 可用于确认外部 Paimon 状态，但不应替代 PMS 内部 sequence 边界。

## 8. 崩溃矩阵

| 场景 | 持久化结果 | 恢复行为 |
|------|------------|----------|
| DATA WAL 写入前崩溃 | 无 WAL DATA | 写入未成功，不恢复 |
| 最新 DATA batch 未写完 | 有效文件头和完整记录前缀、末条半写 | 丢弃整个末 batch，截断并 force 后再开启新 writer |
| 新 WAL 文件头未写完 | 零字节或无效文件头 | 启动失败，人工介入 |
| 完整 WAL 记录 CRC 错误 | 可检测的损坏，包括最新末条 | 启动失败，不跳过记录 |
| WAL 回收删除失败 | 安全前缀删除停止，后继文件保留 | 下次回收重试，不影响已确认的 Sink |
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

- WAL 只写 `0x42 DATA_BATCH`；恢复统一校验完整 batch，仅容忍最新 DATA 尾部，并在创建 writer 前整理尾部。
- SST 文件写出后会同步写独立、人工可读的 `sst-*.meta.json`，并在启动时与 SST properties 交叉校验。
- Sink prepare/success 写入独立 `SinkMetaStore`，文件为可读 metadata，并带校验字段。
- `SinkCoordinator` 通过 `SinkMetaStore` 持久化 prepare/success。
- 启动恢复通过扫描 SinkMeta 恢复 pending prepare，并用 success meta 推导 sinked SST。
- 运行期 durable success 之后的本地收尾失败会进入 `FINALIZING`，由 Maintenance 最高优先级重做；该路径只读取原 success metadata，不创建新 Paimon commit。
- Flush boundary 已使用独立 `flush-boundary.meta`。
- 运行期 flush boundary 写入失败时，非持久化 FlushFlight 会复用已经发布的 SST 完成重试；进程崩溃后仍按 orphan SST + WAL replay 恢复。
- `SSTRecoveryPlanner` 已从 storage manager 中抽离目录扫描与恢复判断；可见集合必须构成 flushId 连续后缀，只自动修复 compact、最老 evict 和 boundary orphan 三类正常崩溃状态。
- retired SST 使用 data-first 删除且不额外 force 目录，data/meta 均成功前不移除 cleanup entry；启动可识别并清理 data 已删、meta 尚存的最老 evict 残留。
- WAL truncate 按 `persistedSequenceId` / `highWatermark` 删除安全前缀，在 sink success 或 recovered prepare commit 后触发；遇不可回收项或删除失败停止，失败候选可重试。
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
- WAL truncate 只删除 `highWatermark <= persistedSequenceId` 的非当前安全前缀，包含合法空头文件。
- 最新尾部半写恢复后再次重启成功；完整 CRC 错、旧文件半写、无效头、文件缺口及 I/O 失败均拒绝启动。
- FileChannel 短写补全，读取错误与异常关闭不能转成 EOF；roll force 失败传播到 fatal。
- 人工改坏 metadata checksum 时拒绝加载或降级为 orphan，行为需要按是否越过 boundary 区分。
