# PMS Core Local Run 与 SST 合并设计

## 1. 背景

PMS core 的本地 SST 不宜按 RocksDB/LevelDB 的传统 LSM level 来理解。PMS 是 Paimon 表的本地最新数据窗口，本地数据会在确认写入 Paimon 后按最老优先退役，因此本地结构的核心约束不是 level size ratio，而是：

- 查询必须优先看到最新写入。
- Sink 必须只处理尚未写入 Paimon 的数据范围。
- 淘汰必须只淘汰已经 sink 且最老的一段数据。
- 合并后仍要保留可平滑退役的逻辑边界。

因此，PMS 本地 SST 抽象为按 flush 顺序排列的 `local run`。一个 local run 可以是一次 MemTable flush 直接生成的 SST，也可以是多个连续 local run compact 后生成的 SST。

## 2. 术语

| 术语 | 含义 |
|------|------|
| `flushId` | 每次 ImmutableMemTable flush 生成 SST 时分配的递增逻辑段号。它表达数据进入本地 SST 队列的批次顺序。 |
| `local run` | 覆盖一个连续 `flushId` 范围的本地有序数据段，物理上由一个 SST 文件承载。 |
| `minFlushId/maxFlushId` | local run 覆盖的闭区间 flush 范围。未 compact 的 run 满足 `minFlushId == maxFlushId`。 |
| `runId` | 物理文件唯一标识，仅用于文件定位、reader cache、删除和排障。它不表达逻辑新旧顺序。 |
| `new run` | 尚未 sink 到 Paimon 的 local run。 |
| `sinked run` | 已确认 sink 到 Paimon 的 local run，可作为本地最新数据副本，后续可被淘汰。 |

`sequenceId` 仍然保留：它是单条写入的顺序坐标，用于 WAL 边界、同 key 最新值选择，以及 compact/scan 时的 deduplicate 判断。`flushId` 是 SST/run 生命周期的顺序坐标，用于组织、sink、compact 和 evict。

## 3. 元数据模型

当前实现中，`SSTMeta` 不再使用 `fileId` 表达逻辑顺序。核心结构为：

```java
record SSTMeta(
    long runId,
    long minFlushId,
    long maxFlushId,
    Path path,
    long fileSize,
    long entryCount,
    Key minKey,
    Key maxKey,
    long minSequenceId,
    long maxSequenceId,
    long oldestWriteAtMillis,
    long createdAtMillis,
    SSTState state
) {}
```

关键约束：

- 可见 local run 的 `[minFlushId, maxFlushId]` 之间不允许重叠。
- 同一状态队列内，只允许存在连续或有明确缺口的 run；compact 只能选择连续范围。
- 逻辑新旧顺序按 `maxFlushId` 判断，而不是按 `runId`、文件名或创建时间判断。
- `runId` 可以自增，也可以使用其他唯一生成方式；它只服务物理文件管理。

## 4. 文件命名

SST 文件名只表达稳定的 `minFlushId/maxFlushId` 范围。当前实现会解析文件名中的范围辅助目录扫描、孤儿文件识别和 meta 文件排序，但不能依赖文件名判断可靠业务状态。恢复和状态判断必须以 `SSTMeta` 与 SinkMeta 为准。

推荐文件名格式：

```text
sst-{minFlushId}-{maxFlushId}.sst
```

实际落盘建议使用固定宽度十进制补零，便于目录中按名称观察：

```text
sst-000001-000001.sst
sst-000002-000004.sst
sst-000001-000010.sst
```

命名规则说明：

- 未 compact 的 SST 使用相同的起止 flushId，例如 `sst-000010-000010.sst`。
- compact 输出使用覆盖后的 flushId 范围，例如 `[11,13]` 输出为 `sst-000011-000013.sst`。
- `NEW` / `SINKED` 写入 `sst-*.meta.json`; 数据文件 publish 后不再因为状态迁移 rename。
- 临时文件和未发布文件可以带 `runId`、随机后缀或 `.tmp` 后缀，避免和可见文件冲突；它们不属于可观测稳定命名。

## 5. 查询顺序

点查路径保持从新到旧穿透：

```text
curMemTable
immutableMemTables
newRuns    按 maxFlushId 倒序
sinkedRuns 按 maxFlushId 倒序
pms-lookup-paimon 历史数据点查
```

因为 PMS 使用 deduplicate latest-state 语义，点查遇到首个 key 命中即可停止；如果命中 tombstone，也必须停止后续历史数据查询，避免旧值从更老 run 或 Paimon 中复活。

Range / prefix scan 不能只返回首个命中层。它必须遍历所有本地层，并按 `Value.sequenceId` 为每个 key 选择最新 entry；最新 entry 是 tombstone 时不返回该 key。

## 6. Sink 边界

SinkMeta 不应长期绑定具体 SST 物理文件。更稳的模型是绑定 flush 边界。

长期设计中，如果 Sink 始终按连续 new run 从老到新提交，Sink success 可记录：

```text
persistedFlushId
persistedSequenceId
```

恢复时：

```text
run.maxFlushId <= persistedFlushId -> sinked
run.minFlushId >  persistedFlushId -> new
```

如果出现 `run.minFlushId <= persistedFlushId < run.maxFlushId`，说明可见 run 跨越了 sink 高水位边界。这种状态不应由合法 compact 产生，恢复时应视为 metadata 不一致并拒绝启动。

如果未来允许非连续 sink，则需要记录 `sinkedFlushRanges`。V1 建议保持连续 sink，高水位模型更简单。

`persistedSequenceId` 仍然用于 WAL truncate；`persistedFlushId` 用于本地 run 生命周期判断。二者服务不同边界，不应互相替代。

第一阶段实现暂不改动 SinkMeta payload 结构，仍保留 `sstIds`，并额外使用 `persistedSequenceId` 修复 compact 后的 sinked 状态恢复：当 compact 后的新 `runId` 不在历史 `sstIds` 中，只要该 run 的 `maxSequenceId <= persistedSequenceId`，恢复时仍判定为 sinked run。后续引入 `persistedFlushId` 后，可把本地 run 生命周期判断从 sequence 高水位迁移到 flush 高水位。

## 7. Compact 规则

Local run compact 是对同一生命周期状态内的连续 run 做多路归并。

允许：

```text
NEW:    [11,11] + [12,12] + [13,13] -> [11,13]
SINKED: [1,3]   + [4,6]             -> [1,6]
```

禁止：

```text
SINKED [1,10] + NEW [11,12]
NEW    [11,11] + NEW [13,13]    // 中间缺少 [12,12]
```

Compact 输出约束：

- 输出 run 的 `minFlushId` 等于输入最小值。
- 输出 run 的 `maxFlushId` 等于输入最大值。
- 输出 run 的 `state` 继承输入 state。
- 输出 run 的 `minSequenceId/maxSequenceId` 覆盖所有输入 sequence 范围。
- 同一 key 在多个输入中出现时，保留 `sequenceId` 最大的 entry。
- tombstone 与普通 PUT 一样参与归并，并可成为输出 entry。
- V1 不做 tombstone GC；即使更老数据已经在 Paimon 中，也不在本地 compact 中丢弃 tombstone。

发布 compact 结果时，应以元数据切换为准：新 run data、meta 与经过完整校验的 cached reader 均在可见视图 monitor 外准备，随后在短临界区内一次替换旧 run。查询只能观察到带完整 reader 的旧集合或新集合，不在 cache miss 时同步重新打开并扫描 SST。

当前实现通过 read epoch 避免并发查询读到已删除文件：查询、scan、sink 和 compact 读取 SST 前会创建 `SSTReadSnapshot` 并注册当前 epoch；compact 发布新 run 或 evict 移除旧 run 后, 旧 run 进入 retired queue。只有当所有活跃 snapshot 的最小 epoch 已经不早于旧 run 的 `retireEpoch` 时, storage 才关闭旧 reader 并删除旧 data/meta 文件。启动恢复时，如果一个无 meta 的 SST 文件的 flush 范围已经被可见 run 连续覆盖，则忽略该 orphan；如果 compact 输出 meta 已写入但旧输入 meta 尚未删除就崩溃，恢复时选择覆盖范围更大的 compact run。

## 8. 淘汰规则

本地 SST 淘汰只作用于 sinked run，并且只淘汰最老 run。

最老判断使用 flush 范围：

```text
先按 maxFlushId 最小，再按 minFlushId 最小
```

一次淘汰的粒度就是一个 sinked run。因此 compact 的目标大小不能照搬传统 LSM 的逐层放大模型，而应接近 PMS 可接受的一次退役数据量。这样本地数据量变化更平滑。

## 9. 调度建议

V1 不设置独立的 `compactMinFiles` 或“小文件大小阈值”。调度只维护 NEW/SINKED 两个 run 数量水位：

- `NEW count > pms.storage.new_sst.max_count` 时，优先从老到新选择第一个至少包含两个 run、连续且总输入不超过 `pms.operation.compact.max_input_size_mb` 的 NEW 分组；不存在候选组时 Sink 最老 NEW 前缀。
- `SINKED count > pms.storage.sinked_sst.max_count` 时，以相同规则优先 compact SINKED；不存在候选组时淘汰最老 SINKED run。
- 每次只执行一个操作；取得进展后重新读取完整状态并从最高优先级判断。
- Sink、local compact 和 sinked evict 使用同一把本地 SST maintenance mutex 串行化，但不持有写入提交锁。
- active/prepared Sink 会保护其固定前缀，NEW compact 不得跨越该边界；SINKED compact 不受已完成 Sink 的限制。

该策略把单次 compact 的最大输出规模与 run 数量上限组合起来，近似约束本地窗口，同时避免为 MVP 引入总字节数、磁盘 free space 或查询放大反馈控制。后续有真实指标后，可再引入 BloomFilter 检查次数、点查 miss 放大等触发条件。

## 10. Sink 与 Compact 融合优化

Paimon sink 需要对多个 new run 做 streaming merge；本地 compact 也需要做相同方向的 ordered merge。因此，未来可以把 sink merge 和 compact output 写入融合成一次流水线，减少一次磁盘读写。

当前明确暂缓实现 sink+compact 融合。本阶段只实现 standalone local run compact，并保持 SinkBatch 仍直接引用待 sink 的 new run 列表。

该优化不作为第一版强约束。推荐演进顺序：

1. 先实现 standalone local run compact。
2. 再评估是否实现 sink 前 compact，SinkBatch 使用 compact 后的 new run。
3. 最后实现 sink merge 与 compact output 的融合流水线。

融合优化需要额外设计恢复语义：Paimon prepare/commit、compact output 发布、旧 run 删除之间必须有清晰的元数据状态机。该状态机也应基于 `flushId` 范围，而不是具体文件名。
