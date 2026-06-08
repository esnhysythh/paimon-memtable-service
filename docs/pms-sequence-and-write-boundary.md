# PMS Sequence 与写入边界设计说明

## 1. 背景

PMS 的写入路径包含 WAL、CurMemTable、ImmutableMemTable、Flush、Sink 和 WAL Truncate 多个阶段。只要其中任一阶段错误推进边界，就可能出现“系统认为数据已经安全进入 Paimon，但实际数据仍只存在于内存或 WAL 中”的丢数风险。

本设计说明记录 sequence、freeze 边界、写入锁粒度和 WAL 优化方向。它不是单个类的 API 文档，而是用于解释 PMS 为什么采用当前写入边界模型，以及未来优化时哪些约束不能破坏。

## 2. 核心问题

### 2.1 Freeze 不是简单的 Map Swap

早期 `SkipListCurMemTable.freeze()` 通过替换内部 `ConcurrentSkipListMap` 引用完成冻结：

```text
oldMap = current map
current map = new empty map
immutable = oldMap
```

这个操作本身很快，但如果写线程已经拿到旧 map 引用，freeze 后仍可能把数据写入旧 map。只要 Flush 线程已经扫描过该位置，数据就可能漏进本次 SST。更危险的是，后续 Sink/WAL Truncate 可能把该边界之前的数据标记为已提交，从而造成永久丢数。

### 2.2 Sequence 解决的是边界坐标问题

轻量级 sequence 的目标不是一开始就实现 MVCC，而是给每条写入一个内部逻辑时钟：

```text
seq=101 put k1=v1
seq=102 delete k2
seq=103 put k1=v2
```

有了 sequence，系统可以明确表达：

```text
ImmutableMemTable 覆盖 seq 1..5000
SST 覆盖 seq 1..5000
Paimon Sink 成功覆盖到 persistedSequenceId=5000（已持久化 sequence 边界）
WAL 文件 maxSequenceId <= 5000 时才可安全截断
```

这比“某个 map 已经 freeze”“某个 snapshotId 已存在”更适合作为 PMS 内部安全边界。

### 2.3 Sequence 字段本身不保证正确

sequence 只有在以下三个动作形成原子提交边界时才可靠：

```text
1. 分配 sequenceId
2. 写入 WAL
3. 写入 MemTable 并更新 MemTable sequence 边界
```

如果 freeze 可以与这三个动作并发交错，ImmutableMemTable 的 `minSequenceId/maxSequenceId` 仍可能与实际 map 内容不一致。也就是说，sequence 不是替代同步的魔法字段；它必须配合正确的写入边界协议。

## 3. LevelDB Java 版本的参考

本项目参考了 `/Users/qinwenhao/workspace/leveldb` 中的 Java LevelDB 移植版。

### 3.1 写入提交路径

`DbImpl.writeInternal()` 的核心流程是：

```text
mutex.lock()
  makeRoomForWrite()
  sequenceBegin = lastSequence + 1
  sequenceEnd = sequenceBegin + batch.size - 1
  versions.setLastSequence(sequenceEnd)
  log.addRecord(batch, sync)
  batch.insertInto(memTable, sequenceBegin)
mutex.unlock()
```

对应源码位置：

```text
/Users/qinwenhao/workspace/leveldb/leveldb/src/main/java/org/iq80/leveldb/impl/DbImpl.java
```

关键点：

- LevelDB Java 版并不放任多个写线程无序写 MemTable。
- sequence 分配、WAL 写入、MemTable 写入在同一把 DB mutex 下完成。
- 这把锁保护的是“提交边界”，不是后台慢任务。

### 3.2 MemTable 切换路径

当 MemTable 满时，`makeRoomForWrite()` 在同一把 mutex 内完成：

```text
close old log
open new log
immutableMemTable = memTable
memTable = new MemTable(...)
schedule compaction
```

这保证了旧 MemTable 的边界与 WAL/sequence 顺序一致。写线程不会在边界切换中处于“WAL 已写但 MemTable 归属未定”的状态。

### 3.3 后台 Flush/Compaction 不持写锁

LevelDB 在真正构建 SST 时释放 mutex：

```text
pendingOutputs.add(fileNumber)
mutex.unlock()
  buildTable(mem, fileNumber)
mutex.lock()
```

这说明经典 LSM 设计不是“完全无锁”，而是：

```text
提交边界短锁
后台慢 IO 不持锁
```

PMS 的写入边界应接近这个锁粒度，而不是为了避免锁而牺牲边界正确性。

## 4. PMS 的写入边界模型

### 4.1 V1 推荐模型

PMS Bucket 内部应引入写入提交锁，保护以下路径：

```text
put/delete:
  lock writeMutex
    ensureNotClosed
    append DATA to WAL and get sequenceId
    write curMemTable with Value(sequenceId)
    maybeFreezeLocked()
  unlock
```

显式 freeze 也使用同一把锁：

```text
freezeCurMemTable:
  lock writeMutex
    ensureNotClosed
    doFreezeLocked()
  unlock
```

这与 LevelDB Java 版的锁粒度接近：WAL、sequence、MemTable 可见顺序、freeze 切换同属提交边界。

### 4.2 不应持锁的路径

以下操作不应持有写入提交锁：

- Flush ImmutableMemTable 到 SST。
- Sink SST/ImmutableMemTable 到 Paimon。
- Paimon commit。
- 本地 SST compaction。
- 文件删除、淘汰、长时间 IO。

这些路径只能消费已经冻结并带有 sequence 边界的 immutable/SST 元数据。

### 4.3 CurMemTable 删除语义

引入 sequence 后，删除不能再通过无 sequence 的静态 `Value.TOMBSTONE` 表达；该常量应移除。统一语义应为：

```java
curMemTable.put(key, Value.tombstone(sequenceId));
```

因此 `CurMemTable.delete(Key)` 不应继续作为新写入路径 API 存在，除非它显式接收 `sequenceId`。

PMS 的 `Value` 不是普通 KV 系统里的任意 byte value，而是序列化后的 Paimon `InternalRow`。因此：

- `Value.bytes == null` 只表示删除 tombstone。
- 非 tombstone 的 `Value.bytes` 应由 RowCodec/序列化管理器生成，表示完整的 `InternalRow` 编码。
- 即使业务列全部为 `NULL`，编码结果也应包含格式头、字段数量、null bitmap 等元信息，设计语义上不应是空 `byte[]`。
- 当前 V1 底层字节接口暂不负责校验 `byte[]` 是否是合法行编码；该校验应在后续 RowCodec/序列化管理器接入后完成。

## 5. WAL 与 Sequence

### 5.1 DATA 记录

PMS 尚未发布，不需要兼容旧 WAL 格式。V1 的 `DATA(type=0x00)` 直接包含 sequence：

```text
type(1) + sequenceId(8) + keyLen(4) + key + valueLen(4) + value
```

其中 `valueLen = -1` 表示 delete/tombstone；`valueLen > 0` 表示 serialized `InternalRow`；`valueLen = 0` 不表示 tombstone 或业务 NULL，后续 RowCodec 接入后应视为非法或保留编码。

### 5.2 WAL 文件头保存 Sequence 水位

WAL 文件头保存文件创建时的 `lastSequenceId`。这样即使旧 WAL 文件被截断，而当前 WAL 文件暂时只有控制记录或为空，重启后仍能恢复全局 sequence 水位，避免从 1 重新开始分配。

### 5.3 Sequence 允许有空洞

sequence 是单调边界坐标，不是连续行号。若某次 WAL 写入失败、进程继续运行，可能出现跳号。只要后续 sequence 单调递增，就不会破坏边界判断。

因此设计要求是：

```text
必须单调递增
不要求无空洞
不得重复分配已成功写入 WAL 的 sequence
```

## 6. WAL Truncate 与 Persisted Sequence

仅有 per-record sequence 还不足以安全截断 WAL。截断需要知道“Paimon 已经持久化包含到哪个 PMS sequence”。本文档使用 `persistedSequenceId` 表示这个已持久化的 PMS 内部边界。

当前 SST 阶段先引入一个更早的本地恢复边界：`lastFlushedSequenceId`。它只表示“本地 SST 已经覆盖到哪个 sequence”，用于重启时跳过已经由 SST 承载的 WAL DATA 记录；它不表示数据已经进入 Paimon，也不能用于最终 WAL 截断。

```text
flush immutable -> SST 原子落盘成功
  -> 原子写入 storage/flush-boundary.meta(lastFlushedSequenceId = sst.maxSequenceId)
  -> 从 immutable 列表移除并加入 newSST 列表

restart:
  load SSTs
  load lastFlushedSequenceId
  replay WAL DATA where sequenceId > lastFlushedSequenceId
```

若 SST 已落盘但边界文件尚未写入就崩溃，重启会重放更多 WAL 记录。这是安全的，代价只是恢复后 curMemTable 中临时包含已 flush 数据的重复表达。若边界文件已经写入，则对应 SST 成为恢复正确性的组成部分；启动时如果发现已持久化 flush 边界但 SST 损坏，应失败而不是静默跳过，以避免边界跳过 WAL 后丢失数据。

Sink 成功元信息需要包含类似：

```text
SinkSuccessMeta(snapshotId, batchId, persistedSequenceId, sstIds)
```

当前实现中，Sink prepare/success 不再写入 WAL，而是写入独立 `SinkMeta` 文件。prepare meta 记录本次 sink 覆盖的 `sstIds`、`minSequenceId/maxSequenceId`、prepared commit payload 与外部 file refs；success meta 确认 `persistedSequenceId` 与 `sstIds`。SST 是否 sinked 由 success meta 中的 `sstIds` 与 `persistedSequenceId` 推导，不再依赖 WAL 控制记录。详见 [pms-recovery-metadata.md](pms-recovery-metadata.md)。

安全截断条件应变为：

```text
walFile.maxSequenceId <= persistedSequenceId
```

`snapshotId` 只能证明 Paimon 外部提交存在，不能单独表达 PMS 内部覆盖边界；因此 WAL 文件删除应以 PMS 内部 `persistedSequenceId` 判断。

## 7. WAL 写入性能与业界优化

### 7.1 当前瓶颈

WAL 写入天然是写路径的顺序点。Java LevelDB 的 `LogWriter.addRecord()` 本身也是 synchronized；DB 层还用 mutex 串行化 sequence/WAL/MemTable 提交。

PMS 当前阶段接受短提交锁，是为了保证边界正确性。它不应显著低于 Java LevelDB 的锁模型，因为两者在提交路径上的锁粒度相近。

### 7.2 常见优化方向

后续如果 WAL 成为瓶颈，可考虑：

- **Writer Queue**：写线程入队，一个 leader 负责批量提交，followers 等待结果。
- **Group Commit**：多个写请求合并成一个 WAL batch，减少系统调用和 fsync 次数。
- **Batch Sequence Allocation**：一次为 batch 分配连续 sequence 范围。
- **Parallel MemTable Writer**：WAL 顺序确定后并行写 MemTable，但必须有严格发布协议，保证 freeze 只能看到完整 batch 边界。
- **Async Fsync / Sync Policy**：普通写只 append，按策略或控制记录 force；强一致写才同步 force。

这些优化不能改变一个约束：WAL 顺序、sequence 顺序、MemTable 可见顺序、freeze 边界必须可证明一致。

### 7.3 V1 当前写入队列优化决策

当前实现先采用保守的 `Writer Queue + Natural Group Commit`：

```text
put/delete:
  create WriteRequest
  enqueue
  wait request.done

leader:
  drain 已经排队的请求形成 batch
  append 一个 WAL batch record
  按 batch 顺序串行写 curMemTable
  batch 结束后检查并执行 maybeFreeze
  唤醒 batch 内所有同步等待的调用方
```

关键决策：

- `put/delete` 仍保持同步语义：调用返回时，该请求已经完成 WAL append 且在 MemTable 中可见。
- 第一版不主动等待 coalesce window；leader 只 drain 当前已经排队的请求。这样单线程循环写不会因为空等聚合窗口而退化。
- batch 内 MemTable apply 暂时保持串行。PMS 当前 MemTable 是 `userKey -> latest Value`，不是 LevelDB 的 `(userKey, sequenceId)` internal key；若并发 apply，同一 key 的低 sequence 写入可能后完成并覆盖高 sequence，造成旧值复活。
- freeze 只在完整 batch apply 后触发，避免一个 WAL batch 被 freeze 切成半个可见边界。
- 后续若要引入主动 coalesce window，应作为可配置项并默认关闭，先用 benchmark 比较吞吐与 p99 延迟。

若未来进一步做 `Parallel MemTable Writer`，必须先补齐 sequence-aware update 与 batch publish barrier：

```text
same-key update:
  仅允许更大的 sequenceId 覆盖更小的 sequenceId

freeze:
  必须等待当前 active batch 的全部 memtable apply 完成
```

否则会破坏 Deduplicate 语义和 WAL/SST 边界一致性。

## 8. 当前实现约束与后续 TODO

当前阶段的约束：

- V1 使用轻量 sequence，不实现 MVCC。
- 同一 Key 仍只保留 latest value。
- 写入提交发布路径仍按 batch 串行化，flush/sink 慢路径不持写锁。
- sequence 可有空洞，但必须单调。
- `lastFlushedSequenceId` 只用于 SST/WAL 本地恢复边界，不用于 Paimon sink 成功判定。

后续 TODO：

- 移除或改造 `CurMemTable.delete(Key)`，避免无 sequence tombstone。
- 为 WAL truncate 增加定期补偿式后台调度，避免只依赖 sink success 后的即时触发。
- 将当前 BucketDirector 内部 writer queue 抽出为 WriteCoordinator。
- 评估可配置 coalesce window，默认保持关闭，避免单线程写入空等退化。
- 若未来需要 MVCC，将 MemTable/SST key 形态升级为 `(userKey, sequenceId)` 并引入 read sequence 可见性过滤。
