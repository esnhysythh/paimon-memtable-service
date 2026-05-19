# PMS Bucket Director 设计文档

## 1. 模块定位
`PMSBucketDirector` 是 `pms-core` 的总协调器，管理一条 Paimon 表数据从写入到最终落盘的完整生命周期。它不直接持有数据，而是协调 MemTableEngine、LocalStorageManager、WALManager、PaimonSinkManager 四个子组件的协作。

## 2. 核心职责
- 管理数据单元的状态机流转。
- 编排查询路径的多层穿透。
- 协调写入、冻结、刷盘、Sink、淘汰各阶段的前后依赖。
- 向上层（pms-server RPC）暴露统一的 `put` / `get` 接口。

## 3. 数据单元状态机

PMS 中的数据单元经历以下状态流转：

```
                    ┌─────────────┐
                    │   WRITE     │
                    └──────┬──────┘
                           ▼
               ┌───────────────────────┐
               │     curMemTable       │ ◄── get() 查询命中
               │     (Active/Writable) │
               └───────────┬───────────┘
                  freeze() │ (curMemTable 达阈值 或 手动触发)
                           ▼
          ┌────────────────────────────────┐
          │   ImmutableMemTable (new)      │ ◄── get() 查询命中
          │   等待 Flush                   │
          └────────────┬───────────────────┘
                  flush() │ (异步刷盘完成)
                          ▼
          ┌────────────────────────────────┐
          │   newSST + ImmutableMemTable   │ ◄── get() 查询命中
          │   (newSSTWithMem)             │     (SST + Mem 双重保证)
          └────────────┬───────────────────┘
                  sink() │ (触发 Sink Paimon)
                         ▼
          ┌────────────────────────────────┐
          │   sinkedSST + ImmutableMemTable│ ◄── get() 查询命中
          │   (sinkedSSTWithMem)          │
          └────────────┬───────────────────┘
                mem退役 │ (ImmutableMemTable 引用计数归零 或 内存不足主动退化)
                         ▼
          ┌────────────────────────────────┐
          │   sinkedSST                    │ ◄── get() 查询命中
          │   等待淘汰                     │
          └────────────┬───────────────────┘
                evict() │ (最老优先淘汰)
                         ▼
                      [删除]
```

### 3.1 状态说明

| 状态 | 内存数据 | 磁盘数据 | 可查询 | 可写入 | 说明 |
|------|---------|---------|--------|--------|------|
| curMemTable | SkipList（可变） | 无 | 是 | 是 | 达到容量阈值或时间阈值触发 Freeze |
| ImmutableMemTable (new) | SkipList（只读） | 无 | 是 | 否 | Flush 完成后转为 newSSTWithMem |
| newSSTWithMem | SkipList（只读） | newSST | 是 | 否 | Sink 完成后转为 sinkedSSTWithMem |
| sinkedSSTWithMem | SkipList（只读） | sinkedSST | 是 | 否 | Mem 引用计数归零或内存不足时退化为 sinkedSST |
| sinkedSST | 无 | sinkedSST | 是 | 否 | 最老且无查询引用时淘汰 |

### 3.2 双持状态的性质

**newSSTWithMem 和 sinkedSSTWithMem 中的 Mem 缓存是纯性能优化，不是正确性要求。**

保留 Mem 缓存的原因：PMS 必须有基于时间阈值的 Sink 操作，在数据量较小时 MemTable 可能还没积攒足够数据就被 Freeze。如果每次 Flush 后立即退役 Mem，PMS 内存中几乎没数据，"内存缓存"功能会被严重削弱。双持保证内存中保留更多缓存数据。

**退化规则**——以下情况可将双持状态退化为不带 Mem 的状态：
- 内存不足，需要回收 Mem 缓存空间。
- Mem 缓存的引用计数已归零。
- 合并时内存不足以同时持有新旧 SkipList。

退化操作是单向的（有 Mem → 无 Mem），不可逆。

### 3.3 状态流转的原子性保证

每个状态流转操作必须是原子的，确保查询路径不会看到中间状态：

- **freeze**：`curMemTable` 引用用 volatile 修饰，原子切换为新的空 MemTable，原 MemTable 标记为 immutable 并加入 `newImmutableList`。
- **flush**：Flush 完成后，将对应 ImmutableMemTable 从 `newImmutableList` 移至 `newSSTWithMemList`，同时注册 newSST 的 BloomFilter。
- **sink**：Sink 完成后，将 newSSTWithMem 条目批量移至 `sinkedSSTWithMemList`。
- **mem退役**：ImmutableMemTable 的引用计数归零后，从对应列表中移除，只保留 SST 引用。
- **evict**：淘汰最老的 sinkedSST，从列表移除并删除磁盘文件（需确认无查询引用）。

## 4. 查询穿透

`get(key)` 按以下顺序穿透，命中即返回：

```
1. curMemTable                    ── 命中概率最高，延迟最低
2. newImmutableMemTables (倒序)   ── 新数据优先
3. sinkedImmutableMemTables (倒序)
4. newSSTs (BloomFilter 加速)     ── 磁盘读取
5. sinkedSSTs (BloomFilter 加速)  ── 磁盘读取
6. Paimon 穿透查询                ── 最慢，基于 Manifest 索引定位
```

**查询穿透过程中的并发**：初期方案为直接遍历 volatile 列表，不做快照拷贝。层列表通过 volatile 引用替换整个列表，查询线程不会看到半更新状态。详见 [pms-core.md](pms-core.md) § 5.2。

## 5. 接口定义

```java
interface PMSBucketDirector {

    // ── 写入 ──
    void put(byte[] key, byte[] value);

    // ── 查询 ──
    Optional<byte[]> get(byte[] key);

    // ── 状态流转触发 ──
    void freezeCurMemTable();
    void flushImmutableMemTable();
    void sinkToPaimon();
    void evictOldestSinkedSST();

    // ── 本地 SST 合并 ──
    void compactLocalSSTs();

    // ── Mem 缓存退化 ──
    void degradeMemCache();  // 将最早的双持状态退化为不带 Mem

    // ── 状态快照（供管理接口使用）──
    BucketStateSnapshot stateSnapshot();
}
```

### 5.1 BucketStateSnapshot

```java
record BucketStateSnapshot(
    int curMemTableEntryCount,
    long curMemTableSizeBytes,
    int immutableMemTableCount,
    long immutableMemTableTotalBytes,
    int newSSTCount,
    long newSSTTotalBytes,
    int sinkedSSTCount,
    long sinkedSSTTotalBytes,
    int withMemCount,               // 当前双持状态的条目数
    long withMemTotalBytes,         // 双持 Mem 缓存的总内存占用
    long lastSinkedSnapshotId
) {}
```

## 6. 冻结与刷盘策略

### 6.1 Freeze 触发条件（满足任一即触发）

- curMemTable 条目数达到 `PMSConfig.memtableMaxEntries`（默认 100 万）。
- curMemTable 内存占用达到 `PMSConfig.memtableMaxSizeMb`（默认 256MB）。
- 流控 OVERLOADED 水位下主动触发。

### 6.2 Flush 策略

- Freeze 后立即异步提交 Flush 任务。
- Flush 任务将 ImmutableMemTable 序列化为 SST 文件（自定义行存格式，参见 [pms-core.md](pms-core.md) § 3.2）。
- Flush 期间，新的写入继续进入新的 curMemTable，不阻塞。

### 6.3 并发限制

- 同一时刻最多 1 个 Flush 任务在执行（单磁盘写串行化，避免 IO 竞争）。
- 如果上一个 Flush 未完成，新 Freeze 的 ImmutableMemTable 排入等待队列。

## 7. Sink 编排

### 7.1 Sink 触发条件

- newSST 数量达到 `PMSConfig.sinkMaxPendingSsts`（默认 8）。
- 距上次 Sink 间隔超过 `PMSConfig.sinkIntervalMs`（默认 30s）。
- 流控 OVERLOADED 水位下提前触发。

### 7.2 Sink 流程（与 PaimonSinkManager 协作）

```
PMSBucketDirector                        PaimonSinkManager
     │                                          │
     │  1. 收集所有 newSST + ImmutableMemTable   │
     │──────────────────────────────────────────►│
     │                                          │
     │  2. 合并 newSST + MemTable → preSink      │
     │     (归并排序，保留最新 Key)               │
     │──────────────────────────────────────────►│
     │                                          │
     │  3. Paimon prepareCommit → CommitMessage  │
     │◄──────────────────────────────────────────│
     │                                          │
     │  4. WALManager 写入 SINK_PREPARE          │
     │──────────────────────────────────────────►│
     │                                          │
     │  5. Paimon commit → new Snapshot          │
     │◄──────────────────────────────────────────│
     │                                          │
     │  6. WALManager 写入 SINK_SUCCESS          │
     │──────────────────────────────────────────►│
     │                                          │
     │  7. 更新内部状态: new → sinked            │
     │                                          │
```

### 7.3 Sink 失败处理

- **prepareCommit 失败**：重试最多 3 次。仍失败则标记 Sink 异常，上报告警，不继续 commit。
- **commit 失败**：依赖 WAL 中的 `SINK_PREPARE` 记录，重启后重试 commit（参见 [pms-core.md](pms-core.md) § 5.4）。
- Sink 失败期间，新的 Freeze 和 Flush 仍可正常进行，写入路径不受影响。只是 newSST 会堆积，可能导致水位上升至 OVERLOADED。

## 8. 淘汰策略

### 8.1 ImmutableMemTable 退役

- 每个 ImmutableMemTable 维护引用计数 `refCount`。
- 查询时进入该层前 `refCount++`，离开时 `refCount--`。
- 退役条件：ImmutableMemTable 处于 sinkedSSTWithMem 状态，且 `refCount` 归零，且不是最新两个（避免刚 Sink 完就退役）。
- 内存不足时可通过 `degradeMemCache()` 主动退役：选择最早的双持条目，释放其 Mem 缓存，状态退化为 sinkedSST。

### 8.2 sinkedSST 淘汰

- **只淘汰最老**：保证 PMS 缓存中永远是最新数据，规避幽灵数据问题。
- 淘汰前确认：无查询引用该 SST（引用计数机制同 ImmutableMemTable）。
- 淘汰时：删除磁盘 SST 文件，从列表移除。
- 触发条件：sinkedSST 总大小超过 `PMSConfig.storageSinkedMaxSizeMb`，或文件数超过 `PMSConfig.storageSinkedMaxCount`。

### 8.3 SST 合并策略

合并的目标是减少文件数量、提升查询效率。合并不限于"淘汰前"，可以随时触发。

#### 8.3.1 合并适用范围

| 合并来源 | 合并结果 | 说明 |
|----------|---------|------|
| newSSTWithMem × N | newSSTWithMem | SST 归并 + SkipList 归并 |
| sinkedSSTWithMem × N | sinkedSSTWithMem | SST 归并 + SkipList 归并 |
| sinkedSST × N | sinkedSST | 仅 SST 归并 |

不同类别之间不合并（new 和 sinked 生命周期不同，不混并）。

#### 8.3.2 合并时的内存缓存处理

对于带 Mem 缓存的合并（newSSTWithMem / sinkedSSTWithMem）：

1. **SST 侧**：多路归并，按主键序，同一 Key 保留层级更高（更新）的值。
2. **Mem 侧**：将多个 ImmutableMemTable 的 SkipList 做归并迭代，同一 Key 保留层级更高的值，构建一个新的 SkipList 作为合并后的 Mem 缓存。
3. 合并完成后，旧的 SST 文件和旧 ImmutableMemTable 被替换。旧 SST 文件延迟删除（确认无查询引用），旧 Mem 引用计数归零后释放。

#### 8.3.3 合并期间内存约束

合并期间需要同时持有旧 SkipList + 正在构建的新 SkipList，有瞬时内存峰值。如果内存不足以完成合并：
- 优先退化最早的双持条目（释放 Mem 缓存空间），再重试合并。
- 若退化后仍不足，跳过本次合并，下次再试。

#### 8.3.4 合并触发条件

- 文件数量：同一类别（new / sinkedWithMem / sinked）的 SST 文件数超过 `PMSConfig.storageCompactMinFiles`（默认 4）。
- 小文件：连续 N 个 SST 的单文件大小 < `PMSConfig.storageCompactThresholdMb`（默认 32MB）。
- 查询效率：当同一类别的 SST 文件数量过多导致 BloomFilter 检查次数过多时，可主动触发合并。
