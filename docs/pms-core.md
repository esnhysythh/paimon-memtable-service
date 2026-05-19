# PMS Core Engine 设计文档

## 1. 模块定位
纯粹的单机 LSM 存储引擎，完全不依赖任何 RPC 框架、Web 容器或外部配置中心。只负责数据的内存管理、本地持久化、WAL 协调以及与 Paimon API 的交互。

## 2. 配置契约

pms-core 定义所有核心配置的类型和默认值。配置的加载与解析由 pms-server 的 ConfigManager 负责，core 本身不知道配置来源（YAML / Properties / 环境变量）。

### 2.1 设计原则

- **谁消费配置，谁定义类型**：core 消费配置，所以配置类定义在 core 中。
- **纯 Record**：配置类无外部依赖，仅包含字段、默认值和校验逻辑。
- **构造时注入**：core 组件通过构造函数接收 `PMSConfig` 对象，不主动查找配置。V1 不支持运行时热更新，配置变更需重启。

### 2.2 配置类

```java
record PMSConfig(
    // ── MemTable ──
    int memtableMaxEntries,          // curMemTable 条目上限，默认 1_000_000
    int memtableMaxSizeMb,           // curMemTable 大小上限，默认 256

    // ── WAL ──
    String walDir,                   // WAL 目录（V1 单盘）
    int walFileSizeMb,               // WAL 文件滚动大小，默认 256

    // ── 本地存储 ──
    long storageSinkedMaxSizeMb,     // sinkedSST 总大小上限，默认 10240
    int storageSinkedMaxCount,       // sinkedSST 文件数上限，默认 100
    int storageCompactThresholdMb,   // 小 SST 合并阈值，默认 32
    int storageCompactMinFiles,      // 触发合并的最小文件数，默认 4

    // ── Sink ──
    int sinkIntervalMs,              // Sink 触发间隔，默认 30000
    int sinkMaxPendingSsts,          // 触发 Sink 的 SST 数量阈值，默认 8

    // ── 流控 ──
    int flowcontrolOverloadedImmutableCount,  // Immutable 数量 >= 此值则拒绝写入，默认 4
    int flowcontrolOverloadedPendingSstCount, // 待 Sink SST 数量 >= 此值则拒绝写入，默认 16

    // ── Paimon ──
    String paimonTablePath,          // Paimon 表路径
    String paimonWarehouse           // Paimon Warehouse 路径
) {}
```

### 2.3 组件构造方式

core 组件通过构造函数接收 `PMSConfig`：

```java
class CurMemTable implements IMemTable {
    CurMemTable(PMSConfig config) { ... }
}

class WALManagerImpl implements WALManager {
    WALManagerImpl(PMSConfig config) { ... }
}
```

## 3. 核心组件与接口定义

### 3.1 MemTableEngine

管理内存中的数据缓冲，基于 SkipList 实现。接口拆分为 `CurMemTable`（可写）和 `ImmutableMemTable`（只读 + 引用计数），由 `CurMemTable.freeze()` 产生 `ImmutableMemTable`。

**CurMemTable 接口**：

```java
interface CurMemTable {
    void put(Key key, Value value);
    void delete(Key key);
    Value get(Key key);
    ImmutableMemTable freeze();  // 冻结为 immutable，返回只读实例
    long estimatedSize();
    int entryCount();
    Iterator<Entry> iterator();
}
```

- schemaId 校验由上层 BucketDirector 处理，不在此接口传递。
- Key 使用无符号字节比较（与 Paimon 主键序一致），参见 [paimon-primary-key-encoding.md](../../references/paimon-primary-key-encoding.md)。

**ImmutableMemTable 接口**：

```java
interface ImmutableMemTable {
    Value get(Key key);
    Iterator<Entry> iterator();
    long estimatedSize();
    int entryCount();
    void incrementRef();
    void decrementRef();
    long refCount();
}
```

**实现**：

- **SkipListCurMemTable**：当前活跃的可写 MemTable。
  - 底层 `ConcurrentSkipListMap<Key, Value>`，线程安全。
  - 写入前检查容量：`entryCount() < config.memtableMaxEntries()` 且 `estimatedSize() < config.memtableMaxSizeMb() * 1024 * 1024`。
  - `freeze()` 原子替换内部 Map 引用，返回持有旧 Map 的 `SkipListImmutableMemTable`。

- **SkipListImmutableMemTable**：冻结后的只读 MemTable。
  - 构造时接收 `SkipListCurMemTable` 的内部 SkipList 引用（浅拷贝，零开销）。
  - 维护 `AtomicLong refCount`，查询进入时 `incrementRef()`，离开时 `decrementRef()`。
  - `refCount` 归零后可安全退役（释放内存）。

**容量阈值**（来自 `PMSConfig`）：
- `memtableMaxEntries`：条目数上限，默认 1,000,000。
- `memtableMaxSizeMb`：内存占用上限，默认 256MB。
- 达到任一阈值触发 Freeze。

### 3.2 LocalStorageManager

负责 `newSST` 和 `sinkedSST` 的落盘与读取。

**SST 文件格式**（自定义行存）：

```
┌──────────────────────────────────────────────────────┐
│ [Data Block 0]                                      │
│ [Data Block 1]                                      │
│ ...                                                  │
│ [BloomFilter Block]                                  │
│ [Index Block]                                        │
│ [Footer]                                             │
│   ├─ Magic: "PMS_SST_1"                             │
│   ├─ Version, Block Count                            │
│   ├─ BloomFilter / Index Block Offset                │
│   └─ Footer CRC32 ── 覆盖 Footer 之前的全部数据      │
└──────────────────────────────────────────────────────┘
```

- Key 按主键序排列（与 Paimon 底层主键序编码 100% 一致，保证无需再排序即可写入 Paimon）。
- V1 仅 Footer CRC32 校验全文件完整性，不做逐 Data Block CRC。

**核心接口**：

```java
interface LocalStorageManager {
    // 将 ImmutableMemTable 刷盘为 newSST
    SSTMeta flushToSST(ImmutableMemTable memTable);

    // 读取 SST 中的指定 Key
    Optional<byte[]> get(SSTMeta meta, byte[] key);

    // 多路归并合并多个 SST，保留最新 Key
    SSTMeta compactSSTs(List<SSTMeta> metas);

    // 删除指定 SST 文件
    void deleteSST(SSTMeta meta);

    // 淘汰最老的 sinkedSST（确认无引用后删除）
    void evictOldest();
}
```

**BloomFilter**：
- 每个 SST 文件包含基于主键的 BloomFilter。
- 写入 SST 时构建，基于期望 FPP（False Positive Rate，默认 0.01）分配位图大小。
- 查询时先检查 BloomFilter，通过则读取 Data Block，不通过则跳过。

**SST 文件校验**：
- 读取时校验 Footer CRC32，覆盖 Footer 之前的全部数据。
- 校验失败 → 标记该 SST 文件为损坏，记录告警日志。损坏 SST 中的数据从其他层（更新层的 MemTable 或 Paimon 穿透）补全。
- V1 不做逐 Block 降级读取。

### 3.3 WALManager

V1 采用单盘 WAL，保证数据持久性和崩溃恢复能力。

> **后续演进**：双盘 WAL（主盘 + 备盘同步写、互恢复）作为后续演进方向。双盘写入时主盘写成功即视为写入成功，备盘失败仅记录告警不阻塞写入；`SINK_SUCCESS` 记录需双盘都写成功。

**记录类型**：

| 类型 | 值 | 说明 |
|------|---|------|
| DATA_RECORD | 0x01 | 写入的数据（schemaId + key + value） |
| SINK_START | 0x02 | 标记 Sink 流程开始 |
| SINK_PREPARE | 0x03 | Paimon 返回的 CommitMessage 字节流 |
| SINK_SUCCESS | 0x04 | 成功提交的 Snapshot ID + 关联元信息 |

**WAL 记录格式**：

```
┌──────────┬──────────┬────────────┬──────────┬──────────┐
│ RecordType│ Length   │ Payload    │ CRC32    │ Magic    │
│ (1 byte) │ (4 byte) │ (N bytes)  │ (4 byte) │ (2 byte) │
└──────────┴──────────┴────────────┴──────────┴──────────┘
```

| 字段 | 大小 | 说明 |
|------|------|------|
| RecordType | 1 byte | 记录类型 |
| Length | 4 byte | Payload 长度 |
| Payload | N bytes | 记录内容 |
| CRC32 | 4 byte | 覆盖 `RecordType + Length + Payload` 的 CRC32 校验和 |
| Magic | 2 byte | 固定值 `0xPM`（0x504D），用于检测写入截断 |

**接口**：

```java
interface WALManager {
    // 写入数据记录
    void appendDataRecord(byte[] schemaId, byte[] key, byte[] value);

    // 写入状态机记录
    void appendSinkStart();
    void appendSinkPrepare(byte[] commitMessage);
    void appendSinkSuccess(long snapshotId);

    // 恢复重放
    void replay(ReplayCallback callback, long highWatermarkSnapshotId);

    // 截断（清理已确认提交的旧日志）
    void truncate(long safeSnapshotId);

    // 关闭（刷盘缓冲区）
    void close();
}

interface ReplayCallback {
    void onDataRecord(byte[] schemaId, byte[] key, byte[] value);
    void onSinkStart();
    void onSinkPrepare(byte[] commitMessage);
    void onSinkSuccess(long snapshotId);
}
```

**WAL 文件管理**：
- 按固定大小滚动（默认 256MB 一个文件）。
- 每个文件头部记录 `maxSnapshotId`，用于截断时快速判断。

**WAL 截断策略**：
- 安全截断条件：存在 `SINK_SUCCESS(snapshotId=X)` 且 Paimon 侧 Snapshot X 确实存在。
- 截断时删除所有 `maxSnapshotId <= 安全 Snapshot ID` 的 WAL 文件，正在写入的文件永不删除。
- 截断触发：由 `BackgroundTaskScheduler` 定期执行（默认每 5 分钟），也可在 WAL 配额使用率超过 80% 时立即触发。

**WAL 恢复时校验**：
- 读取每条记录时校验 CRC32 和 Magic。
- 尾部记录校验失败 → 可能是崩溃时的 partial write → 截断该记录，之前的记录视为安全。
- 中间记录校验失败 → 磁盘损坏 → 报错，人工介入（V1 单盘无法从备盘恢复）。

### 3.4 PaimonSinkManager

封装对 Paimon 底层 API 的调用，严格遵循 2PC 流程。

**2PC 流程**：

```
1. 提取本地所有 newSST 和 ImmutableMemTable
2. 归并生成 preSink Parquet 文件
   └─ 多路归并：按主键序归并 newSST + ImmutableMemTable，保留最新 Key
   └─ 输出 Parquet 格式（与 Paimon 底层格式一致）
3. 调用 Paimon prepareCommit → CommitMessage
4. 通知 WALManager 写入 SINK_PREPARE
5. 调用 Paimon commit → 新 Snapshot
6. 通知 WALManager 写入 SINK_SUCCESS（含 Snapshot ID）
7. 保留 preSink 文件直到 SINK_SUCCESS 落盘
   └─ 防止崩溃重试时需重新生成 preSink
```

**接口**：

```java
interface PaimonSinkManager {
    // 执行完整的 Sink 流程
    SinkResult sink(SinkContext context);

    // 触发 Paimon Compaction
    void compact();

    // 获取当前最新 Snapshot ID
    long latestSnapshotId();

    // 基于 Manifest 索引穿透查询
    Optional<byte[]> paimonGet(byte[] key);
}
```

**Compaction 集成**：
- 调用 Paimon 原生 `Table.compact()` 接口，不自己实现合并逻辑。
- 触发时机：Paimon L0 文件数超过阈值时，由 `BackgroundTaskScheduler` 触发。
- Compaction 是异步操作，不阻塞 Sink 路径。

**Paimon Manifest 索引**：
- 维护 Paimon 最新 Snapshot 的元信息索引，用于加速穿透查询。
- V1 采用全量加载策略。后续按需引入分区分级加载优化。
- TODO: Manifest 索引的具体结构、加载时机、内存占用量化设计待补充。

### 3.5 PMSBucketDirector

总协调器，管理数据从写入到最终落盘的完整生命周期。

详细设计参见 [pms-core-bucket-director.md](pms-core-bucket-director.md)。

**核心职责**：
- 管理 `curMemTable → ImmutableMemTable → newSST → sinkedSST` 的状态机流转。
- 编排查询路径的多层穿透。
- 协调 Freeze、Flush、Sink、Evict 各阶段。
- 向上层暴露统一的 `put` / `get` 接口。

### 3.6 Statistic

可观测性基础设施，提供低开销的指标采集与查询能力。

TODO: 详细设计待核心组件稳定后再补充。初期仅定义 `MetricsRegistry` 接口，具体指标类型和体系待定。

## 4. 流控与内存预算

### 4.1 两层模型

初期采用最简单的两层判断：

```
                ┌──────────────────────────────────┐
                │         Write Admission           │
                │                                  │
                │  ┌────────────┐  ┌────────────┐  │
Write Request ──│─►│  NORMAL    │  │ OVERLOADED │  │
                │  │  正常写入  │─►│   拒绝     │  │
                │  └────────────┘  └────────────┘  │
                └──────────────────────────────────┘
```

**判断条件**（满足任一即为 OVERLOADED）：

| 维度 | 阈值 | 默认值 |
|------|------|--------|
| Immutable MemTable 数量 | >= `PMSConfig.flowcontrolOverloadedImmutableCount` | 4 |
| 待 Sink 的 newSST 数量 | >= `PMSConfig.flowcontrolOverloadedPendingSstCount` | 16 |

**响应策略**：
- **NORMAL**：正常接受写入，RPC 返回 `OK`。
- **OVERLOADED**：快速拒绝，RPC 返回 `SERVICE_OVERLOADED`，Client 走反压重试逻辑。不阻塞写入线程。

### 4.2 实现

```java
enum WatermarkLevel { NORMAL, OVERLOADED }

class WriteAdmissionController {
    private final PMSConfig config;
    private final PMSBucketDirector bucketDirector;

    WatermarkLevel evaluate() {
        BucketStateSnapshot snapshot = bucketDirector.stateSnapshot();

        if (snapshot.immutableMemTableCount() >= config.flowcontrolOverloadedImmutableCount()) {
            return OVERLOADED;
        }
        if (snapshot.newSSTCount() >= config.flowcontrolOverloadedPendingSstCount()) {
            return OVERLOADED;
        }
        return NORMAL;
    }
}
```

### 4.3 与后台任务的联动

| 水位 | 后台任务策略 |
|------|-------------|
| NORMAL | Flush/Sink 按配置间隔正常触发 |
| OVERLOADED | Flush/Sink 立即触发，加速消化积压 |

### 4.4 后续扩展方向

当系统跑起来并有真实负载数据后，可考虑：
- 增加 YELLOW（限速）层，在 NORMAL 和 OVERLOADED 之间提供缓冲。
- 增加写入等待耗时维度。
- 内存使用率维度。

## 5. 并发模型与数据完整性

### 5.1 核心原则

- **无全局锁**：整个系统不使用任何粗粒度全局互斥锁。
- **Volatile 引用切换**：状态变更通过 volatile 引用的原子替换实现，而非就地修改。
- **文件延迟删除**：被淘汰的 SST 文件不立即删除，确认无查询引用后才删除。
- **初期简化**：查询时直接读 volatile 引用遍历，不做快照拷贝。引用计数仅在 Evict 删除文件时检查。

### 5.2 关键场景的并发控制

**并发写入 curMemTable**：底层 `ConcurrentSkipListMap` 本身线程安全，无需额外同步。

**Freeze（curMemTable → ImmutableMemTable）**：
- `curMemTable` 字段用 `volatile` 修饰。
- `freeze()` 时先构造新的空 `CurMemTable`，再原子替换引用。
- 不需要 Copy-on-Write：Freeze 后原 MemTable 天然变为只读。

**查询穿透过程中的并发**：
- 初期方案：直接遍历 volatile 列表，不做快照拷贝。
- 层列表通过 volatile 引用替换整个列表（不是就地修改），查询线程看到的要么是旧列表要么是新列表，不会看到半更新状态。
- 最坏情况：查 curMemTable 时数据刚被 Freeze，查 immutableList 时列表已更新包含了该 MemTable，结果正确。
- 后续扩展：如果查询一致性要求更严格，可引入快照读 + 防御性拷贝 + 引用计数。

**Sink 过程中查询正在归并的 SST**：
- 归并操作不影响查询——查询读的是归并前的 SST 文件，文件内容不变。
- 归并完成后，BucketDirector 原子替换列表（newSST → sinkedSST）。

**Evict 删除磁盘文件**：
- 淘汰 sinkedSST 时，确认文件不被任何查询正在读取。
- V1 简化方案：Evict 时检查 `refCount()`，如果 > 0 则跳过本次删除（下次 Evict 检查时再试）。

### 5.3 内存可见性总结

| 变量 | 类型 | 写入方 | 读取方 | 可见性保证 |
|------|------|--------|--------|-----------|
| `curMemTable` | volatile 引用 | Freeze 线程 | 写入线程 / 查询线程 | volatile 读写 |
| `newImmutableList` | volatile 引用 | Freeze 线程 | 查询线程 | volatile 读写 |
| `newSSTList` | volatile 引用 | Flush/Sink 线程 | 查询线程 | volatile 读写 |
| `sinkedSSTList` | volatile 引用 | Sink/Evict 线程 | 查询线程 | volatile 读写 |
| SkipList 内部 | ConcurrentSkipListMap | 写入线程 | 查询线程 | ConcurrentMap 内部保证 |
| `refCount` | AtomicLong | Evict 线程 | Evict 线程 | Atomic 操作 |

### 5.4 崩溃恢复流程

```
RecoveryManager 启动
        │
        ▼
1. 加载 Paimon 表，获取最新 Snapshot ID (paimonSnapshotId)
        │
        ▼
2. 初始化 WALManager，扫描 WAL 文件
   获取 WAL 中记录的最高 Snapshot ID (walSnapshotId)
        │
        ▼
3. 比对 paimonSnapshotId 与 walSnapshotId
   ┌───────────────────────────────────────────────────────┐
   │ paimonSnapshotId > walSnapshotId                      │
   │ → 不应发生（PMS 独占写入），报警，以 Paimon 为准      │
   │ → WAL 中 snapshotId 之后的 DATA_RECORD 重放           │
   ├───────────────────────────────────────────────────────┤
   │ paimonSnapshotId == walSnapshotId                     │
   │ → 正常，重放 snapshotId 之后的 DATA_RECORD            │
   ├───────────────────────────────────────────────────────┤
   │ paimonSnapshotId < walSnapshotId                      │
   │ → 异常，WAL 记录了 Paimon 没有的 Snapshot             │
   │ → 可能 commit 后 Paimon 侧丢失？检查 Paimon 状态      │
   └───────────────────────────────────────────────────────┘
        │
        ▼
4. 检查是否存在 SINK_PREPARE 但无 SINK_SUCCESS
   ┌───────────────────────────────────────────────────────┐
   │ 有 SINK_PREPARE，无 SINK_SUCCESS                       │
   │ → 检查 preSink 文件是否存在                            │
   │   ├─ 存在 → 使用 CommitMessage 重试 Paimon commit     │
   │   └─ 不存在 → 从本地 SST 重新生成 preSink，重试全流程  │
   ├───────────────────────────────────────────────────────┤
   │ 无 SINK_PREPARE                                       │
   │ → 仅重放 DATA_RECORD 恢复 MemTable                    │
   └───────────────────────────────────────────────────────┘
        │
        ▼
5. 重放 DATA_RECORD，恢复 curMemTable
   - 根据 WAL 截断点，只重放 snapshotId 之后的记录
   - 重放过程中校验每条记录的 CRC32
        │
        ▼
6. 恢复完毕，启动 RPC 和后台任务
```

## 6. 核心状态机流转

详见 [pms-core-bucket-director.md](pms-core-bucket-director.md) § 3。

```
curMemTable ──freeze──► ImmutableMemTable (new)
                          │
                        flush
                          ▼
                      newSSTWithMem
                          │
                        sink
                          ▼
                     sinkedSSTWithMem
                          │
                      mem退役
                          ▼
                       sinkedSST
                          │
                       evict
                          ▼
                       [删除]
```

> 注：newSSTWithMem 和 sinkedSSTWithMem 中的 Mem 缓存是纯性能优化，可在内存不足时随时退化为不带 Mem 的状态（newSST / sinkedSST），详见 [pms-core-bucket-director.md](pms-core-bucket-director.md)。
