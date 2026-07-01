# PMS Server 设计文档

## 1. 模块定位
PMS 的可执行外壳。负责解析配置、管理生命周期、暴露 RPC 接口、编排故障恢复流程、暴露可观测性端点。它消费 `pms-core`、`pms-sink-paimon` 与 `pms-lookup-paimon`，本身不包含本地存储或 Paimon 文件查询逻辑。

## 2. 核心组件

### 2.1 RPCServer (HTTP/2 Binary Hot Path)

依赖 `pms-core` 提供的接口。

PMS 对外热路径采用 `pms-protocol` 定义的 HTTP/2 binary raw bytes API：
server 只解析协议 envelope 和基础限制，不解释 `keyBytes` / `rowBytes` 内部格式。
JSON 接口只保留为调试、测试或历史兼容路径，不作为高 QPS 写入与点查入口。

**暴露的服务**：

| 服务 | 请求 | 响应 | 调用核心接口 |
|------|------|------|-------------|
| `local/put` | `RecordBatch(single PUT)` | `WriteResult(status, acceptedCount)` | `PMSBucketDirector.put()` |
| `local/delete` | `RecordBatch(single DELETE)` | `WriteResult(status, acceptedCount)` | `PMSBucketDirector.delete()` |
| `local/writeBatch` | `RecordBatch(PUT/DELETE)` | `WriteResult(status, acceptedCount)` | `PMSBucketDirector.writeBatch()` |
| `full/get` | `KeyBatch(single key)` | `LookupBatchResult` | `PMSBucketDirector.lookup()` + `pms-lookup-paimon` |
| `local/get` | `KeyBatch(single key)` | `LookupBatchResult` | `PMSBucketDirector.lookup()` |
| `local/getPrefix` | `KeyBatch(single prefix)` | `LookupBatchResult` | `PMSBucketDirector.prefixScan()` |

**点查接口语义**：

- `get` 是默认完整表点查。`pms-server` 先查询 PMS 本地层，本地完全 miss 后再穿透查询 Paimon。
- `getLocal` 只查询 PMS 本地层，不穿透 Paimon。响应中的 `result` 必须区分：
  - `HIT`：本地命中 PUT，返回 `row`。
  - `DELETED`：本地命中 tombstone，不返回 `row`，调用方不得继续把它当成普通 miss 后查 Paimon。
  - `MISS`：PMS 本地完全未命中，调用方可自行决定是否查 Paimon。
- 本地 tombstone 必须阻断后续 Paimon 历史数据查询，避免已删除旧值复活。

**主键前缀查询语义**：

- 请求 JSON 只接受主键字段。
- 字段必须按 Paimon primary key 定义顺序提供连续前缀。例如主键为 `(id, sub_id, version)` 时，允许 `{ "id": 1 }` 和 `{ "id": 1, "sub_id": 2 }`，不允许跳过 `id` 只传 `sub_id`。
- 至少提供第一个主键字段。
- 返回行按 PMS primary key encoded bytes 升序排列。
- 本地多层数据按 sequence 选择最新版本；最新版本为 tombstone 的 key 不返回，避免旧层数据复活。
- V1 仅支持 `prefixLocal`。`prefix` 作为完整表 prefix 查询接口名预留，当前直接返回 `NOT_SUPPORTED`；未来实现时必须合并 PMS 本地层与 Paimon 结果，并用 PMS 本地 tombstone 覆盖 Paimon 旧值。

**点查 Paimon 历史数据语义（pms-lookup-paimon 接入后的目标）**：

- `pms-server` 先将主键编码为 PMS key，并调用 `PMSBucketDirector.lookup()` 获取本地三态结果。
- 本地 PUT 命中时直接解码返回；本地 tombstone 命中时直接返回 not found，禁止继续查询历史 Paimon 数据。
- 只有本地 memTable 与本地 SST 全部 miss 时，才调用 `pms-lookup-paimon`。server 用 `FileStoreTable.createRowKeyExtractor()` 从完整主键请求得到 partition、bucket 与 trimmed primary key，并将它们传入 bucket-scoped lookup primitive。
- lookup 的 HIT / DELETED / MISS 分别映射为 row / not found / not found。UNKNOWN 映射为可重试的 `PmsLookupUnavailableException`，不得映射为 MISS。
- 生产路径不使用 Paimon `ReadBuilder` 或 `LocalTableQuery`。`ReadBuilder` 只用于集成测试与压测正确性对照。模块的完整 snapshot、commit delta、恢复和 profile 约束见 [pms-lookup-paimon.md](pms-lookup-paimon.md)。

**写入响应状态**：

| status | 含义 | Client 行为 |
|--------|------|------------|
| `OK` | 写入成功 | 继续写入 |
| `OVERLOADED` | 系统过载，写入被拒绝 | 反压重试 |
| `SCHEMA_MISMATCH` | Schema 不一致（V1 中视为 Fatal Error） | 停止写入 |
| `SHUTTING_DOWN` | 服务正在停机 | 切换到其他节点 |

完整协议 status、endpoint 和 binary payload 见 [pms-protocol.md](pms-protocol.md)。

**流控集成**：RPC 层的写入入口处调用 `WriteAdmissionController.evaluate()`，若返回 OVERLOADED 则响应 `OVERLOADED`。详见 [pms-core.md](pms-core.md) § 4。

**查询流控**：查询请求一般不流控（读取不消耗内存配额），但在 OVERLOADED 水位下可限制并发查询数（可选，保护磁盘 IO）。

### 2.2 RecoveryManager (启动恢复管理器)

启动时执行逻辑：

```
1. 加载 Paimon 表与本地 SST/flush boundary
2. 初始化 WALManager，扫描 DATA WAL 文件并恢复 sequence 水位
3. 重放 DATA_RECORD：结合本地 `lastFlushedSequenceId` 恢复边界，只重放尚未被 SST 承载的 DATA 记录
4. 初始化 SinkMetaStore，扫描 prepare/success metadata：
   - 若本地有 prepare 但无 success：
     使用 SinkMeta 中保存的 prepared commit payload、batch 信息和 fileRefs 恢复未完成提交；真实 Paimon sink 接入后应先校验 data file refs，再重试 commit。
   - 若存在 success：
     通过 success.sstIds 与 persistedSequenceId 推导 sinkedSST，并修正 SST metadata state。
5. 创建 `pms-lookup-paimon` stack 并取得其独立 cache directory 的所有权；不恢复 live file view，也不回放历史 delta。
6. 恢复完毕，启动 RPCServer 和定时 Flush/Compact 线程。首次访问每个 bucket 时从完整最新 snapshot 安装其 view。
```

**启动顺序**：

```
ConfigManager.load()
    │
    ▼
StorageManager.initialize()
    │
    ▼
SinkMetaStore.initialize()
    │
    ▼
WALManager.initialize()
    │
    ▼
PMSBucketDirector.initialize()
    │
    ▼
BackgroundTaskScheduler.start()
    │
    ▼
RPCServer.start()
```

### 2.3 ConfigManager

配置管理器，是 pms-core `PMSConfig` 的生产者。负责从外部配置源读取原始配置，构造 pms-core 定义的 `PMSConfig` 对象，注入到各核心组件中。

**职责边界**：

```
┌─────────────────────────────────────────────────┐
│  pms-server: ConfigManager（配置生产者）          │
│                                                 │
│  - 从 pms-server.yml / 环境变量 / 启动参数读取   │
│  - 校验配置项合法性和一致性                       │
│  - 构造 PMSConfig 对象                           │
│  - V1 不支持运行时热更新，配置变更需重启           │
│                                                 │
└─────────────────────┬───────────────────────────┘
                      │ 构造并注入
                      ▼
┌─────────────────────────────────────────────────┐
│  pms-core: PMSConfig（配置消费者）                │
│                                                 │
│  - 单一平铺 Record，包含所有核心配置字段          │
│  - 组件通过构造函数接收 PMSConfig                │
│                                                 │
└─────────────────────────────────────────────────┘
```

**配置读取与构造**：

```java
class ConfigManager {
    private PMSConfig currentConfig;

    // 从外部配置源加载并构造 PMSConfig
    PMSConfig load(String configPath) {
        // 1. 读取 pms-server.yml
        // 2. 校验配置项合法性（如路径不为空等）
        // 3. 构造 PMSConfig 对象
        this.currentConfig = buildPMSConfig(rawConfig);
        return currentConfig;
    }
}
```

**外部配置项与 PMSConfig 字段的映射**：

| YAML 配置项 | 默认值 | 映射到 PMSConfig 字段 |
|------------|--------|----------------------|
| `pms.server.port` | 9090 | server 自有，不在 core 中 |
| `pms.memtable.max_entries` | 1000000 | `memtableMaxEntries` |
| `pms.memtable.max_size_mb` | 256 | `memtableMaxSizeMb` |
| `pms.wal.dir` | - | `walDir` |
| `pms.wal.file_size_mb` | 256 | `walFileSizeMb` |
| `pms.storage.dir` | - | `storageDir` |
| `pms.storage.sinked_max_size_mb` | 10240 | `StorageConfig.sinkedMaxSizeMb` |
| `pms.storage.sinked_max_count` | 100 | `StorageConfig.sinkedMaxCount` |
| `pms.storage.local_sst_max_rows` | 0（禁用） | `StorageConfig.localSstMaxRows` |
| `pms.storage.compact_threshold_mb` | 32 | `storageCompactThresholdMb` |
| `pms.storage.compact_min_files` | 4 | `storageCompactMinFiles` |
| `pms.sink.interval_ms` | 30000 | `sinkIntervalMs` |
| `pms.sink.max_pending_ssts` | 8 | `sinkMaxPendingSsts` |
| `pms.flowcontrol.overloaded_immutable_count` | 4 | `flowcontrolOverloadedImmutableCount` |
| `pms.flowcontrol.overloaded_pending_sst_count` | 16 | `flowcontrolOverloadedPendingSstCount` |
| `pms.paimon.table_path` | - | `paimonTablePath` |
| `pms.paimon.warehouse` | - | `paimonWarehouse` |
| `pms.paimon.cache_enabled` | true | `PaimonConfig.cacheEnabled`，透传为 Paimon `cache-enabled` |
| `pms.paimon.manifest_cache_small_file_memory` | 128mb | `PaimonConfig.manifestCacheSmallFileMemory`，透传为 Paimon `cache.manifest.small-file-memory` |
| `pms.paimon.manifest_cache_small_file_threshold` | 1mb | `PaimonConfig.manifestCacheSmallFileThreshold`，透传为 Paimon `cache.manifest.small-file-threshold` |
| `pms.paimon.manifest_cache_max_memory` | - | `PaimonConfig.manifestCacheMaxMemory`，非空时透传为 Paimon `cache.manifest.max-memory` |
| `pms.lookup.cache.enabled` | true | `PmsLookupConfig.cacheEnabled` |
| `pms.lookup.cache.dir` | `${java.io.tmpdir}/pms-lookup-cache/<db>.<table>` | `PmsLookupConfig.cacheDir`；不得位于 WAL、storage 或本地 Paimon warehouse 下 |
| `pms.lookup.cache.max_bytes` | 3gb | `PmsLookupConfig.maxCacheBytes` |
| `pms.lookup.cache.build_threshold` | 3 | `PmsLookupConfig.buildThreshold` |
| `pms.lookup.cache.build_threads` | 2 | `PmsLookupConfig.buildThreads`，也作为当前最大在途 build 数 |
| `pms.lookup.cache.build_timeout_ms` | 30000 | `PmsLookupConfig.buildTimeout` |
| `pms.lookup.cache.retry_backoff_ms` | 60000 | `PmsLookupConfig.retryBackoff` |
| `pms.lookup.direct.metadata_cache_entries` | 1024 | direct Parquet lookup 的文件 metadata cache 容量 |

### 2.4 BackgroundTaskScheduler

定时任务调度器，驱动所有后台操作。

**任务列表**：

| 任务 | 默认间隔 | 说明 |
|------|---------|------|
| MemTable Freeze 检查 | 1s | 检查 curMemTable 是否达阈值，触发 `freezeCurMemTable()` |
| Immutable Flush | 立即（Freeze 后） | 将新冻结的 ImmutableMemTable 刷盘为 SST |
| Sink Paimon | 30s | 检查 newSST 数量，触发 `sinkToPaimon()` |
| Paimon Compaction | 暂不启用 | 未来由 server 显式掌控 Paimon data-file compaction；V1 初期依赖 Paimon 写入提交中的隐式维护 |
| 本地 SST 合并 | 300s | 检查小文件数量，触发 `compactLocalSSTs()` |
| sinkedSST 淘汰 | Sink 后 | 根据本地 SST 总大小、总文件数或总物理 entry 数检查阈值，循环触发 `evictOldestSinkedSST()`；只删除已 sinked 的最老 SST |
| WAL 截断 | 300s | 检查可安全截断的 WAL 文件，执行 `truncate()` |
| 水位线检查 | 0.5s | 评估当前水位线，调整后台任务优先级 |

本地 SST 合并当前是 standalone compact，不与 Paimon sink merge 融合。sink、local compact 和 sinkedSST evict 在 core 内串行化，避免 compact 修改正在 sink 的 new run；sink+compact 融合优化暂缓，需等独立恢复状态机设计清楚后再实现。

Paimon compaction 是 Paimon manifest/data-file 层的维护任务，不进入 `pms-core` 的本地 SST 状态机。V1 初期暂不实现显式 Paimon compaction 控制；普通 sink 成功提交 payload 已能覆盖 Paimon 写入提交中可能携带的 data/compact 文件变动。未来显式 compaction 接入时，server 需确保普通 sink commit 与 compact commit 串行化，并复用 `pms-lookup-paimon` 的成功提交 delta 发布协议。

**任务优先级调整**（与流控联动）：

| 水位 | 任务策略 |
|------|---------|
| NORMAL | 按默认间隔执行 |
| OVERLOADED | Flush/Sink 立即触发，加速消化积压 |

### 2.5 GracefulShutdown (优雅停机)

收到停机信号（SIGTERM / 管理API调用）后的分阶段停机流程。

**Phase 1 - Drain（拒绝新请求）**：
- RPC 立即对新请求返回 `SHUTTING_DOWN`，Client 侧切换到其他节点或重试。
- 已进入 RPC 处理的写入请求允许完成（drain in-flight）。
- 停止接受新的 `BackgroundTaskScheduler` 定时任务触发（允许当前正在执行的任务完成）。

**Phase 2 - Quiesce（等待静默）**：
- 等待正在执行的 Sink 操作完成（设超时，默认 30s）。
- 如果有 Sink 正在进行，不主动触发新的 Freeze/Flush，让当前 Sink 走完。
- 关闭 RPC 监听端口，断开 Client 连接。

**Phase 3 - Shutdown（最终清理）**：
- 强制 Freeze 当前 curMemTable。
- Flush 所有剩余 ImmutableMemTable 到 SST。
- 调用 `WALManager.close()`，确保缓冲区刷盘。
- 释放 Paimon 表资源。
- 停机完成。

**超时兜底**：整个停机流程设置硬超时（默认 60s），超时后强制退出。此时数据已通过 WAL 保证持久性，下次启动时走恢复流程。

### 2.6 MetricsExporter 实现

`pms-core` 定义了 `MetricsExporter` 接口，`pms-server` 提供具体实现。

初期仅提供：
- **JmxExporter**：注册 JMX MBean，供 JConsole / VisualVM / Arthas 实时查看。零外部依赖，JDK 自带。

具体指标体系待 [pms-core-statistic.md](pms-core-statistic.md) 详细设计完成后对接。

## 3. 启动与停机完整流程

```
┌─────────────── 启动 ───────────────┐
│                                     │
│  ConfigManager.load()               │
│       │                             │
│       ▼                             │
│  WALManager.initialize()            │
│       │                             │
│       ▼                             │
│  RecoveryManager.recover()          │
│       │                             │
│       ▼                             │
│  PMSBucketDirector.initialize()     │
│       │                             │
│       ▼                             │
│  BackgroundTaskScheduler.start()    │
│       │                             │
│       ▼                             │
│  RPCServer.start()                  │
│                                     │
└─────────────────────────────────────┘

┌─────────────── 停机 ───────────────┐
│                                     │
│  Signal (SIGTERM / API)             │
│       │                             │
│       ▼                             │
│  Phase 1: Drain                     │
│  - RPC 返回 SHUTTING_DOWN           │
│  - 停止定时任务触发                  │
│       │                             │
│       ▼                             │
│  Phase 2: Quiesce (timeout: 30s)    │
│  - 等待 in-flight Sink 完成         │
│  - 关闭 RPC 端口                    │
│       │                             │
│       ▼                             │
│  Phase 3: Shutdown (timeout: 60s)   │
│  - Freeze + Flush 所有 MemTable     │
│  - WALManager.close()               │
│  - 释放 Paimon 资源                 │
│                                     │
└─────────────────────────────────────┘
```
