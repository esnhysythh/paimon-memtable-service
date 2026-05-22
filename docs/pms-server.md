# PMS Server 设计文档

## 1. 模块定位
PMS 的可执行外壳。负责解析配置、管理生命周期、暴露 RPC 接口、编排故障恢复流程、暴露可观测性端点。它是 `pms-core` 的消费者，本身不包含存储逻辑。

## 2. 核心组件

### 2.1 RPCServer (基于 gRPC 或 Netty)

依赖 `pms-core` 提供的接口。

**暴露的服务**：

| 服务 | 请求 | 响应 | 调用核心接口 |
|------|------|------|-------------|
| `write` | `WriteRequest(key, value)` | `WriteResponse(status)` | `PMSBucketDirector.put()` |
| `get` | `GetRequest(key)` | `GetResponse(status, value?)` | `PMSBucketDirector.get()` |

**写入响应状态**：

| status | 含义 | Client 行为 |
|--------|------|------------|
| `OK` | 写入成功 | 继续写入 |
| `SERVICE_OVERLOADED` | 系统过载，写入被拒绝 | 反压重试 |
| `SCHEMA_MISMATCH` | Schema 不一致（V1 中视为 Fatal Error） | 停止写入 |
| `SHUTTING_DOWN` | 服务正在停机 | 切换到其他节点 |

**流控集成**：RPC 层的写入入口处调用 `WriteAdmissionController.evaluate()`，若返回 OVERLOADED 则响应 `SERVICE_OVERLOADED`。详见 [pms-core.md](pms-core.md) § 4。

**查询流控**：查询请求一般不流控（读取不消耗内存配额），但在 OVERLOADED 水位下可限制并发查询数（可选，保护磁盘 IO）。

### 2.2 RecoveryManager (启动恢复管理器)

启动时执行逻辑：

```
1. 加载 Paimon 表，获取当前最新 Snapshot ID
2. 初始化 WALManager，扫描 WAL 文件
3. 判断对齐情况（详见 [pms-core.md](pms-core.md) § 5.4）：
   - 若 Paimon 有更新（不应发生，因为独占），报警
   - 若本地有 SINK_PREPARE 但无 SINK_SUCCESS：
     使用 WAL 中保存的 prepared commit payload、batch 信息和 fileRefs 恢复未完成提交；真实 Paimon sink 接入后应先校验 data file refs，再重试 commit。当前 MockSinkManager 只用于打通 PMS 内部状态流转，不执行真实 Paimon 恢复。
   - 若只有 DATA_RECORD：结合本地 `lastFlushedSequenceId` 恢复边界，只重放尚未被 SST 承载的 DATA 记录
4. 恢复完毕，启动 RPCServer 和定时 Flush/Compact 线程
```

**启动顺序**：

```
ConfigManager.load()
    │
    ▼
WALManager.initialize()
    │
    ▼
RecoveryManager.recover()
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
| `pms.storage.sinked_max_size_mb` | 10240 | `storageSinkedMaxSizeMb` |
| `pms.storage.sinked_max_count` | 100 | `storageSinkedMaxCount` |
| `pms.storage.compact_threshold_mb` | 32 | `storageCompactThresholdMb` |
| `pms.storage.compact_min_files` | 4 | `storageCompactMinFiles` |
| `pms.sink.interval_ms` | 30000 | `sinkIntervalMs` |
| `pms.sink.max_pending_ssts` | 8 | `sinkMaxPendingSsts` |
| `pms.flowcontrol.overloaded_immutable_count` | 4 | `flowcontrolOverloadedImmutableCount` |
| `pms.flowcontrol.overloaded_pending_sst_count` | 16 | `flowcontrolOverloadedPendingSstCount` |
| `pms.paimon.table_path` | - | `paimonTablePath` |
| `pms.paimon.warehouse` | - | `paimonWarehouse` |

### 2.4 BackgroundTaskScheduler

定时任务调度器，驱动所有后台操作。

**任务列表**：

| 任务 | 默认间隔 | 说明 |
|------|---------|------|
| MemTable Freeze 检查 | 1s | 检查 curMemTable 是否达阈值，触发 `freezeCurMemTable()` |
| Immutable Flush | 立即（Freeze 后） | 将新冻结的 ImmutableMemTable 刷盘为 SST |
| Sink Paimon | 30s | 检查 newSST 数量，触发 `sinkToPaimon()` |
| Paimon Compaction | 60s | 检查 Paimon L0 文件数，触发 `compact()` |
| 本地 SST 合并 | 300s | 检查小文件数量，触发 `compactLocalSSTs()` |
| sinkedSST 淘汰 | 60s | 检查 sinkedSST 数量/大小，触发 `evictOldestSinkedSST()` |
| WAL 截断 | 300s | 检查可安全截断的 WAL 文件，执行 `truncate()` |
| 水位线检查 | 0.5s | 评估当前水位线，调整后台任务优先级 |

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
