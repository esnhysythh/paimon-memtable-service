# PMS Server 设计文档

## 1. 模块定位

`pms-server` 是单机 PMS 的可执行外壳。它负责配置解析、表与本地状态恢复、HTTP 服务、写入 admission、Paimon 历史点查、后台调度和进程生命周期；本地 LSM 操作本身由 `pms-core` 实现。

V1 采用一张 Paimon 表对应一个 runtime、PMS 是该表唯一写入者的模型。绑定表 Schema 在整套
PMS 本地状态生命周期内保持不变；与唯一写入者约束相同，这由产品与部署行为保证，server
不主动监控或阻止 Schema 变更。配置只在启动时加载，不支持热更新。

需要变更 Schema 时，应先停止上游写入，调用 `/sink` 并等待
`lastPersistedSequenceId >= fenceSequenceId`，再停止 PMS。变更完成后使用全新的 WAL、
storage 与 lookup cache 目录部署新实例，不复用旧 Schema 对应的本地状态。

## 2. 核心组件

### 2.1 HTTP 与协议入口

同一 Jetty 端口暴露两类接口：

- `/pms/api/v1/...`：`pms-protocol` 定义的 HTTP/2 binary hot path，传输 opaque key/value bytes。
- 根路径 JSON API：`/health`、`/write`、`/delete`、`/get`、`/getLocal`、`/prefix`、`/prefixLocal`、`/flush`、`/sink`、`/state`，用于调试与管理，不作为高 QPS 主路径。

binary endpoint 默认要求 HTTP/2。h2c client 应先访问 `/pms/api/v1/handshake` 并确认协商结果，再发送带 body 的请求。协议 payload、status 与限制见 [pms-protocol.md](pms-protocol.md)。

查询语义：

- `get` 先查询 PMS 本地层，本地 MISS 后使用 `pms-lookup-paimon` 查询已提交 Paimon 数据。
- `getLocal` 返回 HIT / DELETED / MISS 三态。DELETED 必须阻止 Paimon 旧值复活。
- `prefixLocal` 只查询本地层；完整表 prefix 查询在 V1 返回 `NOT_SUPPORTED`。
- Paimon lookup 的 UNKNOWN 是可重试失败，不得映射为 MISS。生产查询路径不使用 `ReadBuilder`。

写入入口先检查 runtime 必须为 `RUNNING`，再基于 `BucketStateSnapshot` 做一次快速水位判断。core 在 WAL append 前于写入临界区内复查相同水位，覆盖多个并发请求同时通过 server 检查的窗口。两次检查均不做容量预留；目标是及时停止扩大积压，而不是实现精确配额。

| status | 含义 | 建议行为 |
|--------|------|----------|
| `OK` | 写入成功 | 继续写入 |
| `OVERLOADED` | Immutable/NEW backlog 达到水位，写入未进入 WAL | 退避重试 |
| `SCHEMA_MISMATCH` | 防御性保留的 Schema 不一致状态 | 停止使用当前部署并检查产品约束 |
| `SHUTTING_DOWN` | runtime 不再接收请求 | 切换实例或稍后重试 |

WAL append 成功后若 MemTable apply 失败，core 抛出 `PmsFatalWriteException`。此时请求结果是 unknown outcome，runtime 进入 `FAILED` 并异步关闭 HTTP、scheduler 和 table service，等待重启恢复。

### 2.2 启动与恢复

`PmsTableService.open()` 在对外监听前完成本地状态恢复：

```text
load/create durable writer identity
  -> load/validate Paimon table and supported table profile
  -> initialize local storage and flush boundary
  -> initialize SinkMeta and derive NEW/SINKED state
  -> initialize WAL and replay data beyond lastFlushedSequenceId
  -> restore pending prepared Sink, if any
  -> initialize Paimon lookup stack
  -> create scheduler and HTTP server
  -> status = RUNNING
  -> start HTTP, then scheduler
```

启动时的 metadata 损坏、表 profile 不受支持、SST/flush boundary 不一致或非法 prepared 状态属于确定性错误，必须拒绝启动。server 不会把当前表 Schema 与旧进程或旧本地状态做主动比对；Schema 不变由上述产品约束保证。恢复出的、尚未由 `lastPersistedSequenceId` 覆盖的数据会使 scheduler 尽快建立新的 Paimon 可见性 fence，而不是等待可能失真的 wall-clock age。

#### Writer 身份

Paimon 按 `(commit_user, commitIdentifier)` 判定重复提交，而 PMS 使用本地 sequence 作为
`commitIdentifier`。为避免全新本地目录从 sequence 1 开始后被旧提交编号过滤，server 首次
启动时生成 `<pms.server.commit_user>-<12 位随机十六进制 ID>`，例如
`pms-server-7e4c9a21b6d0`，将完整身份保存为 `storage/commit-user` 的一行文本。
短 ID 取 UUID 的前 48 个随机 bit，用于低频的本地状态重建；不增加远端身份注册机制。

身份通过同步写入临时文件、原子 rename 和目录 fsync，在打开 WAL、恢复 prepared Sink 前
持久化。同一套本地状态的所有启动都读取原身份；全新 WAL/storage 目录生成新身份，sequence
仍从 1 开始。这样无需增加远端 sequence 初始化，也不改变 core 的 Flush/Sink 边界。

`pms.server.commit_user` 现在表示身份前缀，使用原状态时不能修改。迁移机器时须连同
WAL/storage 一起保留 `commit-user`；全新部署使用全新 WAL/storage/cache 目录。身份文件
缺失但 WAL/storage 已有内容、身份格式非法或前缀改变时，启动失败，不自动生成替代身份。
只有首次初始化遗留的 `commit-user.tmp` 可以在没有其他本地状态时重试。
V1 不自动迁移缺少身份文件的旧状态；升级前应使用旧版本完成 Sink，再使用全新目录启动。

### 2.3 配置管理

`ConfigManager` 读取 Java properties，完成默认值、目录隔离、跨配置约束和已移除配置检查，再构造：

- `PMSConfig`：MemTable、WAL、本地 storage 路径、flow control 与 Paimon core 配置。
- `PmsSchedulerConfig`：server 的调度目的与单次操作边界。
- `PmsProtocolConfig`、`PmsLookupConfig`：协议和 Paimon lookup 配置。

调度阈值属于 server，而不是 core 操作 API。配置名表达“需要维护的状态”或“单次操作上限”，不把调度目的写成某个固定动作。

### 2.4 Scheduler

#### 2.4.1 目标与边界

Scheduler 同时服务两个独立目的：

1. 维护本地 MemTable/SST 层的文件数量和查询放大。
2. 控制写入进入 Paimon 的正常可见性延迟。

core 只提供 Freeze、Flush、Sink、Compact、Evict 等单步操作。Scheduler 不预先生成多步 plan，也不维护 `Intent/Decision` 模型；它每次读取 `BucketStateSnapshot`，选择一个最高优先级动作，动作取得进展后丢弃旧快照并从头判断。

#### 2.4.2 两个 worker

| worker | 默认周期 | 职责 |
|--------|----------|------|
| Flush worker | 1 秒 | 连续处理等待中的 ImmutableMemTable，每次 Flush 一个；不执行 Sink/Compact/Evict |
| Maintenance worker | 30 秒 | 串行推进 prepared retry、Paimon fence、NEW/SINKED compact、Sink 与 Evict |

两个 worker 各使用一个单线程 executor，信号会合并为当前轮加至多一轮 rerun，避免 executor 队列无界增长。Flush 与 SST maintenance 可以并发，但 core 的发布协议保证查询透明；Sink/Compact/Evict 由 core 的 SST maintenance mutex 再次串行化。

每轮最多执行 64 个取得进展的动作，然后主动重新排队，避免一个表的积压长期占用 worker。64 是自动调和的公平性 slice，不是某个管理请求必须同步完成的预算，因为管理 API 本身不执行同步 drain。

#### 2.4.3 Paimon 可见性 fence

`pms.paimon.visibility.max_delay_ms` 表达“写入在正常情况下进入 Paimon 的目标最大等待时间”，默认 10 分钟。Scheduler 从 Cur/Immutable/NEW 的最早写入时间计算 lag；达到目标时：

1. Freeze 当前 MemTable，捕获固定 `fenceSequenceId`。
2. 将该值发布为 `pendingPaimonFenceSequenceId`。
3. 请求 Flush worker 推进 `lastFlushedSequenceId`。
4. 当完整 fence 已成为 NEW SST 后，以一个或多个有界 Sink batch 推进。
5. `lastPersistedSequenceId >= fence` 后清除 fence。

新写入位于固定 fence 之后，不会无限延长当前可见性目标。由于检查由 Maintenance worker 周期触发，实际正常检测延迟上限约为 `visibility.max_delay + maintenance interval`，默认约 10 分 30 秒，而不是硬实时 SLA。

V1 不设置 `flush.max_delay`：低流量数据留在 CurMemTable，直到容量阈值、可见性 fence 或手动请求需要它下沉，避免固定周期制造小 SST。

#### 2.4.4 Maintenance 优先级

每次循环严格按以下顺序选择一个动作：

1. 存在可恢复 Sink flight（`PREPARED_RETRY` 或 `FINALIZING`）：调用 `resumeSinkFlight()`；前者复用 durable prepare，后者只重做 durable success 对应的本地收尾，成功前不启动其他 SST maintenance。
2. pending Paimon fence 已由 `lastPersistedSequenceId` 覆盖：清除 controller 状态并重新采样。
3. 存在 pending fence 且 `lastFlushedSequenceId < fence`：signal Flush worker，本轮退出。
4. 存在尚未满足、但已经完整 Flush 的 pending fence：Sink 一个受字节上限约束的 NEW 前缀。
5. Paimon 可见性已到期：Freeze 并建立固定 fence。
6. `NEW count > new_sst.max_count` 且存在可合并的连续组：Compact NEW。
7. NEW 超标但无法在 compact 字节上限内选出至少两个连续 run：Sink 最老 NEW 前缀。
8. `SINKED count > sinked_sst.max_count` 且存在可合并的连续组：Compact SINKED。
9. SINKED 超标但无法 compact：Evict 最老 SINKED run。
10. 无动作：退出本轮。

这种单步调和允许一个问题自然转化为下一轮的另一个状态。例如 NEW compact 后仍超标，会再次 compact；无法继续 compact 时会转为 Sink。Evict 不包含隐式 compact，Sink 也不包含隐式 Freeze/Flush。

#### 2.4.5 操作选择

- **Sink**：选择目标 sequence 以内、从最老开始的连续 NEW 前缀，总输入不超过 `batch_max_bytes`；最老单 run 超限时允许单独推进。没有 SST 数量上限。
- **Compact**：从老到新选择第一个同状态、连续且总输入不超过 `max_input_size` 的至少两个 run。NEW/SINKED 不混合。
- **Evict**：只调用 `evictOldestSinkedSST()`，不允许任意 run 淘汰。

Sink 与 compact 不冲突：数据 Sink 后仍可在 SINKED 状态 compact。二者需要串行化的是同一时刻的本地 run 集合变更，而不是生命周期上的先后限制。

#### 2.4.6 失败与日志

Flush/Maintenance worker 捕获运行期 `RuntimeException`，记录失败并在下一个周期或已有 signal 重试；不维护单独 retry timer。积压达到 flow-control 水位后，写入会被 `OVERLOADED` 阻止。

V1 不构建复杂的后台 fatal exception taxonomy。确定性配置、表 profile 与恢复损坏在启动阶段 fail fast；运行期只对已有明确语义的 fatal write 关闭 runtime。Schema 不变属于产品前置条件，不由后台任务主动监控。待故障注入与生产样本证明需要后，再增加少量显式 fatal 类型，而不是按异常消息猜测。

所有实际 Flush 或 Maintenance 动作都使用统一日志标记：

```text
PMS_SCHEDULER_ACTION phase=start|completed|noop|failed
                     worker=flush|maintenance
                     action=...
                     reason=...
```

运维可以直接 grep `PMS_SCHEDULER_ACTION` 重建调度动作序列。

#### 2.4.7 手动管理请求

`POST /flush` 与 `POST /sink` 是异步 fence API：

- `/flush` Freeze 当前边界并返回 HTTP 202、`fenceSequenceId` 和完成字段 `lastFlushedSequenceId`。
- `/sink` Freeze 当前边界，建立/扩展 Paimon fence，并返回 HTTP 202、`fenceSequenceId` 和完成字段 `lastPersistedSequenceId`。
- client 轮询 `/state`，当对应 boundary 大于等于 fence 时视为完成。

V1 不提供同步 drain、回调、task registry 或“停机前全部进入 Paimon”的承诺。异步接口保持实现和故障语义简单，同时仍可观察完成进度。

### 2.5 停机语义

V1 采用 recovery-first shutdown，而不是 Drain/Quiesce/最终 Sink：

```text
RUNNING
  -> SHUTTING_DOWN（立即停止接收新请求）
  -> close HTTP server
  -> close scheduler，停止周期触发并等待已经开始的 worker action
  -> close table service / WAL / local storage / Paimon resources
  -> STOPPED
```

停机不主动 Freeze、Flush 或 Sink，也不建立最终 fence。尚在 Cur/Immutable/NEW 中的数据由 WAL、本地 SST、flush boundary 与 SinkMeta 在下次启动恢复。代价是停机期间 Paimon 可见性可能暂时落后，重启 replay 可能更慢；这对单机 V1 是可接受的，并显著减少停机路径与正常调和循环的重复状态机。

Scheduler `close()` 不设置内部业务超时，而是等待已经开始的单步操作完成，避免在 SST/Paimon publish 中途主动中断。部署系统应在进程级配置 shutdown grace period；超过该时间可以终止进程，恢复协议负责处理边界前后的完整状态。

两个 reconciliation loop 在获取 `BucketStateSnapshot` 后、选择下一动作前再次检查运行状态，以避免被慢快照阻塞的旧 worker pass 在 shutdown 后通常再启动新动作。该检查是 KISS 的 best-effort 生命周期边界，不额外引入全局 action admission lock；与检查真正并发穿过的极小窗口按已经 in-flight 处理，并由 `close()` 等待完成。

### 2.6 状态与可观测性

`GET /state` 汇总：

- core 的 MemTable、NEW/SINKED、sequence boundary、local run 与 Sink flight 状态；
- `writeOverloaded`；
- scheduler 的运行状态、配置、pending Paimon fence 与 worker running 标记；
- runtime 的 `status`、时间戳、最近失败与 recovery summary；
- Paimon lookup cache/view 状态。

age 不存储在 core 快照中；server 在构造 `/state` 响应时由 `observedAtMillis` 与各层 `oldestWriteAtMillis` 即时计算并返回。

## 3. 生产配置

### 3.1 Scheduler 与本地层默认值

| 配置 | 默认值 | 含义 |
|------|--------|------|
| `pms.server.scheduler.flush_reconcile_interval_ms` | `1000` | Flush worker 调和周期 |
| `pms.server.scheduler.maintenance_reconcile_interval_ms` | `30000` | Maintenance worker 调和周期 |
| `pms.paimon.visibility.max_delay_ms` | `600000` | Paimon 正常可见性目标 |
| `pms.memtable.max_entries` | `1000000` | CurMemTable 自动 Freeze 条目水位 |
| `pms.memtable.max_size_mb` | `256` | CurMemTable 自动 Freeze 字节水位 |
| `pms.storage.new_sst.max_count` | `10` | NEW run 维护目标 |
| `pms.storage.sinked_sst.max_count` | `10` | 本地保留 SINKED run 维护目标 |
| `pms.operation.sink.batch_max_bytes_mb` | `1024` | 单次 Sink 输入上限 |
| `pms.operation.compact.max_input_size_mb` | `1024` | 单次 local compact 输入上限 |
| `pms.flowcontrol.overloaded_immutable_count` | `4` | Immutable write overload 水位 |
| `pms.flowcontrol.overloaded_pending_sst_count` | `20` | NEW write overload 水位 |

约束：

- `new_sst.max_count` 必须严格小于 `overloaded_pending_sst_count`，给后台维护保留缓冲区。
- 所有周期、目标和字节上限必须为正数。
- `pms.wal.dir` 与 `pms.storage.dir` 必须不同；lookup cache 也不得位于 WAL/storage 或本地 warehouse 子目录内。
- V1 不在进程内根据磁盘 free space 调度。建议 storage 使用独立卷或明确 quota；按默认 `10 × 1 GiB` SINKED 窗口并考虑 NEW、compact 临时输出及余量，建议至少约 24 GiB，可优先配置 32 GiB，并由外部系统在剩余空间低于约 4 GiB 时告警。

### 3.2 已移除的调度配置

以下键不再受支持，`ConfigManager` 发现后直接拒绝启动，避免旧配置被静默忽略：

```text
pms.server.scheduler.enabled
pms.server.scheduler.failure_retry_delay_ms
pms.server.scheduler.flush_interval_ms
pms.server.scheduler.sink_interval_ms
pms.sink.interval_ms
pms.sink.max_pending_ssts
pms.storage.sinked_max_size_mb
pms.storage.sinked_max_count
pms.storage.local_sst_max_rows
pms.operation.sink.batch_max_ssts
pms.storage.compact_threshold_mb
pms.storage.compact_min_files
```

完整可运行样例见 `pms-server/src/main/resources/pms-server-example.properties`；协议与 lookup 配置见 [pms-server README](../pms-server/README.md)。

## 4. 暂缓项

以下内容不属于当前 MVP 完成条件：

- 故障注入矩阵与长时间 benchmark/默认值校准。
- 基于磁盘剩余空间、精确 SST 总字节数或查询放大的动态调度。
- 同步管理命令、回调和持久化 task registry。
- 运行期完整 fatal exception taxonomy。
- Paimon 显式 compaction scheduler。

其中故障注入与 benchmark 会在当前结构稳定后单独执行，不影响本文件所述 V1 状态与接口契约。
