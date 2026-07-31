# PMS Testing Strategy 设计文档

## 1. 测试分层

```
┌──────────────────────────────────────────────────┐
│  Level 2: Integration Test (端到端)               │
│  2PC 完整流程 / WAL 崩溃恢复 / 跨层查询穿透        │
│  多组件交互 / Sink 流程状态机                      │
├──────────────────────────────────────────────────┤
│  Level 1: Unit Test (单组件)                      │
│  SkipList 读写 / SST 编解码 / WAL 读写 /          │
│  BloomFilter / RowCodec                           │
└──────────────────────────────────────────────────┘
```

---

## 2. Level 1: Unit Test

单组件逻辑正确性验证，无外部依赖，纯内存测试。

### 2.1 测试范围

| 组件 | 测试要点 |
|------|---------|
| CurMemTable | 并发 put/get、容量阈值触发 freeze |
| ImmutableMemTable | Freeze 后只读、sequence/oldest-write 边界、Flush 发布后退出查询路径 |
| LocalStorageManager | SST 写入 → 读取一致性、BloomFilter、`Optional<Value>` 三态、read epoch 延迟删除、多路归并保留最新 Key；锁外 Flush 准备期间旧可见快照不阻塞，发布后的 meta 始终存在 cached reader；flushId 失败复用、retired data-first 删除重试与启动恢复规划 |
| WALManager | 单盘写入 → 读取、CRC 校验正确性、Magic 检测 partial write |
| RowCodec / PrimaryKeyCodec | `InternalRow` 编码 → 解码往返正确性、主键编码顺序一致性、非法格式与不支持类型拒绝 |
| BloomFilter | 假阳性率在预期范围内（如 < 1%）、不同 FPP 配置的效果 |
| Flink Connector | Factory/options、schema/type adapter、primary-key selector、DELETE filter、Lookup key 计划、object reuse |

### 2.2 测试工具

- JUnit 5
- AssertJ（流式断言）
- 无需 Mock 框架——核心组件接口清晰，直接构造测试实现即可

---

## 3. Level 2: Integration Test

端到端和多组件交互的正确性验证。

### 3.1 多组件交互测试

| 场景 | 验证点 |
|------|--------|
| 写入 → Freeze → Flush | curMemTable 数据正确转移为 SST，查询结果不变 |
| 多次 Freeze + 并发查询 | 查询穿透多层 ImmutableMemTable，数据不丢失不重复 |
| Freeze/Flush 并发查询 | 对象切换和“目标先发布、源后移除”不产生瞬时 MISS，lookup 不获取写锁 |
| Flush boundary 在线重试 | SST 已发布但 boundary fail-once 时复用同一个 FlushFlight，成功后只存在一个 local run |
| Sink 流程状态机 | 最老连续 NEW 前缀 → SINKED，固定 sequence fence 可跨多个有界 batch 推进 |
| Prepared Sink 在线恢复 | commit 临时失败后不重启即可重试同一 durable prepare，且不准备新 batch |
| Committed Sink 在线收尾 | durable success 后 SST metadata fail-once 进入 FINALIZING；重试只完成同一 batch 的本地状态，不再次 Paimon commit |
| Sink metadata 部分写 | 多个 SST metadata 逐个标记时部分成功，重试后全部幂等收敛为 SINKED |
| 流控水位线 | server 快速检查与 core WAL 前复查都能返回 OVERLOADED，拒绝批次不进入 WAL |
| 本地 SST 合并 | 只合并同状态连续 run；NEW/SINKED 均可合并，查询与恢复边界不变 |
| SST 淘汰 | 只淘汰最老 SINKED run，read epoch 结束后才物理删除；data/meta 全部成功前保留 cleanup entry |
| SST 半删除恢复 | compact 覆盖的输入和最老 evict meta-only 残留可恢复并清理；保留后缀内部缺口 fatal |
| flushId 连续性 | SST 写入失败、boundary orphan 重启均复用原 ID；本地 cache 全淘汰后允许从 1 建立新连续序列 |
| WAL 截断 | SinkMeta success 的 `persistedSequenceId` 覆盖旧 WAL 文件时正确删除；删除失败的候选可由后续 truncate 重试 |
| Scheduler 优先级 | prepared retry/finalizing、可见性 fence、NEW/SINKED 数量维护按固定优先级单步调和，progress 后重新采样 |
| Scheduler 生命周期 | 周期信号合并、64-action slice 重排队、close 停止 delayed/periodic task 并等待在途 action；Flush/Maintenance 在慢快照返回后复查 running，不从旧 pass 启动新动作 |
| 手动 fence | `/flush`、`/sink` 返回 202；轮询 state boundary 可观察完成，不执行同步 drain |

使用 mock/fake 实现替代 Paimon API：

```java
class FakeSinkManager implements SinkManager {
    // 不依赖真实 Paimon，模拟 prepare/commit 行为
    // 可注入延迟、失败等异常场景
}
```

### 3.2 端到端测试

需要真实的 Paimon 环境（本地文件系统即可）。

| 场景 | 验证点 |
|------|--------|
| 完整写入路径 | Client 序列化 → RPC → MemTable → SST → Paimon 2PC → 数据可查 |
| 完整查询路径 | 各本地层命中 + `pms-lookup-paimon` 的 HIT/DELETED/MISS/UNKNOWN 语义正确；`ReadBuilder` 仅作为测试对照 |
| WAL 崩溃恢复 - 正常 | Kill → 重启 → 数据完整、不重复提交 |
| SinkMeta 崩溃恢复 - prepare 后 | Kill → 重启 → 使用 SinkMeta 中的 prepared payload、batch 和 fileRefs 重试 commit |
| SinkMeta 崩溃恢复 - success 后 SSTMeta 未更新 | Kill → 重启 → 通过 SinkMeta success 推导 sinkedSST，并修正 SST metadata state |
| Recovery-first 停机 | 停止 HTTP 与 scheduler，不强制 Freeze/Flush/Sink；重启后由 WAL/SST/SinkMeta 恢复数据 |

### 3.3 测试基础设施

```java
class PMSTestCluster implements AutoCloseable {
    // 一键搭建本地 PMS + Paimon 测试环境
    PMSClient client();
    PaimonTable paimonTable();
    void killPMS();  // 模拟进程崩溃
}
```

### 3.4 Flink Connector 集成测试

`flink-connector-pms` 以 Flink `1.20.3` 为固定测试基线，分三层验证：

| 层次 | 验证点 |
|------|--------|
| 轻量 wire integration | HTTP 测试服务继续使用真实 PMS handshake、key/row/batch codec，覆盖 Sink 与 sync/async Lookup |
| Planner / Factory | SQL Factory discovery、Sink plan、完整主键 direct DELETE、扫描型 DELETE 拒绝 |
| 真实端到端 | Flink MiniCluster + 真实 PMS Server + 本地 Paimon，覆盖 SQL Sink、processing-time Lookup Join、flush/sink 与 tombstone |

Connector 发布包还需在 `package` 后检查：不包含 Flink class、不包含未 relocation 的
Paimon class、保留 Factory service，并能直接从 shaded JAR 完成 `ServiceLoader`
发现。当前由发布前手工 smoke 执行，接入 CI 后再迁移到独立集成测试 pipeline。详细
契约和长期故障矩阵见 [flink-connector-pms.md](flink-connector-pms.md)。

---

## 4. Benchmark

性能验证不放入常规单元/集成测试链路, 独立由 `pms-benchmark` 模块承载. 第一阶段先实现 `pms-core`
的 db_bench-like macro benchmark, 直接调用内部 byte-oriented API, 用于建立 PMS 本地 LSM/KV 能力基线.

详细说明见 [pms-benchmark.md](pms-benchmark.md)。

## 5. 延后验证

Scheduler 的确定性状态机、并发边界和 API 语义应进入常规单元/集成测试。以下高成本工作按当前计划延后，不作为本轮文档合并的完成门槛：

- 在 Flush data/meta/boundary、Sink prepare/commit/success、compact publish 与 epoch retire 各边界进行进程级故障注入。
- 长时间混合写入/查询/Sink 压测。
- 基于真实文件大小、Paimon snapshot 频率与恢复耗时校准默认参数。

延后不代表改变恢复契约；在完成故障注入前，不应把当前默认值描述为最终容量规划结论。
