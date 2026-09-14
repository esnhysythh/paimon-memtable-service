# PMS 测试策略与测试子项目设计

## 1. 文档定位

本文档是 PMS 全项目测试策略的唯一总入口，同时定义 `pms-tests` 测试子项目的模块边界、构建
生命周期和通用设计原则。各测试模块的 API、环境和场景细节由独立模块文档描述，不在总览中重复。

PMS V1 的测试体系需要回答三类问题：

1. 单个组件和状态机是否满足确定性契约；
2. 当前提交能否通过真实协议、独立进程和 Paimon/HDFS 完成端到端数据闭环；
3. 在正确性成立后，系统在明确 workload 下具有什么容量、吞吐和延迟特征。

正确性测试是发布门槛。Benchmark 独立运行并保留结果，在建立稳定环境和基线前不把瞬时性能数字
作为普通 Maven 构建的 pass/fail 条件。

## 2. 测试分层

```text
┌──────────────────────────────────────────────────────────┐
│ Benchmark：本地 core / Paimon lookup；远端 workload 后续 │
├──────────────────────────────────────────────────────────┤
│ Integration：多组件状态机、本地 E2E、远端 HDFS、恢复     │
├──────────────────────────────────────────────────────────┤
│ Unit：单组件、格式、边界条件和确定性故障分支              │
└──────────────────────────────────────────────────────────┘
```

各层共享生产 API 和必要的测试数据模型，但不共享 runner 或通过标准：

- Unit test 应快速、隔离且不依赖网络和外部服务；
- Integration test 使用确定性数据和有限超时给出明确正确性结论；
- Benchmark 使用独立 warmup/measurement 生命周期和性能报告，不伪装成细粒度 JUnit 性能断言。

## 3. Unit Test

单元测试覆盖单组件逻辑、持久化格式和可注入的异常分支，不访问远端 HDFS。

| 组件 | 主要验证点 |
|------|------------|
| CurMemTable | 并发 put/get、sequence、容量阈值与 freeze |
| ImmutableMemTable | Freeze 后只读、边界统计、Flush 发布后退出查询路径 |
| LocalStorageManager | SST 往返、BloomFilter、查询三态、read epoch 延迟删除、多路归并、flushId 与半删除恢复 |
| WALManager | 写入/回放、CRC、partial write、截断与删除重试 |
| Bucket Director | Flush/Sink/Compact/Evict 状态机、durable boundary、幂等恢复与流控 |
| Row/PrimaryKey Codec | Paimon `InternalRow` 编解码、mem-comparable 主键顺序、非法格式与类型拒绝 |
| Paimon Sink/Lookup | 2PC payload、RowKind/tombstone、snapshot/file view、HIT/DELETED/MISS/UNKNOWN |
| Protocol/Client | handshake、binary batch envelope、HTTP/2 状态映射、重试与 schema negotiation |
| Flink Connector | Factory/options、type adapter、primary-key selector、DELETE filter、lookup key 计划、object reuse |
| Testkit | 环境拒绝规则、owner marker、清理策略、确定性数据、PMS 配置和 admin fence 解析 |

单元测试统一使用 JUnit 5。核心接口优先使用小型 fake 实现覆盖异常和状态分支，不为简单协作关系引入
重量级 mock 框架。

## 4. Integration Test

### 4.1 多组件状态机测试

这类测试在单个测试 JVM 或本地临时目录中组合真实 PMS 组件，对难以通过远端黑盒稳定制造的状态
边界进行确定性验证。

| 场景 | 验证点 |
|------|--------|
| 写入 → Freeze → Flush | curMemTable 数据正确转移为 SST，查询结果不变 |
| 多次 Freeze + 并发查询 | 查询穿透多个本地层，数据不丢失、不重复 |
| Freeze/Flush 并发查询 | “目标先发布、源后移除”不产生瞬时 MISS，lookup 不获取写锁 |
| Flush boundary 在线重试 | SST 已发布但 boundary fail-once 时复用同一个 FlushFlight |
| Sink 流程 | 最老连续 NEW 前缀变为 SINKED，固定 sequence fence 可跨有界 batch 推进 |
| Prepared/Finalizing 在线恢复 | 重试同一个 durable sink flight，不重复 prepare 或 Paimon commit |
| Sink metadata 部分写 | 部分成功后重试，所有 SST metadata 幂等收敛 |
| 流控水位线 | server 快速检查和 core WAL 前复查均能拒绝过载批次 |
| 本地 SST 合并与淘汰 | 只处理合法连续 run，read epoch 结束后再物理删除 |
| SST 半删除恢复 | 可恢复合法尾部残留，保留区间内部缺口必须 fatal |
| WAL 截断 | persisted boundary 覆盖旧 WAL 时删除，失败候选可在后续重试 |
| Scheduler | 固定优先级、单步调和、任务合并、重排队与 close 生命周期 |
| 手动 fence | `/flush`、`/sink` 返回 fence，通过 state boundary 观察异步完成 |

### 4.2 本地端到端测试

本地端到端测试使用真实 Paimon filesystem Catalog 和 PMS HTTP 服务，但 warehouse 位于临时本地
文件系统，适合进入普通 Maven 生命周期。

| 场景 | 验证点 |
|------|--------|
| 完整写入路径 | Client → HTTP/2 → MemTable → SST → Paimon 2PC → 独立读取 |
| 完整查询路径 | 本地层命中、Paimon 历史 lookup 和 tombstone 穿透边界 |
| WAL/SST 恢复 | 重启后已确认写入完整且不重复提交 |
| SinkMeta prepare/success 恢复 | 分别从 prepared payload 和 durable success 完成收敛 |
| Recovery-first 停机 | 停止入口和 scheduler 后直接关闭，由 WAL/SST/SinkMeta 在重启时恢复 |

### 4.3 远端 HDFS 集成测试

远端测试由 `pms-tests/pms-integration-tests` 承载。它通过真实 `PmsClient` 访问独立
`PmsServerMain` JVM，并使用新的 Paimon Catalog/Table 实例作为 oracle，覆盖：

- HDFS metadata 与 DataNode 数据通路预检；
- insert/update/delete、flush、sink 和独立 Paimon 最终状态；
- 使用全新本地状态执行历史查询及接续写入，验证 writer identity 隔离；
- 未 sink WAL、update/delete 以及已持久化状态的进程级 kill/restart。

远端 profile 默认关闭，不进入普通 `mvn test`。环境基线、参数、资源安全边界和完整测试矩阵见
[pms-integration-tests 模块设计](pms-integration-tests.md)。

### 4.4 Flink Connector 集成测试

`flink-connector-pms` 以 Flink `1.20.3` 为固定测试基线：

| 层次 | 验证点 |
|------|--------|
| 轻量 wire integration | 真实 handshake/key/row/batch codec，覆盖 Sink 与 sync/async Lookup |
| Planner / Factory | SQL Factory discovery、Sink plan、完整主键 direct DELETE、扫描型 DELETE 拒绝 |
| 真实端到端 | Flink MiniCluster + PMS + Paimon，覆盖 SQL Sink、Lookup Join、flush/sink 与 tombstone |

Connector 发布包还需检查不包含 Flink class、不包含未 relocation 的 Paimon class、保留 Factory
service，并能从 shaded JAR 完成 `ServiceLoader` 发现。详细契约见
[flink-connector-pms.md](../flink-connector-pms.md)。Kafka + Flink 远端环境在 HDFS 集成测试稳定后接入。

## 5. `pms-tests` 子项目

### 5.1 定位与结构

`pms-tests` 承载不属于生产运行时的跨模块 testkit、远端环境集成测试和后续系统 benchmark。它与
生产模块使用同一个 Git 版本和 Maven reactor，但不进入 `pms-dist`，也不成为 server/client 的
传递依赖。

```text
pms-tests/
├── pom.xml
├── pms-testkit/
├── pms-integration-tests/
└── pms-benchmark/            # 本地 core / Paimon lookup 基准
```

| 模块 | 类型 | 职责 | 状态 |
|------|------|------|------|
| `pms-testkit` | 普通 JAR | 环境解析、run 所有权、Paimon fixture/verifier、PMS 子进程、确定性数据 | 已实现 |
| `pms-integration-tests` | Maven 测试模块 | 远端 HDFS 正确性、sink、lookup 和恢复测试 | 首轮验收及连续 3 轮通过，见 [验收记录](pms-remote-hdfs-acceptance-2026-09-11.md) |
| `pms-benchmark` | 可执行程序 | 本地 core 插入/查询与 Paimon direct/cached 查询；remote workload 后续补充 | 本地版已实现 |

依赖方向固定为：

```text
pms-integration-tests ──────> pms-testkit ──> PMS public modules + Paimon API
pms-benchmark ──────────────> pms-core + pms-lookup-paimon
future remote benchmark ───> pms-testkit

production modules ──X──> pms-tests/*
```

远端共享代码进入 `pms-testkit/src/main/java`。本地基准直接使用对应生产模块。
Integration tests 与 benchmark 不互相依赖，也不通过 Maven test-jar 复用测试源码。

### 5.2 构建与执行

| 命令 | 行为 |
|------|------|
| `mvn test` | 运行常规测试并编译 testkit/远端测试源码，不连接远端 HDFS |
| `mvn verify` | 默认仍不连接远端 HDFS |
| `mvn -Premote-hdfs-it -pl pms-tests/pms-integration-tests -am verify` | 显式执行远端 HDFS 集成测试 |
| benchmark 命令 | 独立 runner JAR / 本地矩阵脚本，不绑定 Surefire/Failsafe |

远端 profile 使用 Maven Failsafe 的 `integration-test` 与 `verify` 阶段。环境准备和清理由 harness
管理，不由静态初始化器或开发者 shell 脚本隐式执行。可复制的参数模板位于
[`remote-hdfs-it.env.example`](../../pms-tests/pms-integration-tests/conf/remote-hdfs-it.env.example)，
其中所有 `export` 默认均被注释。

### 5.3 通用设计原则

- **接近部署边界**：远端测试使用真实 client、独立 PMS JVM 和独立 Paimon oracle。
- **环境显式化**：JDK、Hadoop XML、本地根和 HDFS 根由参数提供，代码没有开发者机器默认路径。
- **资源所有权**：每次执行使用唯一 runId；只有精确路径和 owner marker 同时匹配时才能递归清理。
- **失败可诊断**：默认仅清理成功 run，失败时保留 manifest、PMS properties、state、WAL/SST 和日志。
- **单一 writer**：同一张 Paimon 表任意时刻只启动一个 PMS writer。
- **正确性与性能分离**：两者共享 testkit 中的数据和进程能力，但 runner、生命周期和报告独立。

## 6. Benchmark

性能验证不进入常规单元/集成测试链路。`pms-tests/pms-benchmark` 提供本地 core 基准，以及
真实本地 Paimon 文件的 direct/cached 查询基准。两类入口独立，数据准备不计入查询耗时。
运行结果只作为本地或 CI 产物，不纳入源码提交。当前已提供 core 的 db_bench-like 基线、
独立的数据准备/warmup/measurement、正确性检查与结果输出。后续规划包括：

- 经过 client、独立 PMS JVM 和远端 Paimon/HDFS 的系统 workload；
- 在现有顺序/uniform/hotset 场景上扩充 Zipfian、更多 value size 和多 generation 数据集。

构建、测量口径与当前能力见 [pms-benchmark.md](../pms-benchmark.md)。

## 7. 发布门槛与延后项

MVP 发布前至少要求：

- 常规 unit/local integration 全部通过；
- 远端 HDFS preflight、smoke、recovery 全部通过，并连续多轮无偶发失败；
- 无遗留 PMS 子进程、数据丢失、重复主键或 tombstone 穿透；
- 成功 run 可安全清理，失败 run 能用保留材料定位；
- 使用的 JDK、Paimon/Hadoop client、HDFS 环境和限制被记录。

首轮 HDFS preflight/smoke/recovery 及连续 3 轮已通过，见 [验收记录](pms-remote-hdfs-acceptance-2026-09-11.md)。
该结果是限定环境下的历史验收；源码发布仍需在当前改动上通过本地 `mvn verify`。
远端多 bucket/分区复合主键见 [后续测试计划](pms-next-batch.md)，属于覆盖扩充。

以下工作延后，不作为当前限定范围的 MVP 源码发布条件：

- Flush data/meta/boundary、Sink prepare/commit/success 等精确位置的进程级 failpoint；
- 长时间混合写入/查询/Sink 压测与正式性能 SLA；
- HDFS HA、多副本和 Linux 发行包环境矩阵；
- Kerberos keytab/ticket 生命周期和凭据脱敏支持；
- Kafka + Flink Connector 远端 E2E。

延后不改变已有恢复契约，也不能把当前单节点 HDFS 结果外推为完整生产环境认证。

## 8. 模块文档

| 文档 | 内容 |
|------|------|
| [pms-testkit.md](pms-testkit.md) | testkit API、组件和资源生命周期 |
| [pms-integration-tests.md](pms-integration-tests.md) | 远端 HDFS 环境、场景、执行和失败诊断 |
| [pms-benchmark.md](../pms-benchmark.md) | 本地 benchmark 运行、测量口径和后续规划 |
