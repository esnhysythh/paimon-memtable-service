# pms-integration-tests 模块设计

## 1. 模块定位

`pms-integration-tests` 是 PMS 的远端环境正确性测试模块。它在独立 JVM 中启动当前提交构建的
PMS server，通过真实 client 访问服务，并把 Paimon filesystem Catalog 指向外部 HDFS，以验证
从协议入口到 HDFS data file 的完整链路。

该模块回答“当前 PMS build 能否在声明的环境中正确运行”，不承担系统性能基线。测试数据规模
有限、操作序列确定、结果具有明确 pass/fail；大规模数据和吞吐/延迟实验由后续
`pms-benchmark` 负责。

## 2. 被测边界

```text
JUnit/Failsafe
  -> pms-testkit
     -> PmsClient -> HTTP/2 h2c -> independent PmsServerMain JVM
                                      -> WAL / local SST / SinkMeta
                                      -> Paimon 1.4.1 client
                                      -> remote HDFS 3.4.3
     -> independent Paimon Catalog + ReadBuilder (oracle)
```

测试包含当前仓库构建的 client、protocol、codec、server、sink、lookup 和 Hadoop/Paimon runtime
closure。Hadoop 3.4.3 CLI 只用于人工环境预检，不参与自动测试的业务操作。

## 3. 首套环境基线

| 项目 | 基线 |
|------|------|
| 驱动主机 | macOS |
| Maven/Test/PMS JVM | Temurin Java 17 |
| 独立 Hadoop CLI | Hadoop 3.4.3 + Corretto Java 8，仅诊断使用 |
| HDFS 服务端 | Apache Hadoop 3.4.3，单 NameNode、单 DataNode |
| HDFS 模式 | 非 HA、无 Federation、`dfs.replication=1` |
| 安全 | simple authentication，无 Kerberos |
| PMS Paimon/Hadoop client | Paimon 1.4.1、Hadoop 2.8.5 |

首套环境用于验证 Hadoop 2.8.5 client 与 HDFS 3.4.3 server 的实际兼容性。通过结果只代表该
单节点环境，不外推到 HA、Kerberos、多副本或生产吞吐。

Hadoop XML 保留集群原始 hostname，不在测试副本中机械替换为 IP。运行测试的 Mac 必须能通过
DNS 或明确的 `/etc/hosts` 条目解析 NameNode 和 NameNode 返回的 DataNode advertised hostname；
只替换 `fs.defaultFS` 等入口地址并不能保证 block transfer 可达。Preflight 的 write/read/rename
用于同时验证 metadata 和 DataNode 数据通路。

## 4. Maven 生命周期

测试类命名为 `*IT.java`，由 Failsafe 执行。模块进入正常 reactor 编译，但远端 profile 默认关闭：

```bash
export JAVA_HOME=/path/to/temurin-17/Contents/Home

mvn -Premote-hdfs-it \
  -pl pms-tests/pms-integration-tests -am verify \
  -Dpms.it.pms.java.home="$JAVA_HOME" \
  -Dpms.it.hadoop.conf.dir=/path/to/hadoop-3.4.3/etc/hadoop \
  -Dpms.it.local.root=/absolute/path/to/pms-it/runs \
  -Dpms.it.hdfs.root=hdfs://namenode/tmp/pms-it-user
```

仓库提供了全部为注释状态的
[`remote-hdfs-it.env.example`](../../pms-tests/pms-integration-tests/conf/remote-hdfs-it.env.example)
环境模板。复制到仓库外、填写并显式 `source` 后，可使用模板末尾的 Maven 命令；模板本身不会因误
加载而访问集群。

`mvn test` 和未激活 profile 的 `mvn verify` 不读取这些参数、不连接 HDFS、不启动 PMS 子进程。
Profile 激活后缺少任何必需参数都在创建远端资源前失败。

PMS runtime classpath 取自 Failsafe fork 的完整 test classpath，其中包含 reactor 内当前提交的
`pms-server` 及其 runtime dependencies；不要求预先发布或 `mvn install` 当前 SNAPSHOT。

## 5. 测试资源模型

每个 test class 或隔离场景使用一个 `PmsTestRun`：

```text
local: <pms.it.local.root>/<runId>/
hdfs:  <pms.it.hdfs.root>/<runId>/warehouse/
table: <generated_database>.<scenario_table>
```

一个 run 可以包含多个本地 phase。恢复测试复用同一 phase 的 WAL/storage/cache；历史查询使用新的
phase，从而保证同一远端表只有一个 writer，并且不会错误复用原进程的本地状态。

同一张表任意时刻只有一个 PMS writer。需要使用全新本地状态验证历史 lookup 时，先完全停止
原进程，再为同一远端表启动新的 PMS run phase；不会并行启动两个 writer。

默认 `cleanup=on-success`。成功场景删除本次 run，失败场景保留日志和数据。远端根目录应由用户
预先分配；当前 simple-auth 用户不需要也不假定 HDFS superuser 权限。

## 6. 测试表与数据集

首版使用一张最小主键表：

```text
id       BIGINT NOT NULL
payload  STRING
version  INT

primary key: id
bucket: 1
merge-engine: deduplicate
file.format: parquet
```

每个场景使用固定 seed 和明确规模。首版 smoke 使用 1,000 行，recovery 使用 500 行，足以跨多个
client batch 以及 freeze、flush 和 sink 边界，同时允许 verifier 维护完整 reference state。测试不根据
机器速度调整数据或断言。

数据阶段包括：

1. 初始唯一主键 insert；
2. 对固定子集 update，提高 version；
3. 对固定子集 delete；
4. 生成最终 `Map<id, record>` 作为期望状态。

## 7. 环境预检

`RemoteHdfsPreflightIT` 在创建 PMS 表前验证：

- Java 17 和 server classpath 可用；
- Hadoop XML 可读，Paimon 能加载 `hadoop-conf-dir`；
- HDFS 根的 run 子目录可以 create/write/read/rename/delete；
- 独立 Catalog 可以创建预检 database/table，并由 run 生命周期安全清理；
- 操作使用的有效 HDFS URI 与用户显式配置一致。

预检只操作自己的 run 路径。失败时将异常归类为环境问题，不继续运行 PMS 场景。

## 8. 测试矩阵

### 8.1 `RemoteHdfsSmokeIT`

#### 启动与协议

- fixture 先创建 Paimon database/table，PMS 不自动建表；
- 独立 PMS JVM 在 loopback 地址启动；
- `PmsClient` 完成 HTTP/2 handshake；
- table schema、primary key 和 codec version 与 fixture 一致。

#### 当前态读写

- 批量 insert 后所有 key 返回 HIT 和正确 row；
- update 后返回最高 version；
- delete 后 full/local lookup 遵守 tombstone 语义；
- 未写入 key 返回 MISS，而不是错误或伪 HIT。

#### Flush 与 Sink

- `/flush` 返回 fence，`lastFlushedSequenceId` 最终达到 fence；
- `/sink` 返回 fence，`lastPersistedSequenceId` 最终达到 fence；
- NEW/SINKED、本地行数和 runtime status 与完成状态一致；
- 独立 `PaimonVerifier` 读取到和 reference state 完全相同的最终主键集合。

#### 历史查询

- 停止原 PMS；
- 使用同一远端表和全新 WAL/storage/cache 启动 PMS；
- local lookup 为 MISS，full lookup 从远端 Paimon 返回最终 HIT 或无数据状态；
  历史 tombstone 可能被保留（DELETED）或被压实清除（MISS），两者都必须 status=OK 且无 row；
- 已删除主键不能从旧 snapshot/file 中重新出现。

#### 全新本地状态接续写入

- 新 phase 的持久化 writer identity 与原 phase 不同，配置的 prefix 保持一致；
- 在新 phase 更新已有 key、删除另一个已有 key、插入新 key；
- 新本地 sequence 从 1 开始，sink 后 assigned/persisted 均为 3；
- 独立 Paimon 全量扫描确认三条新操作都生效，未被旧 commit identifier 静默跳过。

### 8.2 `RemoteHdfsRecoveryIT`

#### 未 sink WAL 恢复

- 写入成功响应后，不调用 flush/sink；检查 assigned 等于确认操作数、flushed/persisted 为 0，
  且远端仍为空，再强制终止进程；
- 使用相同 WAL/storage/cache 重启；
- 所有已确认写入恢复，稳定的 runtime.recovery 重放数量与写入数一致，sequence 无额外增长；
- 同一本地状态重启保持持久化 writer identity 不变；
- 恢复后 sink，Paimon verifier 与 reference state 一致。

#### update/delete 恢复

- 先把初始值 sink 到 Paimon；
- 执行 update/delete 后先验证 full lookup 已覆盖旧值，而 Paimon 全量扫描仍为初始状态；
- 检查 flushed/persisted 仍停留在初始批边界后强制终止；
- 重启后的重放数等于 update/delete 操作数；对所有存活 key 和已删 key 验证 full/local lookup；
- 再次 sink 后 verifier 只看到最终状态。

#### 已持久化状态重启

- sink fence 完成后强制终止并重启；
- runtime.recovery 重放数为 0，sequence 保持最终边界；
- 不出现重复主键或无法解释的新 snapshot commit；
- lookup view 能从 snapshot 重建并正常服务。

首版不依赖 sleep 猜测 prepare/commit 的瞬时窗口。精确的 2PC 中间态进程故障需要可控 failpoint，
后续单独设计；现有 fake/mock 测试继续覆盖这些状态机分支。

## 9. 正确性 oracle

每个场景使用三层断言：

1. `PmsClient` 的写入结果和 HIT/DELETED/MISS。
2. `/state` 的 fence、SST、runtime/recovery 状态。
3. 独立 Paimon Catalog/Table `ReadBuilder` 的最终行集合。

Reference state 按已确认操作更新。发生超时或异常时不继续推导期望状态。Paimon 对照在 sink fence
完成后重新打开 Catalog/Table，避免沿用 PMS 或旧 verifier 的内存 view。

## 10. 超时与异步等待

所有异步断言使用有限 deadline 轮询，不使用固定长 sleep：

- 进程 ready/handshake：默认 60 秒；
- flush/sink/recovery：默认 120 秒；
- 单次 client 读写：沿用 `PmsClientConfig` 的有限 timeout；
- 子进程正常停止：使用 operation timeout；超时或中断后强制回收，但保留原始失败；
- 崩溃场景明确使用强制终止并等待进程退出。

这些测试依靠显式管理 fence 驱动 flush/sink，后台 visibility delay 设置为 1 小时，避免慢环境
抢先 sink；强杀前仍检查状态边界，若条件不成立则失败，不用延时参数代替证明。

进程启动和管理操作的失败消息分别包含 runId/日志、fence/最后状态等诊断信息。

## 11. 报告与失败现场

Failsafe XML 报告之外，每个 run 保存：

```text
run-manifest.json
conf/pms-server.properties
logs/pms-server.out
last-state.json
```

Manifest 记录 Maven 构建的 project/Paimon/Hadoop 版本、执行时 Git commit/dirty、test JVM、
HDFS URI、表名、seed、
数据规模和 cleanup policy，不复制 Hadoop XML 内容。各 phase 的完整 PMS 配置、PID 生命周期、日志
和最后 state 分文件保存。关键恢复边界和 sink 状态也写入 manifest。

`<pms.it.local.root>/reports/<runId>.json` 在 run 结束后保留小型报告，记录上述信息、完成时间、
成功标记和清理结果。成功清理不会删除 reports；测试断言失败详情以 Failsafe XML 为准。

失败归类为环境、远端存储、Paimon、PMS 启动/恢复、协议或数据不一致。保留现场后，用户可以用
manifest 中的 HDFS run URI 和本地日志独立排查。

## 12. 发布判定

MVP 远端 HDFS 正确性门槛：

- preflight、smoke、recovery 场景全部通过；
- 同一环境连续多轮运行无偶发失败；
- 没有遗留 PMS 子进程；
- 没有数据丢失、重复主键、tombstone 穿透或无法解释的重复提交；
- 成功 run 可安全清理，失败 run 可复现和定位。

吞吐和延迟只记录诊断值，不在本模块设定正式 SLA。单节点 HDFS 结果不代表 HA、Kerberos、
多副本、Linux 发行包或生产容量已经验证。

## 13. 后续扩展

- 复合主键和分区表；
- 更多受支持 Paimon 类型；
- 可控进程级 2PC failpoint；
- Kafka + Flink Connector E2E；
- HA 环境矩阵；
- **TODO：Kerberos 安全集群支持**，包括 keytab/krb5 配置注入、ticket 生命周期和凭据脱敏；MVP
  不提供开箱即用支持；
- 与 `pms-benchmark` 共享更大数据集和 workload 描述。
