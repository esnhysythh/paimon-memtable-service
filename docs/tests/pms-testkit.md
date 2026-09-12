# pms-testkit 模块设计

## 1. 模块定位

`pms-testkit` 是 `pms-tests` 的共享基础设施 JAR。它把远端环境解析、测试资源所有权、Paimon
测试表、独立 verifier、PMS 子进程和管理 API 等通用能力封装为可组合 Java API，供
`pms-integration-tests` 与后续 `pms-benchmark` 使用。

本模块不是通用 PMS SDK，也不是生产部署组件。它不会被 `pms-server`、`pms-client` 或
`pms-dist` 依赖和发布。

## 2. 设计目标

- 当前 harness 在远端 HDFS 上创建隔离的 Paimon 测试表；本地 Paimon 测试由各模块现有测试承担。
- 使用独立 JVM 启停 PMS，支持正常停止与不执行 shutdown hook 的强制终止。
- 通过当前仓库的真实 `pms-client` 完成协议访问，通过独立 Paimon API 完成正确性校验。
- 资源创建、保留和清理由显式 owner marker 保护，失败时提供足够诊断材料。
- 所有随机数据由 seed 决定，为集成测试和后续 benchmark 提供一致的数据模型。
- testkit API 不依赖 JUnit，使 CLI benchmark 可以直接复用。

非目标：

- 不在本模块中定义 JUnit test case 或性能通过阈值。
- 不模拟 HDFS、Paimon、HTTP/2 或 PMS 进程。
- 不负责部署或管理远端 HDFS、Kerberos、Flink、Kafka。
- 不暴露 `pms-core` 内部对象作为远端测试接口。

## 3. 包与组件

当前包结构：

```text
org.qwh.pms.testkit
├── environment/
│   ├── PmsTestEnvironment
│   └── CleanupPolicy
├── run/
│   └── PmsTestRun
├── paimon/
│   ├── PaimonTestTable
│   ├── PaimonTestTableSpec
│   └── PaimonVerifier
├── process/
│   ├── PmsProcess
│   └── PmsProcessConfig
├── client/
│   └── PmsAdminClient
└── data/
    ├── TestRecord
    ├── TestDataSet
    └── DeterministicDataGenerator
```

## 4. 环境模型

`PmsTestEnvironment` 是不可变配置对象，由系统属性构造。它只描述外部环境，不创建资源：

| 属性 | 类型 | 含义 |
|------|------|------|
| `pms.it.pms.java.home` | 本地绝对路径 | 启动 PMS 子进程的 Java 17 |
| `pms.it.hadoop.conf.dir` | 本地绝对路径 | 包含 `core-site.xml`、`hdfs-site.xml` 的目录 |
| `pms.it.local.root` | 本地绝对路径 | 所有本地 run 的父目录 |
| `pms.it.hdfs.root` | HDFS URI | 所有远端 run 的专用父目录 |
| `pms.it.server.classpath` | classpath 字符串 | PMS 子进程的 runtime classpath；Failsafe 下默认取测试 classpath |
| `pms.it.cleanup` | enum | `on-success`、`always`、`never` |
| `pms.it.start.timeout` | Duration | PMS ready/handshake 超时 |
| `pms.it.operation.timeout` | Duration | flush/sink/recovery 等操作超时 |

解析阶段验证：

- Java executable 存在，`java.specification.version` 为 17。
- Hadoop 配置文件存在且可读。
- 本地根目录是绝对路径，且不等于文件系统根或用户 home 本身。
- HDFS URI 使用 `hdfs` scheme，包含非根路径；测试根不能等于 `/`、`/tmp`、`/user`。
- timeout 为正数，cleanup 值合法。

具体机器路径不出现在代码默认值中。测试 profile 未启用时，不构造该对象。

## 5. Run 与资源所有权

`PmsTestRun` 表示一次测试执行，创建后拥有唯一 `runId`：

```text
<local-root>/<runId>/
├── phases/<phase>/
│   ├── wal/
│   ├── storage/
│   ├── lookup-cache/
│   └── tmp/
├── conf/pms-server-<phase>.properties
├── logs/<phase>.out
├── run/pms-server-<phase>.pid
├── run-manifest.json
├── last-state.json
└── .pms-it-owner

<hdfs-root>/<runId>/
├── warehouse/
└── .pms-it-owner
```

marker 采用两行固定文本，保存 format version 和 runId；读取时保留原始换行，不能使用会拼接行的
Paimon `readFileUtf8()`。创建时间等诊断信息放在 manifest 中。清理
流程重新读取 marker 并进行完整内容比较，不能仅凭路径前缀执行递归删除。

状态转换：

```text
NEW -> LOCAL_READY -> REMOTE_READY -> TABLE_READY -> RUNNING -> FINISHED
                                                       |
                                                       -> FAILED
```

关闭 run 时先停止其 PMS 子进程，再按 cleanup policy 处理资源。`on-success` 在失败时保留本地
日志、WAL/SST/SinkMeta 和远端 warehouse；成功时只删除当前 run 的两个目录。

## 6. Paimon 测试表

`PaimonTestTable` 使用 filesystem Catalog 创建和持有一张测试表。Catalog options 至少包括：

- `warehouse=<hdfs-root>/<runId>/warehouse`
- `hadoop-conf-dir=<pms.it.hadoop.conf.dir>`

首版 `PaimonTestTableSpec` 固定为 PMS 已验证 profile：

```text
id       BIGINT NOT NULL
payload  STRING
version  INT

primary key: id
partition: none
bucket: 1
merge-engine: deduplicate
file.format: parquet
```

database/table 名由 runId 派生并满足 Paimon identifier 规则。创建时不接管同名资源；如果 database
或 table 已存在，run 失败并保留现场。

`PaimonVerifier` 从新的 Catalog/Table 实例读取当前 snapshot，通过 `ReadBuilder` 得到最终主键
集合。它不调用 PMS lookup 模块，以避免被测实现与 oracle 共享同一查询路径。

## 7. 测试数据

首版数据模型使用 `TestRecord(id, payload, version)`，生成器接受 record count、payload size 和
random seed。相同配置必须生成字节级一致的行序列。

集成测试只需要：

- 顺序唯一主键数据；
- 对已有主键的确定性 update；
- 确定性 delete 集合；
- 可以在内存中保存的最终 reference state。

后续 benchmark 在此模型上增加 uniform、Zipfian、hotset、latest、value size/compressibility 和
多 generation 演进。testkit 不负责 warmup、并发压测或 Histogram。

## 8. PMS 子进程

`PmsProcess` 通过 `ProcessBuilder` 启动：

```text
<java17>/bin/java
  -Djava.io.tmpdir=<run>/phases/<phase>/tmp
  -Dpms.log.level=INFO
  -cp <pms-server-runtime-classpath>
  org.qwh.pms.server.PmsServerMain
  <run>/conf/pms-server-<phase>.properties
```

子进程环境显式设置：

```text
JAVA_HOME=<pms.it.pms.java.home>
HADOOP_CONF_DIR=<pms.it.hadoop.conf.dir>
```

Hadoop XML 目录也加入子 JVM classpath，确保直接启动 Java 时实际加载配置；仅设置
`HADOOP_CONF_DIR` 不足以完成加载。

配置文件固定 `pms.server.host=127.0.0.1`。Harness 选择空闲端口、写入 PID 文件，并把 stdout 和
stderr 合并追加到本 run 日志。ready 判定使用真实 HTTP/2 handshake，不以“进程仍存活”或固定
sleep 代替。

服务日志配置复用 `pms-server` runtime classpath 中的 `log4j2.xml`。正常停止调用
`Process.destroy()` 并在 timeout 内等待 shutdown hook；超时或等待中断后强制终止并等待回收，
同时保留原始失败，不能把强杀兜底当成成功。崩溃模拟调用
`destroyForcibly()`。操作后均等待进程确认退出并清理 PID 文件。Harness 只持有自己创建的 `Process`
对象，不按进程名或模糊 PID 扫描终止其他服务。

## 9. 管理 API

`PmsAdminClient` 是 testkit 内部的轻量 HTTP client，覆盖生产 `pms-client` 尚未提供的管理接口：

- `POST /flush`
- `POST /sink`
- `GET /state`

`flushAndAwait()` 和 `sinkAndAwait()` 读取响应中的 `fenceSequenceId`，轮询 `/state` 中相应持久化
边界。轮询使用 monotonic deadline、有限 backoff 和最终一次状态快照；超时异常包含 fence 和
最后状态，进程级 ready/退出错误同时包含日志路径和日志尾部。

业务 put/get/delete 始终通过 `PmsClient`，管理 client 不重复实现 binary hot path。

## 10. 错误模型与诊断

Testkit 抛出的异常按来源保留 cause，并在消息中提供 runId 和相关资源位置：

| 分类 | 示例 |
|------|------|
| 环境错误 | Java 版本错误、XML 缺失、URI/目录不安全 |
| 远端存储错误 | HDFS 权限、NameNode/DataNode 不可达、client/server 不兼容 |
| 表错误 | Paimon 建表失败、profile 不支持、snapshot 读取失败 |
| 进程错误 | 启动退出、ready 超时、异常退出、停止超时 |
| 协议错误 | handshake 不兼容、管理响应缺字段、状态无法收敛 |

失败信息不打印 Hadoop XML 全文。基础 run manifest 通过 Maven filtered resource 记录 project、
Paimon 和 Hadoop 版本，并记录执行时 Git commit/dirty（不可用时为 unknown）、test JVM、
Java/Hadoop 配置目录、HDFS URI、cleanup policy 和时间戳；具体场景再补充表名、seed、
数据规模等元数据。PMS properties、最后 state 和进程输出以独立文件保留。

关闭 run 时在 `<local-root>/reports/<runId>.json` 保留小型报告，包含 manifest、关键阶段 state、
完成时间、测试成功标记、清理结果和 teardown 错误。成功 run 的大文件和 HDFS 数据仍按策略删除；
失败现场保留。Git 信息描述执行时工作区，不替代可复现的已提交构建。

## 11. 依赖边界

允许依赖：

- `pms-client`、`pms-codec`、`pms-protocol`；
- Paimon API/Common/Core/Format 及项目当前 Hadoop runtime；
- 小型 JSON/日志依赖。

不依赖：

- `pms-core` 内部实现；
- `pms-server` 的 `dev.PMSTestServer` 或测试源码；
- JUnit/Failsafe；
- Flink/Kafka。

PMS 子进程需要 `pms-server` runtime classpath，但 testkit 通过外部 classpath 配置启动，不在 API
中调用 server implementation。

## 12. 模块自身验证

`pms-testkit` 的单元测试不访问远端 HDFS，覆盖：

- 环境参数解析和危险路径拒绝；
- runId/identifier 规范化；
- marker 完整匹配与所有权检查；
- cleanup policy；
- 数据生成可重复性；
- PMS properties 生成与本地状态目录隔离；
- admin state/fence 响应解析。

远端 Paimon 和真实 PMS 行为由 `pms-integration-tests` 验证。
