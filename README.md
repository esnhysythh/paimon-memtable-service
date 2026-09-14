[简体中文](README.md) | [English](README.en.md)

# Paimon MemTable Service

**为 Paimon 表提供实时写入与当前态点查的单机 KV 服务。**

PMS 像 Paimon 表前的一层 MemTable：作为表的唯一写入入口，承接最新修改并异步下沉到
Paimon；查询先检查本地最新状态，再访问 Paimon 历史数据。这个名字描述的是 PMS 在
整体存储架构中的角色。

PMS 内部采用包含 **WAL、MemTable、本地 SST 和 compaction 的轻量级 LSM KV 引擎**，
提供本地存储与进程崩溃恢复能力。本地写入层保留最新状态窗口，独立的查询缓存则按需
缓存 Paimon 历史文件；两者共同支持当前态主键点查。Paimon 表仍可供计算引擎执行分析查询。

当前版本为 `0.1-SNAPSHOT`，已经具备限定范围内的 MVP 功能闭环，可从源码构建和试用。

## MVP 支持范围

| 方面 | 当前范围 |
| --- | --- |
| 部署 | 单机、绑定一张预先创建的 Paimon 表；PMS 是该表唯一写入者 |
| 表 | Primary key + `deduplicate` + `HASH_FIXED` bucket + Parquet；分区列必须包含在主键中 |
| Schema | 在 PMS 本地状态生命周期内保持不变，由部署方保证 |
| 主键 | 非 null 的 `INT`、`BIGINT`、`DATE`、`STRING`、非 LTZ 的 `TIMESTAMP(P <= 6)` 及支持的组合；详见 [lookup profile](docs/pms-lookup-paimon.md#7-首期-profile) |
| 写入与恢复 | Put/delete/batch、WAL、MemTable、本地 SST、异步 sink、SinkMeta 恢复与流控 |
| 查询 | 当前态主键点查、本地前缀查询、Paimon direct lookup 与文件级本地缓存；tombstone 阻断历史数据穿透 |
| 接入 | HTTP/2 h2c、Java client；Flink 1.20.3 At-Least-Once Sink、同步/异步 processing-time Lookup Join、完整主键等值 SQL DELETE |
| 交付 | Maven 多模块源码、Flink connector JAR、Linux 发行包与启动/停机脚本 |

普通写入成功表示 WAL append 完成且数据在 MemTable 可见，**不逐次 fsync，也不表示已经
提交到 Paimon**。当前验证覆盖进程崩溃恢复，不承诺每次已确认写入都能抵御机器掉电。
需要确认 Paimon 已可见时，通过管理接口建立 sink fence，并等待对应持久化边界推进。

V1 不提供分布式/多节点服务、外部并发 writer、Schema 演进、MVCC/历史快照读、全表扫描
Source 或 Exactly-Once。独立显式 Paimon compaction 调度延后；当前保留普通 sink 的隐式
compaction 与 Paimon 按表配置执行的 snapshot 清理。行值类型边界见 [codec 文档](docs/pms-codec.md)。

## 构建与运行

构建基线为 Java 17、Maven 3.8.6，固定依赖 Paimon 1.4.1 和 Flink 1.20.3。
Maven `groupId` 为 `io.github.esnhysythh`，模块 artifactId 见各 POM。

```bash
# 常规单元、本地集成测试及打包验证；不访问远端 HDFS
mvn verify

# 构建 Linux 发行包
mvn -pl pms-dist -am package
```

发行文件位于 `pms-dist/target/pms-0.1-SNAPSHOT/` 和同名 `.tar.gz`。
运行前编辑 `conf/pms-server.properties`，配置已有表和持久化 WAL/storage 路径，然后运行
`bin/pms-server.sh`。配置与后台进程管理见 [发行包文档](docs/pms-dist.md)。

本地体验可启动自动创建测试表的开发入口，并使用 [server README](pms-server/README.md)
中的 HTTP 示例：

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.dev.PMSTestServerMain
```

## 验证范围

- 常规 Maven 测试覆盖本地文件系统下的组件、HTTP/2 读写/恢复和 Flink MiniCluster 闭环。
- 首轮真实 HDFS 验收已覆盖独立 PMS 进程、读写删除、全新本地状态接续与 kill/restart，
  并连续 3 轮通过。环境为 HDFS 3.4.3 单 NameNode/单 DataNode、simple 认证、单副本，
  PMS 使用 Hadoop 2.8.5 client。详情见 [验收记录](docs/tests/pms-remote-hdfs-acceptance-2026-09-11.md)。
- 远端验收使用单 bucket、BIGINT 主键、无分区表。多 bucket/分区复合主键已有本地覆盖，
  远端扩充见 [后续测试计划](docs/tests/pms-next-batch.md)。
- S3、HDFS HA/Kerberos、长时间压力测试与目标 Linux 发行包部署尚未完成验收，
  不在当前已验证环境声明内；已有结果不代表生产环境认证或性能 SLA。

本地性能工具包含 **core 插入/查询、Paimon direct 查询、Paimon cached 查询** 三类场景。
运行方法与测量口径见 [benchmark 文档](docs/pms-benchmark.md)。源码仓库保留可复用工具和
说明，运行报告留在本地或 CI 产物中。

## 文档入口

- [架构与模块索引](design.md)
- [服务配置、调度与恢复](docs/pms-server.md)
- [Java client](docs/pms-client.md) / [Flink connector](docs/flink-connector-pms.md)
- [测试策略](docs/tests/pms-testing-strategy.md)
