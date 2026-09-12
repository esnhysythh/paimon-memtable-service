# 首轮远端 HDFS 验收记录

执行日期：2026-09-10 至 2026-09-11（Asia/Shanghai）。

单独执行 preflight、smoke、recovery 全部通过，随后连续 3 轮完整远端测试均通过。
共 12 个成功场景，失败数和跳过数均为 0；此前另有一次已定位并修复的 preflight 清理失败，
失败现场按约定保留，不计入成功轮次。

## 构建与环境

| 项目 | 实际值 |
| --- | --- |
| 基础 Git commit | `815b4ee4332b53715a9c35361d086ac953966c51` |
| 工作区 | dirty，包含本批尚未提交的测试修补；各 run JSON 记录 commit/dirty |
| PMS / Paimon / Hadoop client | `0.1-SNAPSHOT` / `1.4.1` / `2.8.5` |
| Maven / JVM | Maven 3.8.6 / Temurin 17.0.18+8，macOS |
| 远端 HDFS | Hadoop 3.4.3，单 NameNode、单 DataNode，simple authentication，单副本 |
| NameNode | `hdfs://10.82.6.39:8020` |
| HDFS 专用根 | `hdfs://10.82.6.39:8020/tmp/pms-it-qinwenhao` |
| 本地专用根 | `/Users/qinwenhao/pms-integration-tests/runs` |
| Hadoop XML | `/Users/qinwenhao/Downloads/hadoop-3.4.3/etc/hadoop` |

测试使用 reactor 当前构建的完整依赖启动独立 PMS JVM；Hadoop XML 目录加入其 classpath。
业务操作与校验使用 Paimon/Hadoop Java client，不依赖 Hadoop 3.4.3 CLI 代为读写。

## 验收结果

耗时为 Failsafe 场景耗时，不是性能基线。

| 执行 | Preflight | Recovery | Smoke | 结果 |
| --- | ---: | ---: | ---: | --- |
| 单独执行（按 preflight → smoke → recovery） | 2.048 s | 14.047 s | 13.637 s | 全部通过 |
| 连续第 1 轮 | 2.169 s | 14.860 s | 11.326 s | 3/3，无跳过 |
| 连续第 2 轮 | 1.502 s | 14.857 s | 11.212 s | 3/3，无跳过 |
| 连续第 3 轮 | 2.010 s | 17.319 s | 11.044 s | 3/3，无跳过 |

最终未启用远端 profile 的 `mvn -o verify` 也通过：408 个测试，0 failure/error/skip，
包含 Flink MiniCluster 测试与发行包构建，耗时 29.788 s。

- Preflight：HDFS 创建、原文读写、rename、delete，Paimon 建库建表，owner marker 安全清理。
- Smoke：1,000 条初始写入、200 条更新、91 条删除；sink fence 为 1291；独立扫描与最终状态相等。
  新本地目录下 writer identity 改变，再执行更新、插入、删除，sequence/persisted 均为 3，
  独立扫描确认三条操作生效。
- Recovery：首批 500 条写入强杀前 flushed/persisted 均为 0，重启重放 500 条；
  初始状态 sink 后再执行 146 条更新/删除，强杀前 flushed/persisted 保持 500，重启重放 146 条；
  最终 sink 边界为 646、snapshot 为 2，第三次重启重放 0 条且不增加 snapshot。
  每阶段同时检查完整查询、本地 tombstone、独立 Paimon 最终数据与固定的启动恢复摘要。

恢复后的后台 Flush 可以先于查询完成，因此重放数量与启动时边界取自稳定的 `runtime.recovery`，
不依赖读取 `/state` 时 MemTable 是否已被刷盘。

## 本批修补与首次失败

1. 补齐强杀前 WAL 边界、完整查询覆盖历史旧值、恢复数量与 writer identity 断言。
2. 补齐空目录接续写入已有表的远端回归。
3. 子进程正常停止超时或等待中断后强制回收，仍保留失败；新增真实挂起 shutdown hook 的回归测试。
4. 通过 Maven filtered resource 记录实际项目和依赖版本，记录 Git commit/dirty、关键 state 与清理结果；
   成功清理后仍保留小型 JSON 报告。
5. 修复 owner marker 读取：Paimon `readFileUtf8()` 拼接各行，破坏两行 marker；改为读取原始 UTF-8
   字节，并添加保留换行的回归测试。最初的读写与建表成功，但此问题使清理失败。
6. 明确给子 JVM classpath 加载 Hadoop XML，避免仅设置 `HADOOP_CONF_DIR` 而配置未生效。

首个离线尝试因本机未缓存 Failsafe 3.5.2 而未进入远端测试；下载插件后，后续验收均可离线构建。

失败 run 为 `20260910110836-62435485`。最终只读检查确认：12 个成功 run 的本地和远端数据均已清理；
本地/HDFS 专用根仅剩上述失败现场和本地报告目录，没有残留 `PmsServerMain` 进程。

## 复现与证据

```bash
export JAVA_HOME=/Users/qinwenhao/Library/Java/JavaVirtualMachines/temurin-17.0.18/Contents/Home
mvn -o -Premote-hdfs-it -pl pms-tests/pms-integration-tests -am verify \
  -Dpms.it.pms.java.home="$JAVA_HOME" \
  -Dpms.it.hadoop.conf.dir=/Users/qinwenhao/Downloads/hadoop-3.4.3/etc/hadoop \
  -Dpms.it.local.root=/Users/qinwenhao/pms-integration-tests/runs \
  -Dpms.it.hdfs.root=hdfs://10.82.6.39:8020/tmp/pms-it-qinwenhao \
  -Dpms.it.cleanup=on-success
```

单独运行时追加 `-Dit.test=RemoteHdfsPreflightIT`、`RemoteHdfsSmokeIT` 或 `RemoteHdfsRecoveryIT`。
新机器若未缓存 Maven 依赖，首次去掉 `-o`。

- Run JSON：`/Users/qinwenhao/pms-integration-tests/runs/reports/<runId>.json`。
- 本次 Maven 日志、Failsafe XML、清理检查及摘要归档：
  `/Users/qinwenhao/pms-integration-tests/runs/reports/acceptance-2026-09-11/`。
- 失败现场：本地/HDFS 专用根下的 `20260910110836-62435485` 子目录。

本轮覆盖单 bucket、简单主键表和上述单节点 HDFS 环境。多 bucket、复杂类型、进程级 2PC failpoint、
Kafka/Flink 远端 E2E、HA/Kerberos 和性能基线不在本批验收范围。
