# 后续测试：扩充远端表型正确性覆盖

## 目标与理由

首轮 HDFS 验收已通过，但 fixture 固定为单 bucket、单 BIGINT 主键、无分区。
远端环境恢复后可验证 V1 已支持的固定多 bucket 与分区复合主键，重点检查 key 到物理文件的定位、
DELETE 路由和重启后的历史查询。暂不新增生产功能或扩大支持类型范围。

本计划属于 MVP 之后的覆盖扩充，不阻塞当前限定范围的源码发布与试用。
当前发布先确认全新源码构建通过、支持范围明确；在目标 Linux 环境正式部署前再验证发行包启动与停机。

## 工作范围

| 场景 | 数据与操作 | 验收断言 |
| --- | --- | --- |
| 固定多 bucket | 4 个固定 bucket；确定性主键集合；跨两轮 sink 更新与删除 | 实际文件覆盖多个 bucket；full/local lookup 与独立 Paimon 扫描一致；空本地目录重启后历史查询正确 |
| 分区表与复合主键 | 例如 `dt STRING, id BIGINT, payload STRING, version INT`，按 dt 分区，主键 `(dt,id)`；至少两个分区使用相同 id | 更新或删除一个分区的 key 不影响另一分区；完整主键可正确路由 tombstone；sink 和重启后没有旧值复活 |

每个场景包含 insert、update、delete、未写入 key 查询、明确的 sink fence 和完整最终状态对照。
分区表的分区列包含在主键中，遵守现有 V1 约束，不测试跨分区更新或 Schema 演进。
同一远端表始终只有一个 PMS writer；空目录场景必须先停止原进程。

## 实现方式

- 在 `PaimonTestTableSpec` 增加必要的显式表型参数或工厂；保留现有简单表默认值。
- 单主键场景继续复用 `TestRecord`；分区复合主键增加小型专用数据模型和 verifier。
- 新增两个 `*IT` 场景，复用当前进程、owner marker、管理 fence 与报告逻辑。
- 不为两种表型引入通用 schema DSL、通用 workload 框架或新的调度抽象。
- 更新测试矩阵及验收记录；只在测试暴露确定的生产缺陷时实施必要修复。

## 完成标准

1. 新增两个场景单独通过，能证明实际访问了多个 bucket/partition。
2. 原有三个场景保持通过；扩充后的远端测试连续 3 轮通过，无跳过。
3. 本地 verify 通过，成功 run 安全清理，无遗留 PMS 子进程，失败保留现场。
4. 记录环境、Git 状态、数据种子和每个场景的结果；不把耗时作为性能 SLA。

本地 core、Paimon direct/cached 基准已实现。远端系统性能基线、发行包实际部署验收、
Kafka/Flink 远端 E2E、2PC failpoint 分别安排后续批次。本计划仅整理工作内容，尚未开始实现或执行新增场景。
