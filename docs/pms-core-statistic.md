# PMS 可观测性现状与后续方向

当前 MVP 使用 core 状态快照和 server `/state` 暴露运行状态，包括 sequence/flush/sink
边界、流控水位、后台任务与 lookup cache 的基础统计；具体字段见
[pms-server.md](pms-server.md) 和 [pms-lookup-paimon.md](pms-lookup-paimon.md)。

独立的 core `statistic` 模块、`MetricsRegistry` 接口和统一 MetricsExporter 尚未实现，
不属于当前 MVP 的必备能力。后续在实际运维需求明确后，再确定 Counter/Gauge/Histogram、
命名约定与导出方式，避免先引入一套未使用的指标抽象。
