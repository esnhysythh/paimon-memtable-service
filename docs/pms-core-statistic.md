# PMS Core Statistic 模块设计文档

## 1. 模块定位

PMS Core 内部的可观测性基础设施，为核心组件提供低开销的指标采集与查询能力。不依赖任何外部监控系统，仅定义指标注册与采集接口；具体的暴露方式由 `pms-server` 层实现。

## 2. V1 范围

V1 仅定义 `MetricsRegistry` 接口，供各核心组件在构造时注入。具体指标类型（Counter / Gauge / Histogram / Timer）和指标体系待核心组件稳定后再设计。

> **TODO:** 详细设计待核心组件实现稳定后补充。待定内容包括：
> - 指标类型定义（Counter、Gauge、Histogram、Timer）
> - 各组件的指标埋点规划
> - 指标命名规范
> - Histogram 实现策略（简单分桶 vs HDRHistogram）
> - 与 pms-server MetricsExporter 的对接方式
