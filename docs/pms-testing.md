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
| ImmutableMemTable | 只读验证、引用计数增减 |
| LocalStorageManager | SST 写入 → 读取一致性、BloomFilter 构建/查询、`Optional<Value>` 三态语义、多路归并保留最新 Key |
| WALManager | 单盘写入 → 读取、CRC 校验正确性、Magic 检测 partial write |
| RowCodec / PrimaryKeyCodec | `InternalRow` 编码 → 解码往返正确性、主键编码顺序一致性、schema 不匹配拒绝 |
| BloomFilter | 假阳性率在预期范围内（如 < 1%）、不同 FPP 配置的效果 |

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
| Sink 流程状态机 | newSST → sinkedSST 状态转换正确，引用计数正确 |
| 流控水位线 | Immutable 数量达阈值时触发 OVERLOADED，拒绝生效 |
| 本地 SST 合并 | 多个小 SST 合并为大 SST，查询结果不变 |
| 双持状态退化 | 内存不足时 Mem 缓存正确退化，查询结果不变 |
| WAL 截断 | SinkMeta success 的 `persistedSequenceId` 覆盖旧 WAL 文件时正确删除 |

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
| 完整查询路径 | 各层命中 + Paimon 穿透，结果正确 |
| WAL 崩溃恢复 - 正常 | Kill → 重启 → 数据完整、不重复提交 |
| SinkMeta 崩溃恢复 - prepare 后 | Kill → 重启 → 使用 SinkMeta 中的 prepared payload、batch 和 fileRefs 重试 commit |
| SinkMeta 崩溃恢复 - success 后文件名未更新 | Kill → 重启 → 通过 SinkMeta success 推导 sinkedSST，并 best-effort 修正文件名标签 |
| 优雅停机 | 停机 → 所有 in-flight 操作完成 → 重启后数据完整 |

### 3.3 测试基础设施

```java
class PMSTestCluster implements AutoCloseable {
    // 一键搭建本地 PMS + Paimon 测试环境
    PMSClient client();
    PaimonTable paimonTable();
    void killPMS();  // 模拟进程崩溃
}
```
