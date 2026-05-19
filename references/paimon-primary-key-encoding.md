# Paimon 主键序列化与排序语义

## 结论

Paimon 序列化后的主键字节数组，通过**无符号字节比较（unsigned byte comparison）**即可得到与逻辑排序一致的结果。PMS 的 MemTable Key、SST 归并、Sink Parquet 生成都依赖此约束。

**关键：Java `Byte.compare()` 是有符号比较，不能直接用于 Paimon 主键字节的排序比较。必须使用无符号比较：`(a[i] & 0xFF) - (b[i] & 0xFF)`。**

## 序列化路径

```
Row → Projection(提取主键列) → Primary Key BinaryRow → BinaryRowSerializer → byte[]
```

## 各类型保序机制

| 类型 | 序列化方式 | 无符号字节比较是否保序 |
|------|-----------|---------------------|
| 整数 (int/long/short/byte) | 大端序 (big-endian) | 是 |
| 浮点数 (float/double) | IEEE 754 + NormalizedKey 位翻转 | 是（经 NormalizedKey 处理后） |
| 字符串 (BinaryString) | UTF-8 字节 + 长度前缀 | 是（先比较长度再比较 UTF-8 字节） |
| Decimal | 紧凑形式用 long 大端序，非紧凑用变长字节 | 是 |
| 复合主键 (BinaryRow) | 按列顺序写入：header → null bits → 固定长字段 → 变长字段 | 是（按列依次比较） |

## Paimon 源码关键位置

| 功能 | 类 |
|------|-----|
| 主键列提取 | `RowPartitionKeyExtractor` — 用 `CodeGenUtils.newProjection()` 生成投影，从完整行提取主键列为 `BinaryRow` |
| 行序列化 | `org.apache.paimon.data.serializer.BinaryRowSerializer` — 将 BinaryRow 序列化为 byte[] |
| 各类型序列化器 | `org.apache.paimon.data.serializer` 包下：`IntSerializer`, `LongSerializer`, `FloatSerializer`, `DoubleSerializer`, `BinaryStringSerializer`, `DecimalSerializer`, `TimestampSerializer` 等 |
| 归一化键（快速比较） | `NormalizedKeyComputer` — 代码生成，创建固定长度的 normalized key 用于字节级比较 |
| 完整记录比较 | `RecordComparator` — 代码生成，按字段依次比较，作为 normalized key 不够区分时的回退 |
| SST 文件写入排序 | `KeyValueDataFileWriter` — 写入排序后的 SST 文件，使用 Key 序列化 |
| Key 比较器供应 | `KeyComparatorSupplier` — 提供代码生成的主键比较器 |
| 内存排序 | `BinaryIndexedSortable` — 实现 serialized rows 的内存排序 |
