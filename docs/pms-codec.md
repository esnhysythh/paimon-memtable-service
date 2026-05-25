# PMS Codec 设计文档

## 1. 模块定位

`pms-codec` 是 Paimon 行模型与 PMS byte-oriented KV 核心之间的适配层。

PMS 仍然是绑定 Paimon 表的专用服务，但 `pms-core` 只负责本地生命周期：MemTable、WAL、SST、sequence、flush、sink 边界和恢复。`pms-codec` 负责把 Paimon `InternalRow` 转换成 `pms-core` 可以承载的 `byte[] key` 与 `byte[] value`。

## 2. 依赖边界

```text
pms-codec -> Paimon API / common / core
pms-core  -> byte[] key/value 生命周期核心，不依赖 pms-codec
```

`pms-codec` 不反向依赖 `pms-core`。接口返回 `byte[]`，不返回 `Key` 或 `Value`，由上层组合模块调用 `PMSBucketDirector.put(byte[] key, byte[] value)` 或 `PMSBucketDirector.delete(byte[] key)`。

`pms-core` 拥有 sink 状态机和 `SinkManager` SPI：它负责选择 `SinkBatch`、写入 `SINK_PREPARE/SINK_SUCCESS` WAL 记录、推进 SST sinked 状态和 `persistedSequenceId` 边界；真实 Paimon 写入逻辑由上层模块注入 `SinkManager` 实现。这样 `pms-core` 可以追踪“是否 sink 成功”，但不依赖 Paimon sink 的具体实现。

## 3. 核心语义

PMS KV 长期语义：

```text
Key          = Paimon primary key 的稳定有序编码
Value.bytes  = serialized Paimon row value
Value null   = delete tombstone
```

delete/tombstone 是 KV 层语义，不写入 row value 内部：

```text
MemTable delete = Value.bytes == null
WAL delete      = valueLen = -1
SST delete      = valueLen = -1
Row value       = 只表示一条存在的行
```

因此 row value codec 不把 `RowKind.DELETE` 编码为特殊 value bytes。

## 4. RowKind 归一化

进入 `pms-core` 前，调用方必须把 Paimon `RowKind` 归一化为 PMS latest-state KV 操作：

| 输入 RowKind | PMS 操作 |
|--------------|----------|
| `INSERT` | `put(key, rowValueBytes)` |
| `UPDATE_AFTER` | `put(key, rowValueBytes)` |
| `DELETE` | `delete(key)` |
| `UPDATE_BEFORE` | `delete(key)` |

第一版 row value format 中所有被编码的 value 都表示存在的行。若未来需要保留完整 changelog，而不是 latest-state KV，应新增格式版本或 flag，不能复用当前 value 语义。

## 5. Row Value Codec

row value format 采用 TiDB rowcodec 风格的 `metadata + payload` 结构，但不追求与 TiDB 字节兼容。

目标：

- 使用 Paimon `DataField.id()` 作为持久字段标识。
- 支持字段重排、改名、新增 nullable/default 字段时的可解释行为。
- 区分非 NULL 字段、SQL NULL 字段和 missing 字段。
- 支持只解析元数据后按需解码投影列。
- 顶层 row 不使用 Paimon `InternalRowSerializer`，避免整行 ordinal layout 阻碍字段 ID 查找。
- 复杂类型字段内部可以复用 Paimon internal serializer，初版只承诺顶层投影。

建议接口：

```java
interface RowValueCodec {
    byte[] encode(RowType writerType, InternalRow row, int writerSchemaId);

    InternalRow decode(RowType readType, byte[] rowValue);

    InternalRow decodeProjected(RowType readType, byte[] rowValue, int[] projectedFieldIds);

    RowValueView parse(byte[] rowValue);
}
```

`encode` 只接受 `INSERT/UPDATE_AFTER` 归一化后的存在行；`DELETE/UPDATE_BEFORE` 应由调用方转为 `delete(key)`。

## 6. Row Value Header

第一版 header 保留 12 bytes，避免后续频繁改变固定头部大小：

```text
VERSION               1 byte
FLAGS                 1 byte
RESERVED_0            1 byte
RESERVED_1            1 byte
WRITER_SCHEMA_ID      4 bytes, little-endian uint32
NOT_NULL_FIELD_COUNT  2 bytes, little-endian
NULL_FIELD_COUNT      2 bytes, little-endian
```

`FLAGS` 初始定义：

```text
bit 0: LARGE_ROW
bit 1: HAS_CHECKSUM, reserved
bit 2..7: reserved
```

row value 不保存 `RowKind.DELETE`。如果未来需要在 value 内保存 row kind，应通过新 version 或新 flag 显式升级。

## 7. PrimaryKeyCodec

`PrimaryKeyCodec` 负责把 Paimon 主键字段编码成 PMS key bytes。

要求：

- 编码结果必须与 Paimon 主键排序语义一致。
- `pms-core` 的 `Key.compareTo()` 使用无符号字节序，因此 key bytes 必须能在无符号 lexicographical compare 下得到正确顺序。
- 编码结果应稳定，不依赖字段 ordinal 的偶然布局。
- 第一版需要覆盖主键类型支持矩阵，并用排序一致性测试固定行为。

建议接口：

```java
interface PrimaryKeyCodec {
    byte[] encodeKey(InternalRow row);
}
```

## 8. 接入路径

写入路径：

```text
InternalRow
  -> PrimaryKeyCodec.encodeKey(row)
  -> RowKind 归一化
  -> RowValueCodec.encode(...) 或 delete(key)
  -> PMSBucketDirector.put/delete
```

查询路径：

```text
primary key bytes
  -> PMSBucketDirector.get(key)
  -> Optional<byte[]> rowValue
  -> RowValueCodec.decode/decodeProjected
```

Sink 路径：

```text
SST ordered iterator<Entry<byte[] key, byte[] value>>
  -> value == null 表示 delete
  -> value != null 使用 RowValueCodec.decode(...) 得到 InternalRow
  -> Paimon 2PC writer/committer
```

## 9. 后续落地顺序

1. 先完成 Maven 父工程与 `pms-core` 子模块平移。
2. 新增 `pms-codec` 子模块，从 row-codec-demo 迁入 row value codec。
3. 去除 demo 中 value 内部 `RowKind.DELETE` tombstone 语义。
4. 补齐 `PrimaryKeyCodec` 和排序一致性测试。
5. 实现 SST ordered iterator，再接入真实 Paimon sink。
