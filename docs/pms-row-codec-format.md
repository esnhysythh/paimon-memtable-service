# PMS Row Codec 格式设计

本文档从 `row-codec-demo/row_codec_design.md` 迁入，并按 PMS 当前模块边界更新。它描述 `pms-codec` 中 row value 的生产格式；demo 中曾经存在的 value 内部 `RowKind.DELETE` tombstone 语义已经删除。

## 1. 背景

PMS 需要把 Paimon `InternalRow` 存入内部 KV 系统，并在查询或 sink 回 Paimon 时重新构造行数据。

如果直接使用 Paimon `InternalRowSerializer`，KV value 会变成整段 `BinaryRow`。这适合整行复制或 spill，但不适合 PMS 后续的列投影读取：读取少量列时仍要面对整行 ordinal layout，也无法自然按稳定字段 ID 区分 NULL、missing 和 schema evolution。

PMS row value 因此采用 TiDB rowcodec 风格的 `metadata + payload` 结构：

- metadata 保存版本、flags、writer schema id、非 NULL 字段 ID、NULL 字段 ID 和 payload end offset。
- payload 只保存非 NULL 字段值。
- 解码指定字段时，先按 `DataField.id()` 二分查找，再只解码对应 payload。
- 顶层 row value 不保存 delete/tombstone 语义。

## 2. RowKind 与 Tombstone

PMS 是 latest-state KV，不保存完整 changelog。不同 `RowKind` 在进入 `pms-core` 前归一化为 KV 操作：

| 输入 RowKind | PMS 操作 |
|--------------|----------|
| `INSERT` | `put(key, rowValueBytes)` |
| `UPDATE_AFTER` | `put(key, rowValueBytes)` |
| `DELETE` | `delete(key)` |
| `UPDATE_BEFORE` | `delete(key)` |

delete/tombstone 由 KV 层表达：

```text
MemTable delete = Value.bytes == null
WAL delete      = valueLen = -1
SST delete      = valueLen = -1
Row value       = 只表示一条存在的行
```

因此 `RowValueCodec.encode(...)` 只接受 `INSERT/UPDATE_AFTER`。遇到 `DELETE/UPDATE_BEFORE` 必须抛出错误，调用方负责转为 `PMSBucketDirector.delete(key)`。

## 3. Byte Layout

PMS row value v1 格式如下：

```text
+-----------------------+-------------------------------+
| Field                 | Size                          |
+-----------------------+-------------------------------+
| VERSION               | 1 byte                        |
| FLAGS                 | 1 byte                        |
| WRITER_SCHEMA_ID      | 4 bytes, little-endian uint32 |
| NOT_NULL_FIELD_COUNT  | 2 bytes, little-endian        |
| NULL_FIELD_COUNT      | 2 bytes, little-endian        |
| NOT_NULL_FIELD_IDS    | N * idWidth bytes             |
| NULL_FIELD_IDS        | M * idWidth bytes             |
| NOT_NULL_END_OFFSETS  | N * offsetWidth bytes         |
| PAYLOAD               | variable                      |
| CHECKSUM              | optional, reserved for later  |
+-----------------------+-------------------------------+
```

线性表示：

```text
[VERSION]
[FLAGS]
[WRITER_SCHEMA_ID]
[NOT_NULL_FIELD_COUNT]
[NULL_FIELD_COUNT]
[NOT_NULL_FIELD_IDS...]
[NULL_FIELD_IDS...]
[NOT_NULL_END_OFFSETS...]
[PAYLOAD...]
[CHECKSUM optional]
```

固定头部长度为 10 bytes。

## 4. Header 字段

### 4.1 VERSION

`VERSION` 是 PMS row codec 版本。v1 固定为 `1`。

### 4.2 FLAGS

`FLAGS` 是 bitset。

```text
bit 0: LARGE_ROW
bit 1: HAS_CHECKSUM, reserved
bit 2..7: reserved
```

`LARGE_ROW` 决定 `fieldId` 和 `offset` 的宽度：

```text
small row:
  idWidth      = 1 byte
  offsetWidth  = 2 bytes

large row:
  idWidth      = 4 bytes
  offsetWidth  = 4 bytes
```

触发 `LARGE_ROW` 的条件：

```text
max(fieldId) > 255
or
payload.length > 65535
```

### 4.3 WRITER_SCHEMA_ID

`WRITER_SCHEMA_ID` 保存编码该 row value 时使用的 Paimon schema id。PMS 初版绑定单表且 schema 不更新时，可以固定写 0。

字段宽度为 4 bytes，按 unsigned 32-bit schema id 使用。如果未来需要超过该范围，或需要跨表全局 schema identity，应升级 row codec version。

### 4.4 NOT_NULL_FIELD_COUNT 与 NULL_FIELD_COUNT

`NOT_NULL_FIELD_COUNT` 记为 `N`，表示非 NULL 字段数量。

`NULL_FIELD_COUNT` 记为 `M`，表示显式 SQL NULL 字段数量。

二者均为 unsigned 16-bit little-endian。v1 不支持单行出现超过 65535 个字段。

## 5. Field ID 区域

Header 之后紧跟 field id 区域：

```text
[not-null field ids...][null field ids...]
```

规则：

- not-null field ids 必须按 unsigned numeric ascending 排序。
- null field ids 必须按 unsigned numeric ascending 排序。
- 两个数组之间不要求全局有序。
- 同一个 field id 不允许同时出现在两个数组中。
- field id 必须非负。

解码时根据 `N` 和 `M` 切分数组：

```text
notNullFieldIds = fieldIds[0:N]
nullFieldIds    = fieldIds[N:N+M]
```

## 6. Offset 与 Payload

Offset 区域只对应非 NULL 字段，因为 NULL 字段没有 payload。

第 `i` 个 offset 表示第 `i` 个非 NULL 字段 payload 的 end offset，offset 相对于 payload 起始位置计算。

```text
notNullFieldIds = [2, 5, 8]
payload lengths = [4, 0, 7]
endOffsets      = [4, 4, 11]
```

对应字段数据：

```text
field 2: payload[0:4]
field 5: payload[4:4]
field 8: payload[4:11]
```

Payload 只保存非 NULL 字段值，并按照 not-null field ids 的顺序拼接。Payload 不保存 field id，也不保存完整类型 tag；字段如何解码由 `RowType` 中对应 `DataField.type()` 决定。

## 7. NULL、空值、缺失字段

SQL NULL：

- field id 出现在 null field ids 中。
- field id 不出现在 not-null field ids 中。
- 不占用 offset 和 payload。

空字符串和空 byte array：

- field id 出现在 not-null field ids 中。
- 对应 payload length 为 0。
- offset 与前一个 offset 相等。

missing field：

- 既不在 not-null field ids，也不在 null field ids 中。
- 解码时按 default/null/error 规则处理。
- 如果字段可从 KV key 恢复，后续可由上层 key decoder 提供。

## 8. 编码流程

输入：

- `RowType writerType`
- `InternalRow row`
- `int writerSchemaId`

输出：

- `byte[] rowValue`

流程：

1. 校验 `row.getFieldCount() == writerType.getFieldCount()`。
2. 校验 row kind 只允许 `INSERT/UPDATE_AFTER`；`DELETE/UPDATE_BEFORE` 必须由调用方转为 KV delete。
3. 遍历 `writerType.getFields()`。
4. 对每个 `DataField`：
   - `fieldId = DataField.id()`
   - `fieldIndex = RowType ordinal`
   - 如果 `row.isNullAt(fieldIndex)`，加入 nullFields
   - 否则按 `DataField.type()` 编码字段值，加入 notNullFields
5. 按 field id 升序排序 notNullFields 和 nullFields。
6. 拼接 notNullFields 的 encoded value，生成 payload 和 endOffsets。
7. 根据 `max(fieldId)` 与 `payload.length` 设置 `LARGE_ROW`。
8. 写 header、field ids、offsets、payload。

默认策略：编码完整 row 的所有字段，包括主键字段。以后如果 PMS 决定主键只存在 KV key 中，可以通过 missing field 语义省略 value 中的主键字段，但这不作为 v1 默认行为。

## 9. 解码与投影

`RowValueView.parse(rowValue)` 只解析 header、field id 数组、offset 数组和 payload 边界，不反序列化字段值。

字段查找：

```text
findField(fieldId):
  idx = binarySearch(notNullFieldIds, fieldId)
  if found:
      return NOT_NULL(idx)

  idx = binarySearch(nullFieldIds, fieldId)
  if found:
      return NULL

  return MISSING
```

投影解码只遍历被请求的 field id，并只解码这些字段的 payload。

## 10. 列值编码

顶层 row container 不关心具体类型，只要求 `ColumnValueCodec` 满足：

```text
encode(DataType type, InternalRow row, int fieldIndex) -> bytes
decode(DataType type, bytes) -> Object
```

v1 支持：

| Paimon type | Payload encoding |
|---|---|
| BOOLEAN | 1 byte, 0 or 1 |
| TINYINT | 1 byte |
| SMALLINT | 2 bytes little-endian |
| INTEGER, DATE, TIME | 4 bytes little-endian |
| BIGINT | 8 bytes little-endian |
| FLOAT | IEEE 754 4 bytes little-endian |
| DOUBLE | IEEE 754 8 bytes little-endian |
| CHAR, VARCHAR | UTF-8 bytes from `BinaryString` |
| BINARY, VARBINARY | raw bytes |
| DECIMAL | unscaled integer bytes, scale and precision from schema |
| TIMESTAMP | millis and nanos according to precision |
| ARRAY | Paimon `InternalArraySerializer` sub-codec |
| MAP | Paimon `InternalMapSerializer` sub-codec |
| ROW | Paimon `InternalRowSerializer` sub-codec for this nested field only |

复杂类型复用 Paimon internal sub-codec 是 v1 的明确设计选择。PMS 当前只承诺顶层字段投影；只有未来需要 nested projection 时，才需要把复杂类型升级为递归 PMS codec 或新增 nested index。

## 11. 测试要求

必须覆盖：

- primitive、string、binary、decimal、timestamp round-trip。
- array、map、嵌套 row round-trip。
- 空字符串/空 byte array 与 SQL NULL 区分。
- not-null field ids 与 null field ids 排序。
- small row 与 large row 切换。
- 投影读取只解码请求字段。
- 字段重排、改名、增列、缺失字段、默认值和 NULL。
- `DELETE/UPDATE_BEFORE` 被拒绝编码。
- 非法版本、非法 offset、重复 field id、field id 同时出现在 not-null 和 null 数组中的错误处理。

## 12. 与 TiDB RowCodec 的关系

PMS row codec 参考 TiDB rowcodec 的骨架，但不追求字节级兼容。TiDB rowcodec 的固定 header 是 6 bytes：

```text
VER + FLAGS + NOT_NULL_COL_CNT + NULL_COL_CNT
```

PMS row codec 的固定 header 是 10 bytes：

```text
VERSION + FLAGS + WRITER_SCHEMA_ID + NOT_NULL_FIELD_COUNT + NULL_FIELD_COUNT
```

也就是说，PMS 保留 TiDB `metadata + payload` 的主体结构，但在 TiDB 计数 header 之前显式加入 `WRITER_SCHEMA_ID`。这是 PMS 为 Paimon schema evolution 增加的字段，用来记录写入该 row value 时的 Paimon schema id。后续当 PMS 需要同时读取旧 schema 写入的 value 和新 schema 的 `RowType` 时，decoder 可以先根据 `WRITER_SCHEMA_ID` 找到 writer schema，再做字段 ID、默认值、类型变更或兼容性处理。

保持一致或相近的部分：

| TiDB rowcodec | PMS row codec | 说明 |
|---|---|---|
| metadata + payload | metadata + payload | 先用 metadata 定位字段，再按需解码 payload。 |
| `VER` | `VERSION` | 版本字段位于 header 起始位置，用于格式升级。 |
| `FLAGS` | `FLAGS` | bitset 形式表达 large row、checksum 等格式扩展。 |
| 非 NULL column ids | not-null field ids | PMS 使用 Paimon `DataField.id()`。 |
| NULL column ids | null field ids | 显式区分 SQL NULL 和 missing field。 |
| offsets 指向 payload end offset | not-null end offsets | 支持 O(1) 切出字段 payload slice。 |
| small/large row | small/large row | field id 或 payload 超过 small 上限时切换宽度。 |
| flags 扩展位 | flags 扩展位 | 为 checksum、压缩、codec family 等后续扩展预留空间。 |

明确不同的部分：

| TiDB rowcodec | PMS row codec | 原因 |
|---|---|---|
| 面向 TiDB/TiKV SQL row value | 面向 PMS 内部 Paimon row value | PMS 不需要与 TiDB/TiKV 互通。 |
| column id 来自 TiDB schema | field id 来自 Paimon `DataField.id()` | 适配 Paimon schema evolution。 |
| header 不保存 schema version/id | header 保存 `WRITER_SCHEMA_ID` | PMS 需要知道 value 是按哪个 Paimon schema 写入的，便于未来处理 schema 变更后的读取、补默认值和类型兼容。 |
| 固定 header 为 6 bytes | 固定 header 为 10 bytes | PMS 在 TiDB 计数 header 基础上只新增 `WRITER_SCHEMA_ID`。 |
| 处理 handle/default/virtual generated column 等 TiDB 语义 | 不处理 TiDB 专属语义 | PMS 的 key/default/schema 规则来自 Paimon 和 PMS。 |
| 不保存 Paimon row kind | 不保存 Paimon row kind | PMS 是 latest-state KV，delete 由 `Value.bytes == null` / `valueLen = -1` 表达。 |
| 类型编码服务 TiDB Datum/MySQL 类型系统 | 类型编码服务 Paimon `DataType` | PMS 需要支持 Paimon internal Java values。 |
| TiDB checksum 语义 | checksum flag 预留，初版不启用 | 先保持格式简单，后续按需要扩展。 |

PMS 对 missing field 的处理也会依赖 `WRITER_SCHEMA_ID`。TiDB 在 column ID 缺失时会结合 handle、default value、virtual generated column 等 SQL 层语义恢复列值；PMS 则应结合 writer schema、reader schema、Paimon 字段 ID 和 PMS 自身规则判断字段是历史 schema 中不存在、新增 nullable 字段、需要默认值补齐，还是不兼容的 schema 变更。
