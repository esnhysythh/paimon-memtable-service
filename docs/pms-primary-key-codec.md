# PMS Primary Key Codec 格式设计

本文档描述 `pms-codec` 中 PMS primary key 的生产格式。Primary key codec 负责把 Paimon `InternalRow` 中的主键列编码为 PMS `byte[] key`，供 `pms-core` 的 MemTable、WAL、SST 和 range/prefix scan 使用。

PMS primary key format 参考 TiDB clustered table `CommonHandle` 的 mem-comparable tuple encoding 思路，但不追求与 TiDB 字节级兼容。PMS 的类型系统、schema identity、collation 和时间语义以 Paimon 为准。

## 1. 目标

PMS key bytes 必须满足：

```text
compare_paimon_primary_key_tuple(a, b)
==
unsigned_lexicographical_compare(encode(a), encode(b))
```

其中 `unsigned_lexicographical_compare` 与 `pms-core` 中 `Key.compareTo()` 一致：逐字节按 `byte & 0xFF` 比较，前缀相同时短 key 更小。

格式目标：

- 支持联合主键按字段顺序排序。
- 支持主键前缀局部性，例如 `(id1, id2)` 中相同 `id1` 的 key 连续排列。
- 支持通过 `encodePrefix(...)` 与 `PrefixNext` 构造 `[start, end)` 范围。
- 每个字段编码保序，并且字段编码可切分。
- 拒绝 NULL primary key，避免 NULL 排序和唯一性语义进入 PMS v1。
- 不复用 Paimon serializer，也不要求与 Paimon/TiDB 字节级兼容。

## 2. 与 TiDB 的关系

TiDB clustered primary key 的核心结构可以抽象为：

```text
encoded_handle =
  encoded_datum(pk_col_1)
  || encoded_datum(pk_col_2)
  || ...
  || encoded_datum(pk_col_n)
```

每个 `encoded_datum` 由 type flag 和 mem-comparable payload 组成。PMS 采用相同的高层思想：

```text
pms_key =
  encoded_field(pk_col_1)
  || encoded_field(pk_col_2)
  || ...
  || encoded_field(pk_col_n)
```

相同点：

- 多列主键按字段顺序直接拼接。
- 整数使用翻转符号位后的 big-endian 表示。
- string/binary 使用 8 字节分组加 marker 的 mem-comparable bytes encoding。
- 前缀扫描使用 `PrefixNext` 构造右开边界。
- NULL primary key 不进入第一版格式。

差异点：

- PMS key 不写全局 version byte，以贴近 TiDB `CommonHandle` 并降低 MemTable/SST 热路径 key 长度。格式升级若发生，应通过文件级/服务级元数据或整体迁移处理，而不是在每条 key 内保存版本。
- PMS field type 由 Paimon `RowType + primaryKeyFieldIds` 决定，不编码 Paimon field id、field name 或完整 schema。
- PMS 不支持 TiDB/MySQL collation。string 使用 Paimon `BinaryString` 的原始 UTF-8 bytes binary order。
- PMS 不支持 TiDB Decimal、Duration、JSON 等 Datum 类型。
- PMS timestamp 语义按 Paimon `Timestamp` 内部值处理，不写入时区信息。

## 3. Key Layout

```text
+-----------------------+---------------------------------+
| Field                 | Size                            |
+-----------------------+---------------------------------+
| ENCODED_PK_FIELD_0    | variable, self-delimiting       |
| ENCODED_PK_FIELD_1    | variable, self-delimiting       |
| ...                   | ...                             |
+-----------------------+---------------------------------+
```

key bytes 不包含全局 header 或 version，完整 key 由主键字段编码直接拼接而成。该选择与 TiDB `CommonHandle` 保持一致，并避免为每条 key 增加固定开销。

主键字段顺序来自 Paimon primary key 定义顺序。字段 identity 建议在 codec 构造时解析为 `DataField.id()`，这样表字段 reorder/rename 不改变 key 编码；运行时再根据当前 `RowType` 找到对应 ordinal。

key bytes 不保存：

- 表 id
- bucket id
- schema id
- field id
- field name
- RowKind

这些信息由 PMS 上层绑定的单表上下文、bucket 路由和 codec 构造参数提供。

## 4. Field Layout

每个字段编码为：

```text
TYPE_FLAG || TYPE_SPECIFIC_ORDERED_BYTES
```

`TYPE_FLAG` 的主要作用是让字段编码可切分，并让 dump/debug 更直接。由于同一个字段位置的 type flag 在同一个 schema 下固定，flag 不影响该字段内的排序结果。

v1 type flag 定义：

| Type flag | 含义 | Payload length |
|---|---|---|
| `0x01` | BYTES, 用于 `CHAR/VARCHAR/BINARY/VARBINARY` | variable, mem-comparable bytes |
| `0x03` | INT8, 用于 `TINYINT` | 1 |
| `0x04` | INT16, 用于 `SMALLINT` | 2 |
| `0x05` | INT32, 用于 `INTEGER` | 4 |
| `0x06` | INT64, 用于 `BIGINT` | 8 |
| `0x07` | TIMESTAMP_MILLIS | 8 |
| `0x08` | TIMESTAMP_NANOS | 12 |
| `0x09` | DATE_DAYS | 4 |
| `0x0a` | TIME_MILLIS | 4 |

`0x00` 保留给未来 NULL 或最小哨兵语义；v1 不写出该 flag。

## 5. Supported Types

v1 支持：

| Paimon type | PMS key encoding |
|---|---|
| `TINYINT` | `INT8` |
| `SMALLINT` | `INT16` |
| `INTEGER` | `INT32` |
| `BIGINT` | `INT64` |
| `DATE` | `DATE_DAYS` |
| `TIME_WITHOUT_TIME_ZONE` | `TIME_MILLIS` |
| `CHAR`, `VARCHAR` | `BYTES` over raw UTF-8 bytes |
| `BINARY`, `VARBINARY` | `BYTES` over raw binary bytes |
| `TIMESTAMP_WITHOUT_TIME_ZONE` | `TIMESTAMP_MILLIS` or `TIMESTAMP_NANOS` by precision |
| `TIMESTAMP_WITH_LOCAL_TIME_ZONE` | `TIMESTAMP_MILLIS` or `TIMESTAMP_NANOS` by precision |

v1 不支持：

- `BOOLEAN`
- `FLOAT`, `DOUBLE`
- `DECIMAL`
- `ARRAY`, `MAP`, `ROW`
- `MULTISET`, `VECTOR`, `VARIANT`, `BLOB`

这些类型不适合作为 PMS v1 primary key 的默认支持面，或者需要额外语义讨论。

## 6. NULL Policy

v1 直接拒绝 NULL primary key：

```text
if row.isNullAt(primaryKeyOrdinal):
    throw IllegalArgumentException
```

原因：

- Paimon primary key 在业务语义上应为 non-null identity。
- NULL 排序、NULL 等价性和唯一性语义会污染 PMS latest-state KV 的简单模型。
- PMS delete/tombstone 已由 KV 层表达，不需要在 key 内表达 NULL。

## 7. Integer Encoding

有符号整数使用“翻转符号位 + big-endian”的 ordered encoding。

```text
TINYINT:
  payload = uint8(value ^ 0x80)

SMALLINT:
  payload = big_endian_uint16(value ^ 0x8000)

INTEGER:
  payload = big_endian_uint32(value ^ 0x80000000)

BIGINT:
  payload = big_endian_uint64(value ^ 0x8000000000000000)
```

示例：

```text
INT32 -1 -> 7f ff ff ff
INT32  0 -> 80 00 00 00
INT32  1 -> 80 00 00 01
```

这样负数区间映射到 unsigned byte space 的低半区，非负数区间映射到高半区，big-endian 字节序与数值排序一致。

## 8. String and Binary Encoding

`CHAR/VARCHAR` 使用 `BinaryString.toBytes()` 得到的原始 UTF-8 bytes。PMS v1 不支持 SQL collation、大小写不敏感排序或语言相关排序。

`BINARY/VARBINARY` 使用原始 byte array。

两者都使用 TiDB 风格的 mem-comparable bytes encoding：

```text
encGroupSize = 8
encMarker    = 0xff
encPad       = 0x00

raw bytes are split into 8-byte groups.
Each group is followed by a 1-byte marker.
The last group is padded with 0x00.
marker = 0xff - padding_zero_count.
```

编码结构：

```text
[group_0: 8 bytes][marker_0]
[group_1: 8 bytes][marker_1]
...
[last_group: 8 bytes][last_marker < 0xff]
```

如果原始长度刚好是 8 的倍数，必须额外追加一个全 padding 的终止组：

```text
raw: 01 02 03 04 05 06 07 08

encoded:
01 02 03 04 05 06 07 08 ff
00 00 00 00 00 00 00 00 f7
```

示例：

```text
raw: []
encoded payload:
00 00 00 00 00 00 00 00 f7

raw: 61 62 63
encoded payload:
61 62 63 00 00 00 00 00 fa

raw: 01 02 03 00
encoded payload:
01 02 03 00 00 00 00 00 fb
```

该编码满足：

- 原始 bytes 的 unsigned lexical order 等于 encoded payload 的 unsigned lexical order。
- 字段边界可通过第一个 marker `< 0xff` 的 group 识别。
- 原始 bytes 中的 `0x00` 与 padding 可通过 marker 区分。

## 9. Date and Time Encoding

Paimon `DATE` 在 `InternalRow` 中是 `int`，表示从 epoch 起算的 day count。该值可能为负数，因此按 signed int32 ordered encoding：

```text
TYPE_FLAG = DATE_DAYS
payload   = ordered_int32(dateDays)
```

Paimon `TIME_WITHOUT_TIME_ZONE` 在 `InternalRow` 中是 `int`，表示一天内的 millisecond。该值语义范围是 `0..86_399_999`，因此按 unsigned int32 big-endian：

```text
TYPE_FLAG = TIME_MILLIS
payload   = big_endian_uint32(millisOfDay)
```

编码器应校验 `0 <= millisOfDay <= 86_399_999`，避免异常输入破坏 unsigned time encoding 的排序语义。

## 10. Timestamp Encoding

Paimon `TIMESTAMP_WITHOUT_TIME_ZONE` 和 `TIMESTAMP_WITH_LOCAL_TIME_ZONE` 在 `InternalRow` 中都通过 `Timestamp` 暴露。`Timestamp.compareTo()` 先比较 `millisecond`，再比较 `nanoOfMillisecond`。

PMS key 不保存时区信息：

- `TIMESTAMP_WITHOUT_TIME_ZONE` 没有时区语义，按 Paimon 内部 `Timestamp` 值编码。
- `TIMESTAMP_WITH_LOCAL_TIME_ZONE` 在 Paimon 类型语义中表示 UTC timestamp 按 session time zone 展示；进入 PMS key codec 时直接使用内部 epoch millis/nanos，不再引入 session timezone。

编码根据 Paimon timestamp precision 选择：

```text
precision <= 3:
  TYPE_FLAG = TIMESTAMP_MILLIS
  payload   = ordered_int64(timestamp.millisecond)

precision > 3:
  TYPE_FLAG = TIMESTAMP_NANOS
  payload   = ordered_int64(timestamp.millisecond)
              || big_endian_uint32(timestamp.nanoOfMillisecond)
```

其中：

```text
ordered_int64(v) = big_endian_uint64(v ^ 0x8000000000000000)
```

`nanoOfMillisecond` 的取值范围是 `0..999999`，非负 big-endian uint32 与其自然顺序一致。

为了避免静默丢失精度，编码器应在 `precision <= 3` 时校验：

```text
timestamp.nanoOfMillisecond == 0
```

如果上游传入未按 Paimon 声明精度规范化的值，应抛出 `IllegalArgumentException`。

## 11. Prefix Encoding and Range

对联合主键：

```text
PRIMARY KEY (id1, id2, id3)
```

完整 key：

```text
encode(id1) || encode(id2) || encode(id3)
```

前缀 key：

```text
encode(id1)
encode(id1) || encode(id2)
```

由于每个字段编码保序并可切分，相同前缀的完整 key 在 PMS MemTable/SST 中连续排列。

扫描某个 encoded prefix 下的全部 key：

```text
start = encodePrefix(...)
end   = PrefixNext(start)
scan [start, end)
```

`PrefixNext` 规则：

```text
从后向前找第一个不是 0xff 的字节；
将该字节 +1；
截断其后的所有字节；
如果所有字节都是 0xff，则没有有限右边界。
```

示例：

```text
01 02 03 -> 01 02 04
01 02 ff -> 01 03
ff ff    -> no finite upper bound
```

## 12. Schema and Field Identity

推荐 codec 构造参数：

```java
PmsPrimaryKeyCodec(RowType rowType, int[] primaryKeyFieldIds)
```

便利构造可支持：

```java
PmsPrimaryKeyCodec.forFieldNames(RowType rowType, List<String> primaryKeyFieldNames)
```

但内部应尽快解析为 field id。原因：

- Paimon `DataField.id()` 是 schema evolution 下更稳定的字段身份。
- row value codec 已经按 `DataField.id()` 保存顶层字段。
- 字段 rename/reorder 后，只要 primary key field id 和类型不变，PMS key bytes 应保持不变。

限制：

- primary key 字段顺序由 primary key 定义决定，不能按 field id 排序。
- v1 不支持 primary key 字段类型变更。
- v1 不在 key bytes 内保存 writer schema id。PMS v1 绑定单表且 schema 不变；后续 schema evolution 需要单独设计迁移策略。

## 13. Suggested API

建议第一版实现：

```java
public final class PmsPrimaryKeyCodec {
    public PmsPrimaryKeyCodec(RowType rowType, int[] primaryKeyFieldIds);

    public static PmsPrimaryKeyCodec forFieldNames(
            RowType rowType, List<String> primaryKeyFieldNames);

    public byte[] encodeKey(InternalRow fullRow);

    public byte[] encodePrefix(InternalRow fullRow, int primaryKeyFieldCount);

    public byte[] encodeKeyTuple(InternalRow keyTuple);

    public byte[] encodePrefixTuple(InternalRow keyPrefixTuple);

    public InternalRow decodeKey(byte[] key);

    public InternalRow decodePrefix(byte[] prefix, int primaryKeyFieldCount);

    public static Optional<byte[]> prefixNext(byte[] encodedPrefix);
}
```

语义：

- `encodeKey(fullRow)` 从完整 Paimon row 中按 primary key field ids 提取字段。
- `encodePrefix(fullRow, count)` 编码前 `count` 个 primary key 字段。
- `encodeKeyTuple(keyTuple)` 输入只包含主键字段，ordinal 与 primary key 顺序一致。
- `encodePrefixTuple(keyPrefixTuple)` 输入只包含主键前缀字段。
- `decodeKey(key)` 将完整 key 解码为 key tuple row，输出字段顺序按 primary key 定义顺序。
- `decodePrefix(prefix, count)` 将 encoded prefix 解码为只包含前 `count` 个 primary key 字段的 tuple row。
- `prefixNext(...)` 返回右开边界；若无有限右边界，返回 `Optional.empty()`。

decode 不返回完整表 row，因为 key bytes 不保存非主键字段，也不保存 field id、field name 或 schema id。调用方如需重建完整 row，应先用 row value 解码，再在需要时用 key tuple 补齐 missing primary key 字段。

decode 必须严格校验：

- 每个字段的 `TYPE_FLAG` 必须与构造 codec 时的 primary key type 匹配。
- fixed-width payload 必须有足够字节。
- string/binary 的 mem-comparable bytes 必须存在合法终止 group。
- bytes marker 必须在合法范围内，终止 group 的 padding 必须全为 `0x00`。
- timestamp nanos 必须在 `0..999999`。
- time millis 必须在 `0..86_399_999`。
- 解码指定字段数后不能存在 trailing bytes。

## 14. Test Requirements

实现时至少用测试固定以下行为：

- golden bytes for mixed fixed/variable key。
- `TINYINT/SMALLINT/INTEGER/BIGINT` 的负数、0、正数、边界值排序一致性。
- `DATE/TIME_WITHOUT_TIME_ZONE` 的边界和排序一致性。
- `STRING/BINARY` 的空值、前缀值、内含 `0x00`、内含 `0xff`、长度为 8 的倍数等场景。
- `TIMESTAMP(3)` 使用 8-byte millis；`TIMESTAMP(6)` 使用 millis+nanos；排序与 `Timestamp.compareTo()` 一致。
- `TIMESTAMP(3)` 遇到非 0 `nanoOfMillisecond` 显式拒绝。
- 联合主键排序与 tuple comparator 一致。
- 相同主键前缀的 key 连续，并且 `[prefix, PrefixNext(prefix))` 完整覆盖该前缀。
- 完整 key 与 prefix key decode round-trip。
- 损坏 key、错误 type flag、截断 payload、非法 bytes marker、非法 padding、trailing bytes 显式拒绝。
- 字段 reorder/rename 后，基于相同 field id 的 key bytes 不变。
- NULL primary key 和 unsupported type 显式拒绝。
