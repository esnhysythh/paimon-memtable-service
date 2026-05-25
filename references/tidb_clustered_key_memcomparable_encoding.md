# TiDB Clustered 表多列主键 Key 编码参考：CommonHandle 与 Mem-Comparable Encode

> 目的：本文只关注 TiDB clustered table 中“多个主键列如何编码成 row key 的 handle 部分”，即：
>
> ```text
> encoded_handle = encode_key(pk_col_1) || encode_key(pk_col_2) || ... || encode_key(pk_col_n)
> ```
>
> 不展开 `table_record_prefix`、`table_id`、`_r` 等表级前缀编码细节。

---

## 1. 背景：Clustered Primary Key 与 CommonHandle

在 TiDB clustered index 表中，row data 的 key 由用户给出的 primary key data 组成；也就是说，主键数据直接决定 row key，value 存 row data。

对于：

```sql
CREATE TABLE t (
  id1 BIGINT NOT NULL,
  id2 VARCHAR(64) NOT NULL,
  v   VARCHAR(64),
  PRIMARY KEY(id1, id2) CLUSTERED
);
```

TiDB 的 row key 可以抽象成：

```text
row_key = table_record_prefix || encoded_handle
```

本文只讨论后半段：

```text
encoded_handle = EncodeKey(id1, id2)
```

对于非单列整数 handle，TiDB 使用 `CommonHandle`。源码注释中说明：

```go
// CommonHandle implements the Handle interface for non-int64 type handle.
// NewCommonHandle creates a CommonHandle from a encoded bytes
// which is encoded by code.EncodeKey.
```

所以多列 clustered primary key 的核心是：

```text
CommonHandle.encoded = codec.EncodeKey(pk_col_1, pk_col_2, ...)
```

---

## 2. 核心目标：逻辑顺序等价于 Bytewise 顺序

TiDB 的 key 编码目标是：

```text
compare_sql_tuple(a, b)
==
bytes.Compare(EncodeKey(a), EncodeKey(b))
```

也就是说：

```text
(id1_a, id2_a) < (id1_b, id2_b)
```

当且仅当：

```text
EncodeKey(id1_a, id2_a) < EncodeKey(id1_b, id2_b)
```

其中 byte comparison 是普通字典序比较，也就是 RocksDB/TiKV 的 key comparator 所依赖的顺序。

TiDB 源码中 `EncodeKey` 的注释写得很直接：

```go
// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
func EncodeKey(loc *time.Location, b []byte, v ...types.Datum) ([]byte, error) {
    return encode(loc, b, v, true)
}
```

这里 `comparable = true` 表示使用 mem-comparable encoding。

---

## 3. 总体编码结构

对于联合主键：

```sql
PRIMARY KEY (c1, c2, c3) CLUSTERED
```

其 handle 编码是：

```text
encoded_handle =
  encoded_datum(c1)
  || encoded_datum(c2)
  || encoded_datum(c3)
```

每个 `encoded_datum` 的结构是：

```text
type_flag || type_specific_memcomparable_bytes
```

例如：

```text
BIGINT:
  intFlag || EncodeInt(value)

VARCHAR / BYTES:
  bytesFlag || EncodeBytes(collation_key_or_raw_bytes)

DATETIME / TIMESTAMP:
  uintFlag || EncodeMySQLTime(value)

DURATION:
  durationFlag || EncodeInt(duration_nanos)

DECIMAL:
  decimalFlag || EncodeDecimal(value)

NULL:
  NilFlag
```

TiDB 的 flag 常量包括：

```go
NilFlag          = 0
bytesFlag        = 1
compactBytesFlag = 2
intFlag          = 3
uintFlag         = 4
floatFlag        = 5
decimalFlag      = 6
durationFlag     = 7
varintFlag       = 8
uvarintFlag      = 9
jsonFlag         = 10
vectorFloat32Flag = 20
maxFlag          = 250
```

对 clustered primary key 而言，主键列在 SQL 语义上通常应为 `NOT NULL`。如果你的系统只参考 clustered primary key 设计，建议直接规定：

```text
primary key columns must be NOT NULL
```

这样可以避免 NULL 排序、NULL 等价性、唯一性语义等复杂问题。

---

## 4. 有符号整数编码：翻转符号位 + Big Endian

普通有符号整数的二进制补码不能直接按字节比较。例如：

```text
-1 的补码字节通常大于 0
```

TiDB 的做法是：

```go
const signMask uint64 = 0x8000000000000000

func EncodeIntToCmpUint(v int64) uint64 {
    return uint64(v) ^ signMask
}

func EncodeInt(b []byte, v int64) []byte {
    var data [8]byte
    u := EncodeIntToCmpUint(v)
    binary.BigEndian.PutUint64(data[:], u)
    return append(b, data[:]...)
}
```

核心思想：

```text
int64 logical order
  -9223372036854775808 ... -1, 0, 1 ... 9223372036854775807

映射为 uint64：
  0 ... 0x7fffffffffffffff, 0x8000000000000000 ... 0xffffffffffffffff
```

即：

```text
encoded = big_endian(uint64(value) ^ 0x8000000000000000)
```

示例：

```text
-1 -> 0x7f ff ff ff ff ff ff ff
 0 -> 0x80 00 00 00 00 00 00 00
 1 -> 0x80 00 00 00 00 00 00 01
```

再加上 datum flag：

```text
BIGINT -1:
  intFlag || 7f ff ff ff ff ff ff ff

BIGINT 0:
  intFlag || 80 00 00 00 00 00 00 00

BIGINT 1:
  intFlag || 80 00 00 00 00 00 00 01
```

由于 `intFlag` 相同，比较真正发生在后面的 8 字节上。

---

## 5. 无符号整数编码：Big Endian

`uint64` 的自然顺序与 big-endian 字节序一致，所以 TiDB 直接写 8 字节 big-endian：

```go
func EncodeUint(b []byte, v uint64) []byte {
    var data [8]byte
    binary.BigEndian.PutUint64(data[:], v)
    return append(b, data[:]...)
}
```

结构：

```text
uintFlag || big_endian_uint64(value)
```

示例：

```text
0 -> uintFlag || 00 00 00 00 00 00 00 00
1 -> uintFlag || 00 00 00 00 00 00 00 01
2 -> uintFlag || 00 00 00 00 00 00 00 02
```

---

## 6. 字符串 / Bytes 编码：8 字节分组 + Marker

变长 bytes/string 不能直接拼接，否则存在两个问题：

1. 无法知道一个字段在哪里结束。
2. 简单 length-prefix 可能破坏字典序。

TiDB 使用 mem-comparable bytes encoding。规则：

```text
原始 bytes 按 8 字节一组切分。
每组后面追加 1 byte marker。
最后一组不足 8 字节时，用 0x00 padding。
marker = 0xff - padding_zero_count。
```

常量：

```go
encGroupSize = 8
encMarker    = 0xff
encPad       = 0x00
```

编码结构：

```text
[group1: 8 bytes][marker1]
[group2: 8 bytes][marker2]
...
[groupN: 8 bytes][markerN]
```

最后一组的 `marker < 0xff`，表示这是终止组。

### 6.1 示例

空 bytes：

```text
raw: []

encoded group:
  00 00 00 00 00 00 00 00 f7

解释：
  padding_count = 8
  marker = 0xff - 8 = 0xf7
```

`[01 02 03]`：

```text
raw:
  01 02 03

encoded:
  01 02 03 00 00 00 00 00 fa

解释：
  padding_count = 5
  marker = 0xff - 5 = 0xfa
```

`[01 02 03 00]`：

```text
raw:
  01 02 03 00

encoded:
  01 02 03 00 00 00 00 00 fb

解释：
  padding_count = 4
  marker = 0xff - 4 = 0xfb
```

正因为 marker 不同，`[01 02 03]` 与 `[01 02 03 00]` 可以区分，并且保持正确的 bytes 字典序。

长度刚好为 8 的 bytes：

```text
raw:
  01 02 03 04 05 06 07 08

encoded:
  01 02 03 04 05 06 07 08 ff
  00 00 00 00 00 00 00 00 f7
```

注意：长度刚好为 8 时，第一组 marker 是 `ff`，表示“没有 padding，还没有结束”；随后还需要追加一个全 padding 的终止组。

### 6.2 加上 datum flag

在 `EncodeKey` 中，bytes/string 前面还有 `bytesFlag`：

```text
encoded_string = bytesFlag || EncodeBytes(raw_or_collation_key)
```

例如空字符串：

```text
bytesFlag
00 00 00 00 00 00 00 00 f7
```

字符串 `"abc"`：

```text
bytesFlag
61 62 63 00 00 00 00 00 fa
```

---

## 7. String Collation：原始 bytes 不一定等于排序 bytes

TiDB 的 `encodeString` 逻辑大致是：

```go
func encodeString(b []byte, val types.Datum, comparable bool) []byte {
    if collate.NewCollationEnabled() && comparable {
        return encodeBytes(b, collate.GetCollator(val.Collation()).ImmutableKey(val.GetString()), true)
    }
    return encodeBytes(b, val.GetBytes(), comparable)
}
```

也就是说：

```text
如果启用了 new collation，并且是在 EncodeKey/comparable 模式：
  string 使用 collator 生成的 sort key

否则：
  string 使用原始 bytes
```

这点非常重要。

如果你自己的 KV 系统只想支持 binary collation，可以直接规定：

```text
string key uses raw UTF-8 bytes / binary bytes order
```

如果你要兼容 SQL collation，例如大小写不敏感、语言相关排序，则不能直接编码原始字符串，而应该编码 collation sort key。

---

## 8. 时间、Duration、Decimal 等类型的编码思路

### 8.1 Time / Date / Datetime / Timestamp

TiDB 对 MySQL time 类型会先转成 packed uint，然后用 `EncodeUint`：

```go
b = append(b, uintFlag)
b, err = EncodeMySQLTime(loc, timeValue, type, b)
```

`EncodeMySQLTime` 内部核心是：

```go
v, err := t.ToPackedUint()
b = EncodeUint(b, v)
```

对于 `TIMESTAMP`，TiDB 还会考虑时区转换，将其转换到 UTC 语义后编码。

抽象结构：

```text
uintFlag || EncodeUint(packed_time)
```

### 8.2 Duration

Duration 可能为负数，所以用 signed int 的保序编码：

```text
durationFlag || EncodeInt(duration)
```

### 8.3 Decimal

Decimal 前面写 `decimalFlag`，随后写 decimal 的 mem-comparable encoding。实现细节比整数和 bytes 更复杂，核心目标仍然是：

```text
numeric decimal order == encoded byte order
```

如果你自己的系统暂时不支持 Decimal，可以先只实现：

```text
int64
uint64
bytes/string
timestamp as int64/uint64
```

---

## 9. Tuple 编码：为什么直接拼接就可以

因为每个 `encoded_datum` 都具备两个性质：

1. **保序**：同类型值的逻辑比较顺序等于字节比较顺序。
2. **自分隔**：decoder 可以知道这个 datum 占多少字节。

因此联合主键可以直接拼接：

```text
encoded_handle =
  encoded_datum(id1)
  || encoded_datum(id2)
  || encoded_datum(id3)
```

对于：

```sql
PRIMARY KEY(id1, id2)
```

如果 `id1 = 100`，所有满足该条件的 row key 都有共同前缀：

```text
encoded_prefix = encoded_datum(100)
```

所以可以构造：

```text
start = table_record_prefix || encoded_datum(100)
end   = PrefixNext(start)
```

扫描 `[start, end)` 即可覆盖所有：

```text
(100, id2_min)
...
(100, id2_max)
```

---

## 10. PrefixNext：前缀查询的右边界

要扫描某个 encoded prefix 下的所有 key，常见方式是构造：

```text
[start, end)
```

其中：

```text
start = prefix
end   = prefix_next(prefix)
```

`prefix_next` 逻辑通常是：

```text
从后向前找第一个不是 0xff 的字节；
把它 +1；
截断其后的所有字节。
```

伪代码：

```pseudo
function prefix_next(key):
    b = copy(key)
    for i from len(b)-1 downto 0:
        if b[i] != 0xff:
            b[i] = b[i] + 1
            return b[0 : i+1]
    return +infinity  // 无有限上界
```

示例：

```text
01 02 03    -> 01 02 04
01 02 ff    -> 01 03
ff ff       -> +infinity
```

TiDB 的 `CommonHandle.Next()` 也使用 `Key(ch.encoded).PrefixNext()` 生成比当前 encoded handle 更大的边界。

---

## 11. 反序列化 / 切分：CommonHandle 如何知道每一列边界

`CommonHandle` 持有两个核心字段：

```go
type CommonHandle struct {
    encoded       []byte
    colEndOffsets []uint16
}
```

构造时，TiDB 会反复调用 `codec.CutOne`：

```go
remain := encoded
endOff := uint16(0)

for len(remain) > 0 {
    if remain[0] == 0 {
        // padded data
        break
    }

    col, remain, err = codec.CutOne(remain)
    endOff += uint16(len(col))
    ch.colEndOffsets = append(ch.colEndOffsets, endOff)
}
```

也就是说，`encoded_handle` 本身没有额外的列数 header，也没有 offset array。列边界是通过每个 datum 自分隔能力推导出来的。

例如：

```text
encoded_handle = col1_encoded || col2_encoded || col3_encoded
```

`CutOne` 第一次返回：

```text
col1_encoded, remain = col2_encoded || col3_encoded
```

第二次返回：

```text
col2_encoded, remain = col3_encoded
```

第三次返回：

```text
col3_encoded, remain = empty
```

然后记录：

```text
colEndOffsets = [
  len(col1),
  len(col1) + len(col2),
  len(col1) + len(col2) + len(col3)
]
```

后续访问第 i 个 encoded column：

```go
func (ch *CommonHandle) EncodedCol(idx int) []byte {
    colStartOffset := uint16(0)
    if idx > 0 {
        colStartOffset = ch.colEndOffsets[idx-1]
    }
    return ch.encoded[colStartOffset:ch.colEndOffsets[idx]]
}
```

---

## 12. CutOne 如何工作

`codec.CutOne` 的逻辑是：

```go
func CutOne(b []byte) (data []byte, remain []byte, err error) {
    l, err := peek(b)
    if err != nil {
        return nil, nil, err
    }
    return b[:l], b[l:], nil
}
```

核心是 `peek(b)`：看第一个 byte，即 type flag，然后根据类型判断这个 encoded datum 的长度。

伪代码：

```pseudo
function peek(encoded):
    flag = encoded[0]
    switch flag:
        case NilFlag:
            length = 1

        case intFlag, uintFlag, floatFlag, durationFlag:
            length = 1 + 8

        case bytesFlag:
            length = 1 + peekBytes(encoded[1:])

        case compactBytesFlag:
            length = 1 + peekCompactBytes(encoded[1:])

        case decimalFlag:
            length = 1 + peekDecimal(encoded[1:])

        case jsonFlag:
            length = 1 + peekJSON(encoded[1:])

        ...
```

对于 `bytesFlag`，`peekBytes` 会每次跳过 8 字节 group + 1 字节 marker，直到遇到一个 marker 表示 `padCount != 0`。

伪代码：

```pseudo
function peek_bytes(b):
    offset = 0
    loop:
        require len(b) >= offset + 9
        marker = b[offset + 8]
        padCount = 0xff - marker
        offset += 9
        if padCount != 0:
            break
    return offset
```

因此 bytes/string 类型虽然变长，但也是可以自分隔的。

---

## 13. DecodeOne：把 encoded column 还原成 Datum

`CommonHandle.Data()` 会对每个 encoded column 调用 `codec.DecodeOne`：

```go
func (ch *CommonHandle) Data() ([]types.Datum, error) {
    data := make([]types.Datum, 0, ch.NumCols())
    for i := range ch.NumCols() {
        encodedCol := ch.EncodedCol(i)
        _, d, err := codec.DecodeOne(encodedCol)
        if err != nil {
            return nil, err
        }
        data = append(data, d)
    }
    return data, nil
}
```

`DecodeOne` 的核心流程：

```pseudo
function decode_one(b):
    flag = b[0]
    payload = b[1:]

    switch flag:
        case intFlag:
            value = DecodeInt(payload)

        case uintFlag:
            value = DecodeUint(payload)

        case floatFlag:
            value = DecodeFloat(payload)

        case bytesFlag:
            value = DecodeBytes(payload)

        case decimalFlag:
            value = DecodeDecimal(payload)

        case durationFlag:
            value = DecodeInt(payload)

        case NilFlag:
            value = NULL

        ...
```

对于 bytes/string：

```pseudo
DecodeBytes:
    loop:
        read 8-byte group
        read 1-byte marker
        padCount = 0xff - marker
        append group[0 : 8 - padCount]
        if padCount != 0:
            validate padding bytes are 0x00
            break
```

---

## 14. 一个完整例子：`PRIMARY KEY(id1 BIGINT, id2 VARCHAR)`

假设：

```sql
PRIMARY KEY(id1, id2) CLUSTERED
```

插入：

```text
id1 = 100
id2 = 'abc'
```

则：

```text
encoded_handle =
  encoded_datum(id1)
  || encoded_datum(id2)
```

### 14.1 id1 编码

`id1 = 100`：

```text
int64 100
uint64(100) ^ 0x8000000000000000
= 0x8000000000000064
```

所以：

```text
encoded_datum(id1) =
  intFlag
  80 00 00 00 00 00 00 64
```

### 14.2 id2 编码

`id2 = 'abc'`，UTF-8 bytes：

```text
61 62 63
```

EncodeBytes：

```text
61 62 63 00 00 00 00 00 fa
```

加上 flag：

```text
encoded_datum(id2) =
  bytesFlag
  61 62 63 00 00 00 00 00 fa
```

### 14.3 合并

```text
encoded_handle =
  03 80 00 00 00 00 00 00 64
  01 61 62 63 00 00 00 00 00 fa
```

其中：

```text
03 = intFlag
01 = bytesFlag
```

如果要扫描：

```sql
WHERE id1 = 100
```

构造前缀：

```text
prefix =
  table_record_prefix
  || 03 80 00 00 00 00 00 00 64
```

范围：

```text
[start, end)
=
[
  prefix,
  PrefixNext(prefix)
)
```

这会覆盖所有：

```text
(id1 = 100, id2 = anything)
```

---

## 15. 为什么 `WHERE id2 = ?` 不能用这个主键前缀直接定位

对于：

```sql
PRIMARY KEY(id1, id2)
```

key 排序是：

```text
id1 first, then id2
```

物理编码是：

```text
encode(id1) || encode(id2)
```

所以：

```sql
WHERE id1 = ?
```

能形成连续 key range。

但是：

```sql
WHERE id2 = ?
```

无法形成连续 key range，因为不同 `id1` 下的相同 `id2` 分散在不同位置：

```text
(1, 'x')
(1, 'y')
(2, 'x')
(2, 'y')
(3, 'x')
...
```

这和 B+Tree 联合索引的左前缀原则相同。

---

## 16. 如果你要复用 TiDB 设计，建议采用的最小规范

如果你的 KV 系统只需要支持常见主键类型，可以先实现一个简化版：

### 16.1 支持类型

```text
int64
uint64
bytes/string with binary collation
timestamp as int64 or uint64
```

### 16.2 每列编码

```text
encoded_value = type_flag || payload
```

建议：

```text
0x03 = int64
0x04 = uint64
0x01 = bytes/string
```

### 16.3 int64

```text
payload = big_endian(uint64(value) ^ 0x8000000000000000)
```

### 16.4 uint64

```text
payload = big_endian(value)
```

### 16.5 bytes/string

```text
payload = EncodeBytes(raw_bytes)

EncodeBytes:
  for each 8-byte group:
    pad last group with 0x00
    marker = 0xff - pad_count
    append group + marker
  if data length is multiple of 8:
    append extra all-zero terminal group + 0xf7
```

### 16.6 tuple

```text
encoded_tuple = encoded_value(c1) || encoded_value(c2) || ... || encoded_value(cn)
```

### 16.7 prefix scan

```text
prefix = encoded_value(c1) || ... || encoded_value(ck)

start = table_prefix || prefix
end   = PrefixNext(start)
```

### 16.8 decoding

```text
while remain not empty:
    flag = remain[0]
    length = peek_length_by_flag(remain)
    encoded_col = remain[0:length]
    datum = decode_one(encoded_col)
    remain = remain[length:]
```

---

## 17. 设计注意事项

### 17.1 Type flag 会参与排序

如果同一列在 schema 中类型固定，那么同一列的 type flag 恒定，不影响同类型值之间排序。

但如果你允许同一 key position 出现多种类型，则 type flag 的大小本身会决定跨类型排序。例如：

```text
intFlag = 3
uintFlag = 4
```

这会让所有 signed int encoded value 排在所有 unsigned int encoded value 前面。数据库系统通常依赖 schema 保证同一列类型固定，不在同一 key position 混用类型。

### 17.2 主键列建议 NOT NULL

TiDB 的 `EncodeKey` 支持 `NilFlag`，但 SQL primary key 一般不允许 NULL。自己实现时建议避免 nullable primary key。

### 17.3 Descending index 需要反转编码

TiDB codec 里也有 `EncodeIntDesc`、`EncodeBytesDesc` 这类降序编码，基本思路是对升序编码结果做 bitwise NOT。若你只做 clustered primary key，通常全 ASC 即可。

### 17.4 Collation 是字符串编码的最大复杂点

如果要支持非 binary collation，需要用 sort key，而不是原始字符串 bytes。

### 17.5 PrefixNext 生成的 key 不一定可解码

`PrefixNext(prefix)` 只是作为扫描右边界，它可能不是一个合法的 encoded tuple。这没关系，因为它只用于比较边界，不需要反序列化。

---

## 18. 参考源码与文档

- TiDB Clustered Indexes 文档：说明 clustered 表中 row data 的 key 由用户主键数据组成。
- `pkg/util/codec/codec.go`：`EncodeKey`、datum flag、`DecodeOne`、`CutOne`、`peek`。
- `pkg/util/codec/number.go`：`EncodeInt`、`EncodeUint`。
- `pkg/util/codec/bytes.go`：`EncodeBytes`、`DecodeBytes`、mem-comparable bytes format。
- `pkg/kv/key.go`：`CommonHandle`、`NewCommonHandle`、`EncodedCol`、`Data`、`Compare`、`Next`。
- `pkg/tablecodec/tablecodec.go`：`EncodeRowKey` / `EncodeRowKeyWithHandle`，说明 row key 是 table record prefix 加上 handle encoded bytes。

---

## 19. 一句话总结

TiDB clustered 多列主键 key 编码的核心是：

```text
encoded_handle =
  memcomparable_encode(pk_col_1)
  || memcomparable_encode(pk_col_2)
  || ...
  || memcomparable_encode(pk_col_n)
```

其中每个 encoded column 都是：

```text
type_flag || self-delimiting_order-preserving_payload
```

因此它同时满足：

```text
1. bytewise order == SQL tuple order
2. 可以通过 CutOne 顺序切分每个主键列
3. 可以用左前缀构造 LSM range scan
```
