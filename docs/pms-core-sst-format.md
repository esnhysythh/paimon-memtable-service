# PMS Core SST Format 设计文档

## 1. 模块定位

SST 是 PMS 本地 LSM 缓冲层的磁盘只读文件格式，承接 `ImmutableMemTable` 的 flush 输出，并作为后续本地点查、SST 合并、Paimon Sink 和崩溃恢复边界判断的基础数据单元。

PMS SST 的设计主要参考 LevelDB/RocksDB 的 Block Based Table 思路：文件由有序 Data Block、Index Block、元信息 Block 和 Footer 组成；Block 内按 Key 排序并支持 seek；Footer 通过 BlockHandle 定位关键元数据。PMS 不照搬 LevelDB 的 MVCC InternalKey 设计，而是根据 PMS V1 的轻量 sequence 和 Deduplicate Merge Engine 语义进行简化。

## 2. 设计目标

- 点查高效：通过 BloomFilter 快速排除不存在的 Key，通过 Index Block 定位少量 Data Block。
- 顺序归并友好：SST iterator 按 Paimon 主键序输出，供本地 compact 和 Paimon Sink 使用。
- Tombstone 语义完整：读取结果必须区分 miss、delete、put，避免已删除数据从更老层或 Paimon 穿透中复活。
- Sequence 边界可恢复：SST 文件和 `SSTMeta` 都必须记录 `minSequenceId/maxSequenceId`。
- 格式简单可演进：V1 优先完成可 flush、可点查、可恢复边界的最小闭环，压缩、逐 Block CRC、多级索引可后续演进。

## 3. 与 LevelDB 的关系

### 3.1 保持一致的部分

PMS SST 参考 LevelDB Java 移植版 `org.iq80.leveldb.table` 包中的核心结构：

| LevelDB 设计 | PMS SST 对应设计 | 说明 |
|--------------|------------------|------|
| Table file 由 Data Block、Meta Block、Index Block、Footer 组成 | PMS SST 由 Data Block、BloomFilter Block、Index Block、Properties Block、Footer 组成 | 文件组织方式一致，PMS 将 Bloom 和属性显式化 |
| `BlockHandle(offset, size)` | `BlockHandle(offset, size)` | 用于从 Footer/Index 定位 Block |
| Data Block 内 Entry 按 Key 递增排序 | Data Block 内 Entry 按 PMS `Key` 无符号字节序递增排序 | PMS Key 顺序必须与 Paimon 主键序一致 |
| Block 内 prefix compression + restart points | V1 采用相同的 restart points 结构 | 支持 block 内二分 seek + 局部线性扫描 |
| Index Block 映射 `indexKey -> BlockHandle` | V1 使用 `blockLastKey -> BlockHandle` | `blockLastKey` 是一种最朴素的 indexKey；后续可优化为 shortest separator |
| Footer 固定位置读取 | Footer 固定长度并位于文件末尾 | 打开 SST 时先读 Footer，再读 Index/Properties/Bloom |
| Builder/Reader/Iterator 分层 | `SSTWriter` / `SSTReader` / `SSTIterator` | 便于单元测试和后续替换 IO 实现 |

### 3.2 明确不同的部分

| LevelDB 设计 | PMS SST 设计 | 原因 |
|--------------|--------------|------|
| Key 使用 `InternalKey = userKey + sequence + valueType` | Key 只使用 PMS/Paimon 主键字节；sequence 放在 Value payload 和元数据中 | PMS V1 不提供 MVCC 快照读，同一 Key 只保留 latest value |
| 同一 user key 可保存多个版本 | 同一 SST 内同一 Key 只保存一个 latest value 或 tombstone | PMS 遵循 Deduplicate Merge Engine 语义 |
| ValueType 是 InternalKey 的一部分 | 接口层 tombstone 由 `Value.bytes == null` 表达，磁盘层 tombstone 由 `valueLen = -1` 表达 | 与现有 MemTable `Value` 和 WAL `valueLen = -1` 模型保持一致 |
| MetaIndex Block 用于查找多种 meta block | V1 可省略 MetaIndex，Footer 直接记录 Bloom/Index/Properties 的 BlockHandle | PMS V1 元数据种类固定，直接定位更简单 |
| 默认逐 Block trailer checksum | V1 先使用全文件 CRC；逐 Block CRC 作为后续演进 | 与当前设计文档的 Footer CRC 策略一致，降低最小闭环复杂度 |
| 可选 Snappy 压缩 | V1 默认不压缩，保留 `compressionType = NONE` 元信息 | Block 级压缩实现不复杂，但会扩大最小闭环的依赖和测试面 |

## 4. 文件整体布局

PMS SST V1 文件布局如下：

```text
┌──────────────────────────────────────────────────────┐
│ Data Block 0                                         │
│ Data Block 1                                         │
│ ...                                                  │
│ Data Block N                                         │
│ BloomFilter Block                                    │
│ Index Block                                          │
│ Properties Block                                     │
│ Footer                                               │
└──────────────────────────────────────────────────────┘
```

写入顺序固定为：先写 Data Blocks，再写 BloomFilter Block、Index Block、Properties Block，最后写 Footer。读取时从文件末尾读取 Footer，再通过 Footer 中的 BlockHandle 加载 Properties、Index 和 BloomFilter。

## 5. 基本编码约定

- 多字节定长整数编码：统一使用 little-endian，复用当前 WAL util 中 `Slice/SliceInput/SliceOutput` 的 `writeInt/writeLong/readInt/readLong` 语义。
- 变长整数编码：Block entry 中的 `sharedKeyLen/unsharedKeyLen` 和 `BlockHandle` 的 `offset/size` 使用 varint，参考 LevelDB `VariableLengthQuantity`。
- 长度字段：Block 内部的 Key 长度使用 varint；PMS Entry 内的 `sequenceId/valueLen` 使用固定长度，便于解析和调试。
- Key 比较：使用 `Key.compareTo()` 的无符号字节序，不使用 Java signed byte 比较。
- 文件写入原子性：SST 先写临时文件，完成 Footer 和 fsync 后再 rename 为正式文件；正式文件名只在完整 SST 生成后对 BucketDirector 可见。

## 6. Data Block 格式

Data Block 内 Entry 按 Key 递增排序。V1 采用 LevelDB 风格 prefix compression 和 restart points：

```text
Entry:
┌────────────────┬──────────────────┬────────────┬──────────┬─────────────┬────────┐
│ sharedKeyLen   │ unsharedKeyLen   │ sequenceId │ valueLen │ unsharedKey │ value  │
│ varint32       │ varint32         │ int64      │ int32    │ bytes       │ bytes  │
└────────────────┴──────────────────┴────────────┴──────────┴─────────────┴────────┘

Block Tail:
┌──────────────────────────┬──────────────┐
│ restartOffsets           │ restartCount │
│ int32[restartCount]      │ int32        │
└──────────────────────────┴──────────────┘
```

Restart 规则：
- 第一条 Entry 必须是 restart point，`sharedKeyLen = 0`。
- 每 `blockRestartInterval` 条 Entry 产生一个 restart point，默认建议为 16。
- seek 时先在 restart point 中二分，再在 restart 区间内线性扫描。

### 6.1 Tombstone 落盘语义

Data Block Entry 直接保存 PMS 的 `sequenceId` 和 signed `valueLen`，不使用 LevelDB 的 InternalKey，也不额外引入 `kind/valueType` 字段：

```text
valueLen >= 0  -> PUT，后面跟 value bytes
valueLen = -1  -> DELETE tombstone，后面没有 value bytes
valueLen < -1  -> 文件损坏
```

约束：
- `sequenceId > 0`。
- `DELETE` 必须落盘为 `valueLen = -1`，该约定与 WAL DATA 记录中的 delete 表达一致。
- `PUT` 的 `valueLen = 0` 在底层字节接口中允许保留，但不表示 tombstone 或业务 NULL；RowCodec 接入后可将其视为非法或保留编码。
- SST Reader 读到 `valueLen = -1` 时，必须还原为 `Value.tombstone(sequenceId)`；读到 `valueLen >= 0` 时，还原为 `new Value(valueBytes, sequenceId)`。
- 同一 SST 文件内同一 Key 只保存一个 Entry；flush 单个 ImmutableMemTable 时天然满足，compact 多个 SST 时必须保留最新层或最大 sequence 对应的 Entry。

## 7. BloomFilter Block

原版 LevelDB 支持通过 FilterPolicy 在 table 中引入 Filter Block，通常由 MetaIndex Block 间接定位；当前可参考的 Java 移植版 table 包未实现 Bloom/Filter Block，MetaIndex 也是空 block。PMS V1 因元数据种类固定，选择自行定义 BloomFilter Block，并由 Footer 直接记录 `bloomHandle`。

BloomFilter Block 基于 user key 构建，PUT 和 DELETE 都必须加入 BloomFilter。原因是 tombstone 命中时必须阻断更老层或 Paimon 穿透。

建议格式：

```text
┌──────────────┬───────────────┬─────────────┬────────────┐
│ bitCount     │ hashCount     │ keyCount    │ bitset     │
│ int32        │ int32         │ int32       │ bytes      │
└──────────────┴───────────────┴─────────────┴────────────┘
```

默认 false positive rate 建议为 0.01。BloomFilter 只能用于排除不存在的 Key；若 Bloom 命中，仍必须通过 Index/Data Block 验证真实 Entry。

## 8. Index Block

Index Block 采用和 Data Block 相同的 block entry 编码方式。每条 index entry：

```text
indexKey -> blockHandle
```

`indexKey` 是 Index Block 中用于定位某个 Data Block 的边界 key，不一定是真实业务 key。V1 中 `indexKey` 直接使用对应 Data Block 的 `blockLastKey`，即该 Data Block 中真实存在的最后一个 Key。查找 Key 时，找到第一个 `indexKey >= targetKey` 的条目，再读取其指向的 Data Block。

LevelDB 的 shortest separator 是对 indexKey 的空间优化：当某个 Data Block 的最后一个 Key 为 `lastKey`，下一个 Data Block 的第一个 Key 为 `nextFirstKey` 时，可以选择一个满足 `lastKey <= indexKey < nextFirstKey` 的更短字节串作为 indexKey。这个 indexKey 可能不是真实业务 Key，但仍能正确定位 Block。例如 `lastKey = "the quick brown fox"`、`nextFirstKey = "the who"` 时，可用 `"the r"` 作为更短的边界 key。PMS V1 不要求该优化，先使用 `blockLastKey` 保证实现简单。

`blockHandle` 编码：

```text
┌────────┬──────┐
│ offset │ size   │
│ varint │ varint │
└────────┴──────┘
```

后续可参考 LevelDB 的 `findShortestSeparator/findShortSuccessor` 缩短 index key，但 V1 不要求。

## 9. Properties Block

Properties Block 保存 SST 文件级元数据，用于构建内存中的 `SSTMeta`，避免每次状态判断都扫描文件。

建议字段：

```text
version              int32
entryCount           int64
dataBlockCount       int32
fileSize             int64
minKey               bytes
maxKey               bytes
minSequenceId        int64
maxSequenceId        int64
createdAtMillis      int64
hasTombstone         boolean
compressionType      byte     // V1 = NONE
checksumType         byte     // V1 = FULL_FILE_CRC32; 后续可增加 BLOCK_TRAILER_CRC32C
```

`SSTMeta` 至少应包含：

```java
record SSTMeta(
    long runId,
    long minFlushId,
    long maxFlushId,
    Path path,
    long fileSize,
    long entryCount,
    Key minKey,
    Key maxKey,
    long minSequenceId,
    long maxSequenceId,
    long createdAtMillis,
    SSTState state,
    long refCount
) {}
```

`SSTState` 初期可包含 `NEW` 和 `SINKED`；带 Mem 缓存的状态由 BucketDirector 的状态条目表达，而不是写进 SST 文件。SST 数据文件 publish 后不再 rename, 文件名只包含稳定的 `flushId` range, 例如 `sst-000001-000001.sst`。可靠状态来源是 metadata 和 SinkMeta, 而不是文件名或 Footer。

## 10. Footer 格式

Footer 位于文件末尾，固定长度，便于打开文件时一次定位。

建议 V1 Footer：

```text
┌───────────────┬────────────┬───────────────┬─────────────┬──────────────────┬───────────────┬────────────┐
│ magic         │ version    │ bloomHandle   │ indexHandle │ propertiesHandle │ fullFileCrc32 │ padding    │
│ 8 bytes       │ int32      │ BlockHandle   │ BlockHandle │ BlockHandle      │ int32         │ bytes      │
└───────────────┴────────────┴───────────────┴─────────────┴──────────────────┴───────────────┴────────────┘
```

约束：
- `magic` 建议使用固定 8 字节，例如 `PMS_SST1`。
- Footer 使用固定长度；其中每个 `BlockHandle` 按 LevelDB 风格预留最大编码长度并用 0 padding 补齐。
- `fullFileCrc32` 覆盖 Footer 之前的全部数据，不覆盖 Footer 自身。
- 打开或注册 SST 时校验一次 full-file CRC；点查路径不应每次扫描全文件校验。
- V1 不做逐 Data Block CRC。后续如果引入 Block Trailer，可参考 LevelDB 的 `compressionType + crc32c` 设计。

### 10.1 Block Trailer 与压缩演进

LevelDB 在每个 block 后追加 5 字节 trailer：

```text
┌─────────────────┬────────┐
│ compressionType │ crc32c │
│ byte            │ int32  │
└─────────────────┴────────┘
```

如果 PMS 后续引入 Block Trailer，读取单个 Block 时流程为：根据 `BlockHandle(offset, size)` 读取 block data，再读取紧随其后的 trailer；先校验 CRC，再根据 `compressionType` 判断是否需要 Snappy/LZ4 解压。这个逻辑并不复杂，但会引入压缩库依赖、压缩收益判断、CRC 覆盖范围和更多测试用例，因此 V1 最小闭环暂不实现，只在 Properties/Footer 中保留演进空间。

## 11. 查询语义

SST 查询接口必须表达三态：

```java
try (SSTReadSnapshot snapshot = storageManager.readSnapshot(ssts)) {
    Optional<Value> value = snapshot.get(meta, key);
    SSTEntryIterator iterator = snapshot.openIterator(meta, startInclusive, endExclusive);
}
```

`LocalStorageManager` 不直接暴露单 SST `get/openIterator` 作为外部读取入口；调用方必须先创建 `SSTReadSnapshot`, 再通过 snapshot 读取。snapshot 注册 read epoch, compact/evict 只能把旧 SST 放入 retired queue, 等所有可能看到旧 SST 的 snapshot 关闭后才物理删除文件。

语义：

| 返回值 | 含义 | 查询路径动作 |
|--------|------|--------------|
| `Optional.empty()` | SST miss | 继续查更老层 |
| `Optional.of(value)` 且 `value.bytes() != null` | PUT 命中 | 返回 value bytes |
| `Optional.of(value)` 且 `value.bytes() == null` | DELETE tombstone 命中 | 停止穿透，返回查询不存在 |

禁止使用 `Optional<byte[]>` 表达 SST 查询结果，因为它无法区分 miss 和 tombstone。

范围 iterator 输出 `[startInclusive, endExclusive)` 内的原始 SST entry，包含 PUT 和 DELETE tombstone，且按 key 升序排列。它不在 SST 层做多版本合并；跨 memtable、NEW SST、SINKED SST 的 latest sequence 选择和 tombstone 过滤由 BucketDirector 统一完成。

## 12. Flush 规则

`flushToSST(ImmutableMemTable memTable)` 必须满足：

- 输入 iterator 已按 Key 排序输出。
- 输出 SST 继承 `memTable.minSequenceId()/maxSequenceId()`。
- 每个 Entry 写入 Data Block，同时将 Key 加入 BloomFilter。
- 写入完成后生成 Properties 和 Footer，并校验生成的 `SSTMeta` 与输入边界一致。
- SST 对 BucketDirector 可见前，必须保证文件已完整写入。

## 13. Compact 规则

本地 SST compact 是多路归并：

- 输入 SST iterator 均按 Key 升序。
- 同一 Key 出现在多个输入中时，保留更新层级更高的 Entry；若层级无法表达，则保留 `sequenceId` 更大的 Entry。
- tombstone 与普通 PUT 一样参与归并，且可成为输出 Entry。
- 不同生命周期类别不混合 compact：`newSST` 与 `sinkedSST` 不混并。
- 输出 SST 的 `minSequenceId/maxSequenceId` 覆盖所有输入文件的 sequence 范围。

## 14. V1 后续演进

- Block Trailer：为每个 Block 增加 `compressionType + crc32c`，点查时只校验读取的 Block。
- 压缩：对 Data Block 支持 Snappy 或 LZ4；实现方式是在读取 Block Trailer 后按 `compressionType` 解压。
- Index key 缩短：引入 shortest separator，减少 Index Block 空间。
- 多级 Index：当 SST 很大时引入二级索引。
- Prefix Bloom：若 Paimon 主键或查询模式稳定，可引入前缀 Bloom。
- MVCC：如果未来支持快照读，再将 Key 形态升级为 `(userKey, sequenceId)`，并引入可见性过滤。
