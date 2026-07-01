# PMS Protocol 设计文档

## 1. 模块定位

`pms-protocol` 是 PMS 对外 RPC 协议的 wire contract 模块，沉淀从
`pms-protocol-demo` 验证后迁入 PMS 主项目的稳定边界。

本模块只负责 client 与 server 之间的协议 envelope：

- 协议常量、endpoint path、capability 名称和 handshake model。
- 写入、删除、点查、prefix 查询的 raw DTO。
- HTTP/2 binary hot path 使用的 request/response codec。
- status code、lookup 三态和 batch 整批语义。

本模块不解释 `keyBytes` 和 `rowBytes` 的内部格式。它们由 `pms-codec`
产生，对协议层是 opaque bytes。`pms-protocol` 不依赖 `pms-core`、
`pms-codec` 或 Paimon runtime。

## 2. 依赖边界

推荐依赖方向：

```text
pms-protocol -> JDK + Jackson(handshake JSON)
pms-client   -> pms-protocol
pms-server   -> pms-protocol + pms-core + pms-codec + pms-sink-paimon + pms-lookup-paimon
pms-codec    -> Paimon
pms-core     -> no protocol, no Paimon, no server
```

`pms-protocol` 与 `pms-codec` 的职责不同：

- `pms-codec`：Paimon row/key 到 PMS KV bytes 的数据层 codec。
- `pms-protocol`：PMS KV bytes 在网络上的请求/响应 envelope。

因此 protocol wire codec 不并入 `pms-codec`。

## 3. Transport 与 Endpoint

热路径 API 使用 HTTP/2 + `application/octet-stream` binary body。JSON 只用于
handshake、health、调试或历史兼容接口，不作为写入和点查热路径。

v1 endpoint：

| Method | Path | Request | Response | 说明 |
|---|---|---|---|---|
| `GET` | `/pms/api/v1/handshake` | JSON | JSON | 协议能力与限制 |
| `POST` | `/pms/api/v1/local/put` | `RecordBatch` | `WriteResult` | 单条 PUT |
| `POST` | `/pms/api/v1/local/delete` | `RecordBatch` | `WriteResult` | 单条 DELETE |
| `POST` | `/pms/api/v1/local/writeBatch` | `RecordBatch` | `WriteResult` | 批量 PUT/DELETE |
| `POST` | `/pms/api/v1/local/get` | `KeyBatch` | `LookupBatchResult` | PMS local 点查 |
| `POST` | `/pms/api/v1/local/getPrefix` | `KeyBatch` | `LookupBatchResult` | PMS local prefix 查询 |
| `POST` | `/pms/api/v1/full/get` | `KeyBatch` | `LookupBatchResult` | 完整表点查语义 |

`/local/writeBatch` 是 PMS v1 的 canonical batch write endpoint。demo 中的
`/local/putBatch` 不进入 PMS 主项目协议契约。

## 4. Handshake

client 在调用热路径 endpoint 前必须完成 handshake，并确认协议名称、版本、HTTP/2
要求、能力和大小限制。

示例：

```json
{
  "protocol": "pms-http2-binary",
  "protocolVersion": 1,
  "requiredHttpVersion": "HTTP_2",
  "backend": "pms",
  "maxKeyBytes": 65536,
  "maxRowBytes": 16777216,
  "maxBatchEntries": 1024,
  "maxConcurrentStreams": 512,
  "maxRequestBodyBytes": 33554432,
  "maxResponseBodyBytes": 33554432,
  "capabilities": [
    "localPut",
    "localDelete",
    "localWriteBatch",
    "localGet",
    "localGetPrefix",
    "fullGet",
    "strictHttp2"
  ]
}
```

handshake 是非热路径 JSON。`pms-protocol` 提供 model 与 JSON 编解码，server 和
client 复用该 model，避免 capability 字符串和限制字段漂移。

## 5. Status 与查询语义

`PmsStatus` 使用 int32 编码：

| 值 | 名称 | 含义 |
|---:|---|---|
| 0 | `OK` | 成功 |
| 1 | `BAD_REQUEST` | 请求格式或 endpoint 语义错误 |
| 2 | `OVERLOADED` | server 过载，写入或查询被拒绝 |
| 3 | `SCHEMA_MISMATCH` | schema 不匹配，v1 预留 |
| 4 | `SHUTTING_DOWN` | server 正在停机 |
| 5 | `INTERNAL_ERROR` | server 内部错误 |
| 6 | `LOOKUP_UNAVAILABLE` | 完整表点查无法证明结果正确，可重试 |

`LOOKUP_UNAVAILABLE` 是 PMS 主项目相对 protocol-demo 的收口修正。`pms-lookup-paimon`
的 `UNKNOWN` 必须映射为该状态或等价可重试错误，不能降级为 `MISS`。

`LookupResultType` 只表示 `status = OK` 时的每个 key 查询结果：

| 值 | 名称 | 含义 |
|---:|---|---|
| 0 | `HIT` | 命中 row |
| 1 | `MISS` | 未命中 |
| 2 | `DELETED` | 命中 tombstone |

`DELETED` 不能被调用方当作普通 miss 后继续穿透 Paimon。

## 6. Binary 基础类型

```text
int32       4 bytes, signed, big-endian
uvarint32  unsigned varint, base-128, little-endian groups, max 5 bytes
bytes      raw bytes, length provided by surrounding field
```

v1 decoder 必须：

- 拒绝 trailing bytes。
- 拒绝非 canonical `uvarint32`。
- 拒绝超出 handshake/server config 限制的 key、row、count 和 response body。
- 不解释 `keyBytes`、`rowBytes` 内部格式。

## 7. RecordBatch 与写入语义

`Record`：

```text
[recordLength:int32]
[op:uvarint32]
[keyLength:uvarint32]
[keyBytes]
[rowBytes?]
```

约束：

- `op = PUT` 时必须包含 `rowBytes`，允许空 row bytes。
- `op = DELETE` 时必须省略 `rowBytes`。
- 写入 key 必须非空。

`RecordBatch`：

```text
[recordCount:uvarint32]
[record1]
[record2]
...
[recordN]
```

v1 采用整批语义：

- 请求格式错误、过载、停机或内部错误：整批失败，`acceptedCount = 0`。
- 成功：整批成功，`acceptedCount = recordCount`。
- v1 不使用 `acceptedCount` 表达部分成功。

PMS core 已提供 `writeBatch(List<WriteOp>)`，server adapter 后续必须将一个
`RecordBatch` 映射成一次 core batch 写入：WAL 整体写入，memTable 按 batch 内顺序
apply。若 WAL 成功后 apply 失败，server 不应返回普通非 `OK` 掩盖状态；当前 PMS
core 会进入 fatal 路径，client 侧结果按 unknown/failure 策略处理。

## 8. KeyBatch 与查询响应

`KeyBatch`：

```text
[keyCount:uvarint32]
[key1Length:uvarint32]
[key1Bytes]
...
```

v1 中 `local/get`、`full/get` 和 `local/getPrefix` 均要求 `keyCount = 1`。点查
key 必须非空；prefix 查询可以允许空 prefix，但 server 可基于配置拒绝。

`LookupBatchResult`：

```text
[status:int32]
[resultCount:uvarint32]
[lookupResult1]
[lookupResult2]
...
```

每个 `lookupResult`：

```text
[resultType:uvarint32]
[rowLength:uvarint32?]
[rowBytes?]
```

约束：

- `status != OK` 时 `resultCount = 0`。
- `resultType = HIT` 时必须携带 row bytes。
- `MISS` 与 `DELETED` 不携带 row bytes。
- `local/getPrefix` v1 只返回 `HIT` result，且不分页；server 必须限制结果数量或
  response body 大小。

## 9. 当前落地状态

当前 PMS 主项目已落地：

- `pms-protocol` 模块：DTO、handshake model、binary codec、`RawKvStore` 边界与 golden tests。
- `pms-server` HTTP/2 binary endpoint：基于 Jetty h2c，在同一监听端口上同时保留旧 JSON debug API。
- server raw adapter：`RecordBatch` 映射为一次 `PMSBucketDirector.writeBatch()`；local/full/prefix 查询返回 raw row bytes。
- full get：local miss 后穿透 `pms-lookup-paimon`，lookup UNKNOWN 映射为 `LOOKUP_UNAVAILABLE`。
- `pms-client` raw HTTP/2 client：复用本协议的 handshake、DTO 和 binary codec，提供 raw batch 写入、local/full/prefix 查询与轻量 batch writer。

尚未实现：

- row-aware client facade。
- benchmark。

这些内容分别进入后续 client 和 benchmark 阶段。
