# Flink Connector PMS 模块规范

## 1. 文档状态

本文档是 `flink-connector-pms` 的 V1/MVP 实现规范，替代早期仅描述单条 Sink
写入的草案。当前实现已经落地本文定义的 Sink、Lookup Join Source 和精确主键
DELETE 三条业务路径；后续测试、评审与演进仍以本文档为准。

当前已确定的核心决策：

- 编译和测试基线固定为 Flink `1.20.3`，只使用 Flink 1.20 公共 API，不依赖
  1.21、2.x 或更高版本才提供的能力。
- 提供 Flink SQL/Table API 的 PMS Sink 与 processing-time Lookup Join Source。
- Sink 为 At-Least-Once，不在本轮扩展 PMS request id、事务或幂等提交协议。
- Lookup 查询 PMS 当前最新状态，不提供 event-time 历史版本或 time travel。
- 同一 Flink 写入作业内强制按完整 primary key shuffle；MVP 不提供关闭开关。
- 同步和异步 Lookup 都支持，默认异步；Connector 本地缓存默认不存在，也不提供启用选项。
- 支持完整主键常量等值条件的 SQL `DELETE` pushdown；不实现需要扫描旧数据的通用
  row-level delete。
- 不实现 PMS Catalog。用户通过
  `CREATE TABLE ... WITH ('connector' = 'pms')` 声明 Flink 表。

## 2. 模块定位与边界

`flink-connector-pms` 是 Flink `RowData` 与 `pms-client` 之间的适配模块：

```text
Flink SQL / Table API
          |
          v
flink-connector-pms
  - DynamicTableSource / LookupTableSource
  - DynamicTableSink / SupportsDeletePushDown
  - RowData <-> Paimon InternalRow adapter
  - primary-key shuffle
  - Sink batching / Lookup runtime
          |
          v
      pms-client
          |
          v
       PMS Server
```

模块依赖方向固定为：

```text
flink-connector-pms -> pms-client -> pms-codec / pms-protocol
```

`pms-core` 仍保持 byte-oriented，不允许为了 Flink Connector 引入 Flink 类型、
Paimon `InternalRow` 或反向依赖。

### 2.1 MVP 包含

- `DynamicTableSourceFactory` 与 `DynamicTableSinkFactory`，factory identifier 为 `pms`。
- 基于 Sink V2 的流式 upsert/delete 写入。
- processing-time temporal Lookup Join，支持同步和异步运行方式。
- 完整 primary key 常量等值 SQL `DELETE` pushdown。
- Flink/Paimon/PMS schema 与类型校验。
- 可随 SQL Client 或 Flink 作业分发的 Connector JAR。
- 单元测试、Planner 测试、MiniCluster/真实 PMS 集成测试。

### 2.2 MVP 明确不包含

- Scan Source、批量全表读取、普通 `SELECT * FROM pms_table`。
- event-time 历史 Lookup、snapshot/time-travel Lookup。
- PMS Catalog、自动发现表或自动从 PMS 创建 Flink schema。
- Connector 侧 partial/full Lookup cache。
- 任意谓词 `DELETE`、row-level delete、SQL `UPDATE`。
- Exactly-Once、Flink checkpoint 与 PMS/Paimon snapshot 的事务绑定。
- 多 endpoint 发现、负载均衡、故障转移和写入 fencing。
- schema evolution、运行期 schema reload。
- 部分列 upsert、metadata columns、nested projection pushdown。

“不支持 SQL `UPDATE`”与“Sink 可以接收 `UPDATE_AFTER` changelog”并不冲突：
前者需要读取并改写目标表中的既有行，后者只是把上游已经产生的完整 changelog 写入
PMS。

## 3. 版本、依赖与发布

### 3.1 版本约束

| 依赖 | MVP 版本/范围 | 说明 |
|------|---------------|------|
| Java | 17 | 与 PMS 父工程一致 |
| Flink | 1.20.3 | compile/test 基线；不使用更高版本 API |
| Paimon | 1.4.1 | 与 `pms-client`、`pms-codec` 一致 |
| PMS modules | 当前项目版本 | `pms-client`、`pms-codec`、`pms-protocol` |

Flink 依赖使用 `provided` scope，不打入 Connector JAR。CI 至少对 Flink 1.20.3
执行编译、Planner 测试与 MiniCluster 测试。

### 3.2 Flink/Paimon 薄适配层

实现参考 Paimon Connector 的 `LogicalTypeConversion`、`FlinkRowWrapper` 和
`FlinkRowData`，但不直接依赖 `paimon-flink-common`。Paimon 1.4.1 的该模块包含为
Flink 1.x 回填的 `VariantType` API；把它放入 Flink 1.20 classpath 会向
`org.apache.flink.*` 命名空间注入当前版本不存在的类。PMS 本身又明确不支持
`VARIANT`，为复用这些适配器引入该兼容层没有收益。

Connector 因此只保留三个小而明确的自有组件：

- `PmsFlinkTypeAdapter`：逐层比较 Flink/Paimon 类型、类型参数和 nullability；
- `PmsFlinkRowWrapper`：将 Flink `RowData` 只读适配为 Paimon `InternalRow`；
- `PmsFlinkRowData`：将 PMS 解码得到的 Paimon `InternalRow` 只读适配为 Flink
  `RowData`。

适配器只实现第 5 节声明的 PMS 类型交集，遇到 `RAW`、`VARIANT`、`BLOB` 或
`VECTOR` 立即拒绝。Paimon 类型仍被限制在 `flink-connector-pms` 内部，不进入
`pms-core` API。发布依赖只保留实际需要的 `paimon-api` / `paimon-common`；
Connector 在依赖边界排除 `paimon-core` / `paimon-format`，不把文件读写、Codegen
等无关能力带入 SQL Client。

### 3.3 JAR 与 classloader

发布物是一个可直接放入 Flink `$FLINK_HOME/lib` 或通过 `pipeline.jars` 分发的
Connector JAR：

- 保留 `META-INF/services/org.apache.flink.table.factories.Factory`；
- 不打包或 relocate Flink 类；
- 打包 `pms-client` 及其运行时依赖；
- 对随 Connector 打包的 Paimon 实现类进行 shading/relocation，避免与用户作业中的
  Paimon Connector 版本冲突；
- 对 PMS handshake 使用的 Jackson 进行 relocation，不向 Flink `lib` 暴露
  `com.fasterxml.jackson.*`；
- 已打入 Connector JAR 的 `pms-client` 与 Jackson 在发布 POM 中标记为 optional，
  Maven 消费者不会再次获得未 relocation 的依赖树；
- SLF4J API 由 Flink 1.20 提供，不打入 Connector JAR；
- 排除 Connector 生产路径没有使用的 Paimon Core/Format；
- 排除 Paimon API JAR 已经内嵌、但 `paimon-common` POM 再次声明的 Jackson、
  Guava 与 Caffeine shade artifacts，以及不需要的原始压缩库；
- shade 后必须保留 factory service resource，并执行 SQL Client factory discovery
  冒烟测试。

## 4. SQL 使用界面

### 4.1 表声明

PMS V1 不提供 Catalog。用户必须显式声明与 PMS 绑定 Paimon 表一致的物理 schema：

```sql
CREATE TABLE pms_user_dim (
    tenant_id BIGINT NOT NULL,
    user_id   BIGINT NOT NULL,
    user_name STRING,
    level     INT,
    updated_at TIMESTAMP(6),
    PRIMARY KEY (tenant_id, user_id) NOT ENFORCED
) WITH (
    'connector' = 'pms',
    'endpoint' = 'http://pms-host:9090'
);
```

MVP 不在 Flink DDL 中重复声明 Paimon partition/bucket 配置。PMS Server 已绑定唯一
Paimon 表并负责真实 partition/bucket 路由，因此该 Connector 表不得声明
`PARTITIONED BY`。

### 4.2 Sink

```sql
INSERT INTO pms_user_dim
SELECT tenant_id, user_id, user_name, level, updated_at
FROM upstream_cdc;
```

上游可以产生 `INSERT`、`UPDATE_AFTER` 和 `DELETE`。Connector 写入的是完整物理行，
不支持部分列 upsert。

### 4.3 Lookup Join

```sql
CREATE TABLE orders (
    order_id BIGINT,
    tenant_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(18, 2),
    proc_time AS PROCTIME()
) WITH (...);

SELECT o.order_id, o.amount, d.user_name, d.level
FROM orders AS o
LEFT JOIN pms_user_dim FOR SYSTEM_TIME AS OF o.proc_time AS d
ON o.tenant_id = d.tenant_id
AND o.user_id = d.user_id;
```

这里的 `FOR SYSTEM_TIME AS OF o.proc_time` 是 Flink processing-time Lookup Join
语法，不表示 PMS 保存或查询历史版本。每次查询读取 PMS 在请求执行时可见的当前状态。

### 4.4 精确主键 DELETE

```sql
DELETE FROM pms_user_dim
WHERE tenant_id = 100 AND user_id = 200;
```

该语句走 `SupportsDeletePushDown`，直接把 `(100, 200)` 编码为 PMS key 并调用
`PmsClient.delete`。它不先查询 PMS，也不扫描或反查 Paimon。

以下语句不受支持：

```sql
-- 缺少完整主键
DELETE FROM pms_user_dim WHERE tenant_id = 100;

-- 是否删除取决于旧 value，必须先读数据
DELETE FROM pms_user_dim
WHERE tenant_id = 100 AND user_id = 200 AND level < 5;

-- 范围、OR、IN、函数、子查询和全表删除均不支持
DELETE FROM pms_user_dim WHERE user_id IN (1, 2);
```

## 5. Schema 与类型契约

### 5.1 两阶段校验

Factory 创建阶段执行不访问网络的本地校验：

- 只允许物理列，不支持 metadata column；
- 必须声明非空 primary key；
- primary key 字段必须是顶层字段且为 `NOT NULL`；
- 不允许 `PARTITIONED BY`；
- Flink 类型必须能映射到 PMS 当前支持的 Paimon 类型；
- Connector 配置必须完整且无未知配置。

每个 Sink writer、Lookup function 以及 direct DELETE executor 在 `open/connect`
阶段执行 PMS handshake，并严格比较 DDL 与 Server schema snapshot：

- 物理字段数量、顺序、名称完全一致；
- 每个字段的逻辑类型参数与 nullability 一致；
- primary key 字段集合和顺序完全一致；
- row value codec、primary key codec 与协议版本受当前 Connector 支持。

Paimon `DataField.id()` 由 Server handshake 中的 Paimon `RowType` 决定。Flink DDL
没有 Paimon field id，因此 schema 比较不使用由 Flink 临时转换产生的 field id；
编码始终使用 handshake 返回的 Server `RowType`，Flink 行按已验证的 ordinal
一一适配。

唯一例外是最外层 row container：Flink physical row 固定为 `NOT NULL`，Paimon table
返回的根 `RowType` 通常为 nullable，而这个容器本身不是可写字段。因此根容器
nullability 不参与比较；所有顶层字段、nested row、array element 和 map key/value 的
nullability 仍严格一致。

字段名比较采用精确字符串匹配。若 Paimon 字段名包含大小写或特殊字符，Flink DDL
必须正确引用并得到相同的最终字段名。

任何不匹配都在开始处理数据前 fail fast。V1 不自动修改 DDL、不 reload schema，也不在
运行时静默切换 codec。

### 5.2 非主键字段类型

Connector 的完整行类型支持面是以下三者的交集：

```text
Flink <-> Paimon conversion
∩ PMS RowValueCodec
∩ 当前 PMS/Paimon table profile
```

MVP 可接受的 RowValueCodec 类型为：

- `BOOLEAN`；
- `TINYINT`、`SMALLINT`、`INT`、`BIGINT`；
- `FLOAT`、`DOUBLE`、`DECIMAL(p, s)`；
- `CHAR`、`VARCHAR` / `STRING`；
- `BINARY`、`VARBINARY` / `BYTES`；
- `DATE`、`TIME`；
- `TIMESTAMP`、`TIMESTAMP_LTZ`；
- `ARRAY`、`MAP`、`ROW`。

`MULTISET`、`RAW`、interval、`VECTOR`、`VARIANT`、`BLOB` 以及不能被上述三层共同
表达的类型在 Factory 阶段拒绝。复杂类型只承诺整字段转换，不承诺 nested projection
pushdown。

### 5.3 Primary key 类型

虽然 `PmsPrimaryKeyCodec` 自身支持更宽的编码集合，Connector MVP 的端到端 primary
key profile 还必须与当前 `pms-lookup-paimon` 对 Paimon live files 的完整点查能力取
交集。因此只承诺：

- `INT`；
- `BIGINT`；
- `DATE`；
- `VARCHAR` / `STRING`；
- `TIMESTAMP(p)`，其中 `p <= 6`，且不是 `TIMESTAMP_LTZ`；
- 上述类型构成的复合主键。

Primary key 不允许 SQL NULL。`BINARY`、`VARBINARY`、`DECIMAL`、`TIMESTAMP_LTZ`
及其他类型即使能够被某一层单独编码，也不属于当前 Connector 的完整可用 profile。

## 6. Sink 规范

### 6.1 Flink SPI 与组件

建议类结构：

```text
PmsDynamicTableFactory
PmsDynamicTableSink
PmsSink implements Sink<RowData>
PmsSinkWriter implements SinkWriter<RowData>
PmsPrimaryKeySelector
PmsFlinkRowWrapper
PmsConnectorOptions
```

`PmsDynamicTableSink#getSinkRuntimeProvider` 返回 `DataStreamSinkProvider`。选择
`DataStreamSinkProvider` 是因为 Connector 必须在真正的 Sink V2 算子前插入
primary-key shuffle，而不是只把一个裸 `SinkV2Provider` 交给 Planner。

`PmsSink` 是无状态的基础 Sink V2，不实现 `SupportsWriterState` 或
`SupportsCommitter`。

### 6.2 ChangelogMode 与 RowKind

Connector 对 latest-state KV 的归一化如下：

| Flink `RowKind` | PMS 操作 |
|-----------------|----------|
| `INSERT` | `put(key, rowValue)` |
| `UPDATE_AFTER` | `put(key, rowValue)` |
| `DELETE` | `delete(key)` |
| `UPDATE_BEFORE` | 防御性地按 `delete(key)` 处理 |

`getChangelogMode` 向 Planner 声明 `INSERT`、`UPDATE_AFTER`、`DELETE`，并表明 Sink
不需要 `UPDATE_BEFORE`。运行时仍处理意外到达的 `UPDATE_BEFORE`，使标准
`UPDATE_BEFORE -> UPDATE_AFTER` 更新流能够收敛为 delete 后 put。

Put 必须收到完整物理行。Connector 不根据旧值补列，也不支持只包含被更新列的 partial
update。若 `DynamicTableSink.Context` 表示显式 target columns 未覆盖全部物理列，
应在 planning 阶段拒绝。

### 6.3 为什么强制 primary-key shuffle

PMS 是按 key 保存 latest state 的 KV 系统。若同一个 key 同时由两个 Sink subtask
通过独立连接发送：

```text
subtask 0: value = old  ----\
                              +--> PMS 的到达顺序可能是 new, old
subtask 1: value = new  ----/
```

即使业务上 `old` 先产生，网络排队、重试或 subtask 调度也可能让它后到 PMS，最终旧值
覆盖新值。

Connector 必须在 Sink 前按完整 primary key 执行 keyed exchange：

```text
upstream RowData
  -> 提取并复制完整 primary key 为 immutable BinaryRowData
  -> Flink keyBy / deterministic key partition
  -> 同一 key 固定进入一个 PmsSinkWriter
  -> writer 按输入顺序串行发送 batch
```

这个保证的精确定义是：

- 在同一个运行中的 Flink 作业和给定并行度下，相同主键只进入一个 Sink subtask；
- 一个 writer 内只允许一个同步在途写 batch，batch 内记录顺序保持不变；
- 不同主键仍可由不同 subtask 并行写入；
- Sink 并行度变化后 key 可以映射到新的 subtask，但恢复后的新输入仍按 key 单路处理。

它不提供以下保证：

- 不会为来自不同 upstream subtask、原本就没有全局顺序的记录创造业务全序；
- 不协调两个独立 Flink 作业；
- 不 fence 尚未完全停止的旧 attempt；
- 不把 At-Least-Once 升级为 Exactly-Once。

因此更准确的部署约束不是“整张表永远只能有一个 Flink job”，而是：

```text
同一个 primary key 在同一时刻只能有一个有序的逻辑生产者。
```

多个作业只有在业务上严格保证 key domain 不相交时才是安全的。若多个作业写入重叠
key，PMS 不主动拒绝，最终结果是按实际到达顺序 last-write-wins，业务结果不确定。

这个 Connector 约束也不改变 PMS 的另一条独立约束：绑定的 Paimon 表只能由 PMS
作为唯一写入者提交，不能同时由外部 Paimon writer 绕过 PMS 写入。

### 6.4 RowData 转换与对象复用

Flink 运行时可能复用 `RowData`。`PmsSinkWriter.write` 不得把输入对象或
`PmsFlinkRowWrapper` 保存到 buffer：

1. 在 `write` 调用内用 `PmsFlinkRowWrapper` 临时适配；
2. 立即使用 handshake schema 编码出独立的 `byte[] key`；
3. 对 put 立即编码出独立的 `byte[] rowValue`；
4. 构造不可变语义的 `RawKvEntry` 后才进入 batch buffer。

这样 batch、timer flush 和 checkpoint flush 都不会读取已被 Flink 复用或修改的行对象。

### 6.5 Batching 与 flush

每个 writer 维护一个有序 `RawKvEntry` buffer，任一条件满足即同步 flush：

- 记录数达到 `sink.batch.max-rows`；
- 估算编码大小达到 `sink.batch.max-bytes`；
- 最老记录等待达到 `sink.flush.interval`；
- Flink 调用 `flush(false)` 开始 checkpoint；
- Flink 调用 `flush(true)` 结束有界输入。

processing-time timer 只负责向 task mailbox 投递一次 flush，不允许 timer 线程和
`write` 并发操作 buffer。writer 同一时刻只发送一个 batch；发送期间阻塞 Sink task
线程，通过 Flink 上游反压限制输入。

实际 batch 上限还必须取 Connector 配置与 PMS handshake 中
`maxBatchEntries` / `maxRequestBodyBytes` 的较小值。单条记录超过 Server key、row 或
request 限制时立即失败，不能无限重试或拆分一条 row。

`flush` 成功的条件是 `PmsRawClient.writeBatchDetailed` 返回 `OK`，且
`acceptedCount == batch.size()`。协议按整批接受或整批拒绝处理，不接受部分成功。

`close()` 只关闭 timer/client 并释放资源，不在取消路径偷偷提交尚未由 checkpoint 或
end-of-input flush 的数据。正常有界结束依赖 `flush(true)`，checkpoint 依赖
`flush(false)`。

### 6.6 重试、失败与反压

写入复用 `pms-client` 的有限指数退避策略：

- 只自动重试 Server 明确返回、可以确认本批尚未写入的 `OVERLOADED` 与
  `SHUTTING_DOWN`；
- 网络超时、连接中断或响应解析失败具有未知写入结果，不在当前 attempt 内直接重发；
- `BAD_REQUEST`、`SCHEMA_MISMATCH`、`INTERNAL_ERROR` 和协议错误直接失败；
- 明确可重试状态达到次数上限后失败。

同步等待和退避会阻塞 Sink task，从而自然形成 Flink backpressure。MVP 不实现早期草案
中的“判断 mailbox 容量后在线程睡眠与异步 mailbox 重试之间切换”，因为该策略增加并发
状态且不能改善无幂等协议下的未知结果语义。

任何最终写失败都抛出异常，使 checkpoint 或作业失败并由 Flink 从最近完成的 checkpoint
恢复。

### 6.7 At-Least-Once 语义

`SinkWriter.flush(false)` 在 checkpoint 完成前把当前 buffer 全部写入 PMS。由于不保存
committable，也没有 PMS 事务：

- checkpoint 成功前已写入 PMS、随后作业失败的记录会在恢复后重放；
- 重复 put/delete 对 latest-state KV 通常可收敛，但协议不承诺 exactly-once；
- 未知结果、旧 attempt 与新 attempt 的短暂重叠没有 request fencing，极端情况下仍可能
  出现跨 attempt 的到达顺序竞争；
- Connector 不把 PMS 后台 sink 到 Paimon 的 snapshot 与 Flink checkpoint 绑定。

这些是本轮已经接受的 MVP 边界。若未来需要 Exactly-Once，必须先设计 PMS
`writerId + epoch + sequence/requestId` 或 checkpoint transaction 协议，不能只增加一个
空 `Committer`。

## 7. SQL DELETE Pushdown 规范

### 7.1 `SupportsDeletePushDown` 是什么

Flink 1.20 在规划：

```sql
DELETE FROM t WHERE predicate
```

时会尝试把 `WHERE` 拆为 AND 连接的
`List<ResolvedExpression>`，并调用目标 `DynamicTableSink` 的：

```java
boolean applyDeleteFilters(List<ResolvedExpression> filters);
```

这里的返回值不是“我能处理其中一部分”，而是：

```text
true = Connector 能完整、精确地执行全部 filters
false = Connector 不能单独完成这个 DELETE
```

只有返回 `true` 时，Flink 才调用：

```java
Optional<Long> executeDeletion();
```

并让 Connector 直接执行删除。这条路径不会启动扫描目标表再产生 DELETE changelog 的
Flink 作业。

若返回 `false`，Flink 会尝试 row-level delete：扫描目标数据、应用谓词、把匹配行交给
实现了 `SupportsRowLevelDelete` 的 Sink。PMS Source 只有 lookup、没有 scan，而且 PMS
Sink 故意不实现 `SupportsRowLevelDelete`，所以该回退路径会以“不支持 row-level
delete”失败，而不是偷偷扫描 Paimon。

这就是该接口能够表达“只接受完整主键等值条件”的原因：Connector 在 planning 阶段拥有
对所有条件的否决权，只有能直接构造唯一 PMS key 时才返回 `true`。

### 7.2 接受规则

`PmsDynamicTableSink.applyDeleteFilters` 仅在以下条件全部成立时返回 `true`：

1. `filters.size()` 与 primary key 字段数相同；
2. 每个 filter 都是一个顶层物理字段与一个非 NULL 常量之间的 `EQUALS`；
3. 字段可以位于等号任意一侧；
4. 每个 primary key 字段恰好出现一次；
5. 不存在非 primary key 条件；
6. 常量可以无损转换为对应 primary key 类型；
7. 不含 OR、IN、范围、函数、subquery、dynamic parameter 或 nested field。

| WHERE 条件，PK 为 `(tenant_id, user_id)` | 是否 pushdown | 原因 |
|------------------------------------------|---------------|------|
| `tenant_id = 1 AND user_id = 2` | 是 | 完整唯一 key |
| `user_id = 2 AND tenant_id = 1` | 是 | 条件顺序不重要 |
| `1 = tenant_id AND user_id = 2` | 是 | 字段可在等号右侧 |
| `tenant_id = 1` | 否 | 缺少一部分 key |
| `tenant_id = 1 AND user_id = 2 AND level = 3` | 否 | 需要读取旧 value 判断 |
| `tenant_id = 1 AND user_id > 2` | 否 | 范围删除 |
| `tenant_id = 1 AND (user_id = 2 OR user_id = 3)` | 否 | 多 key |
| `tenant_id = 1 AND user_id = NULL` | 否 | primary key 不允许 NULL |
| 无 `WHERE` | 否 | 全表删除需要 scan/truncate |

不做“先根据主键删除，再忽略额外条件”的近似实现，因为这会删除本不满足完整谓词的行。

### 7.3 执行规则

`applyDeleteFilters` 把已验证的常量按 Server primary key 顺序保存为可序列化的删除计划；
`copy()` 必须保留该计划。`executeDeletion`：

1. 创建短生命周期 `PmsClient` 并完成 handshake/schema 校验；
2. 将 Flink literal 转为 Paimon primary-key tuple；
3. 调用 `PmsClient.delete(keyTuple)`；
4. 仅在结果为 `OK` 时成功；
5. 关闭 client。

PMS delete 总是写 tombstone，并不知道删除前是否存在 live row，因此
`executeDeletion()` 返回 `Optional.empty()`，表示 affected row count 未知。

Direct DELETE 在提交 SQL 的客户端/`TableEnvironment` 执行，不是 checkpoint 管理的
分布式 Sink 作业。它沿用 Client 对明确 `OVERLOADED/SHUTTING_DOWN` 的安全重试；网络
未知结果时语句失败并向用户报告结果未知，不额外反查 Paimon。

## 8. Lookup Join Source 规范

### 8.1 Source 能力

`PmsDynamicTableSource` 只实现 `LookupTableSource`（它本身继承
`DynamicTableSource`）。

它不实现 `ScanTableSource`。Flink 1.20 的 `LookupTableSource` 契约本身只产生
insert-only 结果，并且没有 `ScanTableSource#getChangelogMode()` 这个方法；一次
Lookup 返回的是查询时刻的一行当前快照，而不是 PMS 内部 changelog。

建议类结构：

```text
PmsDynamicTableSource
PmsLookupFunction extends LookupFunction
PmsAsyncLookupFunction extends AsyncLookupFunction
PmsLookupPlan
PmsFlinkRowData
```

### 8.2 Lookup key 验证与归一化

`LookupContext.getKeys()` 中所有 key path 必须是顶层字段。Connector 接受：

- 恰好覆盖完整 primary key 的等值 join；
- 完整 primary key 之外再带顶层字段等值条件。

不接受：

- 缺少任意 primary key 字段；
- nested field key；
- 不能映射到物理字段的 key。

Flink 传入 lookup key 的顺序不要求与 PMS primary key 顺序一致。
`PmsLookupPlan` 必须：

1. 建立 lookup key ordinal 到表字段 ordinal 的映射；
2. 把 primary key 重新排列为 Server primary key 顺序；
3. 在调度异步请求前复制 key value，不能保存可复用的输入 `RowData`；
4. 对额外等值字段保存比较器；
5. 收到 HIT 后用 Flink 对应类型的 SQL equality 语义检查额外条件。

若任一 primary key 或额外等值 key 为 NULL，SQL `=` 不可能为 TRUE，直接返回空集合，
不访问 PMS。

额外非主键等值条件不是 PMS Server filter：Connector 仍按完整主键点查至多一行，然后在
本地过滤结果。这不会变成扫描，也不会产生多行结果。

### 8.3 查询路径与结果映射

Lookup 使用 `PmsClient.get`，即完整表点查：

```text
Flink lookup key
  -> 复制并重排为 PMS primary-key tuple
  -> PmsClient.get
  -> PMS local latest-state
  -> local MISS 时由 PMS Server 穿透当前 Paimon live files
  -> Paimon InternalRow
  -> Flink RowData
```

结果映射：

| PMS 结果 | Flink Lookup 返回 |
|----------|-------------------|
| `OK + HIT` | singleton collection |
| `OK + MISS` | empty collection |
| `OK + DELETED` | empty collection |
| 非 `OK` | 重试或异常，不得当作 MISS |

`DELETED` 必须与 `MISS` 分别计数。虽然二者对 Join 都返回空集合，但 tombstone 不能被
错误地解释为“继续绕过 PMS 到其他数据源查旧值”；穿透决策已经由 PMS Server 完成。

返回的 `FlinkRowData` 只能在当前调用结果生命周期内引用解码出的独立
`InternalRow`，不能复用一个随后会被下一次 Lookup `replace` 的共享 wrapper。

### 8.4 同步 Lookup

当 `lookup.async = false` 时返回 `LookupFunctionProvider`：

- 每个 Lookup operator subtask 在 `open` 时创建一个 `PmsClient`；
- `lookup` 在 operator 调用线程同步执行；
- 请求耗时直接阻塞该 operator，并通过 Flink 反压限制上游；
- `close` 关闭 client。

PMS 在原理和接口上没有阻止同步 Lookup。异步是默认性能策略，不是正确性要求。

### 8.5 异步 Lookup

当 `lookup.async = true` 时返回 `AsyncLookupFunctionProvider`。当前 `pms-client`
是同步 API，因此 MVP 使用固定大小 executor 包装同步点查：

- 在 `asyncLookup` 的调用线程复制完整 lookup key，再把副本提交给 worker；
- executor 线程数由 `lookup.async.thread-number` 控制；
- 不跨线程保留 Flink 可复用 `RowData`；
- `open` 创建固定数量的单线程 worker slot，每个 slot 独占一个 `PmsClient`，不假定当前
  Client facade 可被并发调用；
- 所有 worker client 在 `open` 返回前完成 handshake/schema 校验，不能等第一批记录到达
  后才暴露 schema 错误；
- `close` 停止接收任务、取消/等待在途任务，并关闭全部 worker clients；
- task cancellation 和线程中断必须能够结束退避与请求等待。

Flink async lookup operator 的请求 timeout 与 buffer capacity 继续使用 Flink 1.20
自身的 table execution 配置/lookup hint；Connector 的线程数只控制客户端实际并发，
不重复定义另一套 capacity。

`client.read-timeout = 0` 会让单次 PMS 请求不设置 Client timeout。async 模式允许该
配置，但不推荐使用；此时卡住请求只能依赖 Flink async timeout 触发 Task 失败，并由
Task cancellation 中断 worker。生产环境应保留有限 read timeout，并确保它与重试总
耗时显著小于 Flink async lookup timeout。

未来若 `pms-client` 提供真正的 non-blocking async API，可替换 executor wrapper，但不得
改变 Lookup Source 的 SQL 语义。

### 8.6 Lookup 重试

Lookup 是只读操作，可以安全重试。最多重试 `lookup.max-retries` 次，指数退避参数使用
`lookup.retry.initial-backoff` 与 `lookup.retry.max-backoff`。

可重试：

- `LOOKUP_UNAVAILABLE`；
- `OVERLOADED`、`SHUTTING_DOWN`；
- read timeout、连接中断等 transport failure。

不可重试：

- `BAD_REQUEST`；
- `SCHEMA_MISMATCH`；
- 协议解码错误；
- 不支持的 schema/profile；
- `INTERNAL_ERROR`，MVP 直接暴露以避免持续放大服务端未知错误。

重试耗尽后 future/同步调用以异常结束，让 Flink 根据作业策略处理。任何查询错误都不能
降级为 `MISS`。

### 8.7 当前态与一致性

Lookup 是 processing-time 当前态查询：

- 同一条事实流记录重试时可能读到不同版本的 dimension row；
- Flink checkpoint 不保存 PMS 查询版本；
- Sink put/delete 一旦在 PMS 当前层可见，后续 Lookup 可以读到；
- PMS 后台 Paimon commit/publish 的内部一致性仍由 PMS Server 保证；
- Connector 不提供 read-your-checkpoint、event-time 或 snapshot isolation。

### 8.8 不启用 Connector 本地缓存

MVP 不实现 Flink Connector 侧 partial/full cache，原因是：

- PMS 自身已经有 MemTable/SST 与 Paimon lookup cache；
- Connector cache 会额外引入跨 TaskManager 的失效与 freshness 问题；
- 对 latest-state dimension lookup，错误的 stale HIT 或 stale MISS 都比一次 RPC 更危险；
- Flink full cache 需要 Scan Source，而 PMS 明确没有全表 scan。

因此不接受 `lookup.cache` 及 partial/full cache 相关配置。后续若要增加，只能作为显式
opt-in 能力，并先定义 TTL、一致性、DELETE/MISS 缓存与失效契约。

## 9. Connector 配置

### 9.1 通用和 Client 配置

| 配置项 | 必填/默认值 | 说明 |
|--------|-------------|------|
| `connector` | 必须为 `pms` | Factory identifier |
| `endpoint` | 必填 | PMS Server 根 URI，`http` 或 `https`；不允许 user-info、query、fragment 或额外 path |
| `client.connect-timeout` | `5 s` | handshake/连接超时，必须大于 0 |
| `client.write-timeout` | `5 s` | 单次写请求 timeout；`0` 表示不设置 |
| `client.read-timeout` | `3 s` | 单次 Lookup timeout；`0` 表示不设置，async 模式不推荐使用 |
| `client.require-http2` | `true` | 是否拒绝非 HTTP/2 响应 |

### 9.2 Sink 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `sink.parallelism` | 未设置 | 未设置时沿用 Flink 推导的 Sink 并行度 |
| `sink.batch.max-rows` | `256` | 单 batch 最大记录数，必须大于 0 |
| `sink.batch.max-bytes` | `4 mb` | 单 batch 估算最大编码字节数，必须大于 0 |
| `sink.flush.interval` | `50 ms` | 非空 buffer 最大等待时间；`0` 表示每条及时 flush |
| `sink.write-retry.max-retries` | `10` | 明确拒绝状态的最大重试次数 |
| `sink.write-retry.initial-backoff` | `10 ms` | 初始退避 |
| `sink.write-retry.max-backoff` | `640 ms` | 最大退避 |

Primary-key shuffle 固定启用，不提供 `sink.key-shuffle=false`。上述 batching 默认值是
MVP 初值，发布前应由 Connector + PMS 端到端 benchmark 校准；调整默认值不得改变
checkpoint flush 和顺序契约。

### 9.3 Lookup 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `lookup.async` | `true` | 是否使用 Async Lookup provider |
| `lookup.async.thread-number` | `4` | 每个 async Lookup subtask 的 worker 数 |
| `lookup.max-retries` | `3` | 只读请求最大重试次数，沿用 Flink 1.20 标准 key |
| `lookup.retry.initial-backoff` | `10 ms` | 查询重试初始退避 |
| `lookup.retry.max-backoff` | `640 ms` | 查询重试最大退避 |

所有数值、duration、memory size 选项在 Factory 阶段完成范围和组合校验。未知配置直接
报错，避免拼写错误被静默忽略。

## 10. 生命周期与故障矩阵

| 场景 | Sink | Lookup | Direct DELETE |
|------|------|--------|---------------|
| Factory 参数非法 | planning fail | planning fail | planning fail |
| handshake/schema 不匹配 | writer open fail | function/worker open fail | statement fail |
| `OVERLOADED` / `SHUTTING_DOWN` | 有限安全重试，耗尽后 fail job | 有限重试，耗尽后 fail lookup | Client 有限安全重试 |
| `LOOKUP_UNAVAILABLE` | 不适用 | 有限重试，不能转 MISS | 不适用 |
| transport timeout | 结果未知，fail job，不立即重写 | 只读，有限重试 | 结果未知，statement fail |
| `BAD_REQUEST` / `SCHEMA_MISMATCH` | fail fast | fail fast | fail fast |
| `INTERNAL_ERROR` / 协议错误 | fail fast | fail fast | fail fast |
| checkpoint flush 失败 | checkpoint/job fail | 不适用 | 不适用 |
| task cancellation | 停止 timer，关闭 client，不额外 flush | 取消 future/executor，关闭 clients | 不适用 |
| Lookup key 含 NULL | 不适用 | 返回空集合，不发 RPC | 常量 NULL 不接受 pushdown |

## 11. 指标与日志

所有指标挂在 Connector operator 的 `pms` metric subgroup 下。MVP 至少提供：

### 11.1 Sink

| 指标 | 类型 | 含义 |
|------|------|------|
| `records` | Counter | 收到的 changelog 记录数 |
| `puts` | Counter | 编码为 put 的记录数 |
| `deletes` | Counter | 编码为 delete 的记录数 |
| `batches` | Counter | 成功发送的 batch 数 |
| `batchBytes` | Counter | 成功发送的总编码字节数 |
| `flushes` | Counter | threshold/timer/checkpoint/end-input flush 次数 |
| `writeRejected` | Counter | 重试耗尽后最终返回 `OVERLOADED/SHUTTING_DOWN` 的 batch 数 |
| `writeFailures` | Counter | 最终失败 batch 数 |
| `bufferedRecords` | Gauge | 当前 buffer 记录数 |
| `bufferedBytes` | Gauge | 当前估算 buffer bytes |
| `writeLatency` | Histogram | batch 写请求总耗时 |

### 11.2 Lookup

| 指标 | 类型 | 含义 |
|------|------|------|
| `lookupRequests` | Counter | 实际发往 PMS 的查询次数，不含 NULL key short-circuit |
| `lookupHits` | Counter | HIT |
| `lookupMisses` | Counter | MISS |
| `lookupDeleted` | Counter | DELETED |
| `lookupRetries` | Counter | 查询重试次数 |
| `lookupFailures` | Counter | 最终异常次数 |
| `lookupNullKeys` | Counter | NULL key short-circuit 次数 |
| `lookupInFlight` | Gauge | async 在途数；同步模式为 0/1 |
| `lookupLatency` | Histogram | 含重试的端到端查询耗时 |

当前 `pms-client` 不暴露内部 write retry attempt 数，因此 MVP 只记录最终
`writeRejected`，不通过解析日志伪造 `writeRetries`。若未来 Client 增加稳定的
observer/attempt metadata，再补充逐次重试指标。

Connector 不输出逐条成功日志。最终失败以包含 endpoint、operation、status 和原始
cause 的异常交给 Flink；Flink task failure log 负责附加 job/subtask/attempt 上下文。
Schema 不匹配异常包含精确 field path 与两侧类型。任何异常都不得包含 key/value 内容。

## 12. 测试规范

本节同时是后续回归测试的长期清单。MVP 当前自动化覆盖见第 12.6 节。

### 12.1 Unit

- Factory identifier、service discovery、required/optional options、未知 option。
- DDL/server schema 的字段顺序、名称、类型参数、nullability、PK 顺序校验。
- 所有支持类型的 Flink `RowData` 与 Paimon `InternalRow` 往返。
- unsupported row/PK type、NULL PK、复杂类型边界。
- `RowKind` 到 put/delete 的归一化。
- `RowData` object reuse 下 buffer 内容保持不变。
- primary-key selector：同 key 稳定相等，不同 key 可分散，复合 key 顺序正确。
- batch rows/bytes/timer/checkpoint/end-input 边界。
- 明确拒绝状态、未知 transport outcome、accepted count 不匹配。
- `copy()` 保留 source/sink 配置与 DELETE 计划。

### 12.2 DELETE Planner

- 单列和复合完整 PK 等值接受，字段/常量左右互换和条件换序接受。
- partial PK、额外非 PK、OR/IN/range/function/subquery/NULL/全表删除拒绝。
- 接受的语句生成 direct delete operation，不生成 Scan。
- 拒绝的语句因未实现 `SupportsRowLevelDelete` 明确 planning fail。
- `executeDeletion` 只调用一次 PMS delete，不调用 get/Paimon read，返回 unknown row count。

### 12.3 Lookup

- 完整 PK、复合 PK 重排、PK 加额外等值字段。
- partial PK 与 nested key planning fail。
- NULL key 不发 RPC。
- HIT/MISS/DELETED/LOOKUP_UNAVAILABLE 分离。
- extra equality 本地过滤正确。
- 同步与异步结果一致。
- async 在调度前复制 key，开启 Flink object reuse 后仍正确。
- retry 分类、次数、退避、中断和 close。
- 验证 Source 无 Scan 能力，普通全表查询失败。

### 12.4 Sink/ordering

- Planner 生成 Sink 前 primary-key exchange。
- 多 upstream/subtask 时同 key 只进入一个 writer。
- 一个 writer 内 batch 与请求顺序不反转。
- checkpoint 前 buffer 全部 flush。
- checkpoint 后故障重放符合 At-Least-Once。
- 两个独立 producer 写同 key 的测试明确展示 arrival-order last-write-wins，不误报为有序。

### 12.5 End-to-end

使用 Flink 1.20.3 MiniCluster、真实 `pms-server` 和本地 Paimon 表覆盖：

1. SQL DDL + INSERT/UPDATE_AFTER/DELETE 写入后 PMS 当前态正确；
2. PMS sink 到 Paimon 后结果正确，delete 不反查旧 Paimon row；
3. processing-time INNER/LEFT Lookup Join 的 HIT/MISS/DELETED；
4. sync/async Lookup；
5. exact-PK SQL DELETE；
6. checkpoint、TaskManager/Server 故障与恢复；
7. server overload/shutdown/lookup unavailable；
8. Connector shaded JAR 在 SQL Client 中可发现，且与另一版本 Paimon Connector
   同时存在时无 class conflict。

### 12.6 当前 MVP 验证覆盖

当前实现的前五项由 Maven 测试自动执行。最终 shaded JAR smoke 在 `package` 后手工
执行，待项目接入 CI 时再迁移到独立的集成测试 pipeline：

| 测试 | 覆盖范围 |
|------|----------|
| `PmsFlinkRowAdapterTest` / `PmsFlinkTypeAdapterTest` | 所有已声明 row value 类型族往返、schema/PK 校验 |
| `PmsSinkIntegrationTest` | 真实 PMS codec 上的 upsert/delete、object reuse、checkpoint flush |
| `PmsLookupIntegrationTest` / `PmsLookupPlanTest` | sync/async、HIT/MISS/DELETED、NULL short-circuit、重试、key copy/重排 |
| `PmsDeletePlanTest` / `PmsSqlIntegrationTest` | 完整 PK filter 解析、Planner direct DELETE、拒绝扫描型 DELETE、零反查 |
| `PmsRealServerEndToEndTest` | Flink 1.20 MiniCluster SQL Sink、异步 temporal Lookup Join、真实 PMS/Paimon flush/sink、SQL DELETE |
| shaded JAR 手工 smoke | 无 Flink class、无未重定位 Paimon class、Factory 从最终 JAR 被 `ServiceLoader` 发现 |

进程级 TaskManager/Server kill、checkpoint 后重放和与另一个版本 Paimon Connector
共同加载属于第 12.4/12.5 节保留的发布加固矩阵。它们不改变当前 At-Least-Once、
故障分类或 classloader 契约，后续应在独立 Flink distribution 测试环境中持续补齐。

## 13. 实施状态与后续加固

MVP 已按以下阶段完成：

1. Maven module、Flink 1.20.3 依赖、Factory、options、service 和 shaded JAR；
2. 自有 Flink/Paimon 薄 adapter 与两阶段 schema/type 校验；
3. Sink V2、强制 primary-key shuffle、顺序 batching 和 checkpoint flush；
4. sync Lookup 与固定 worker/client ownership 的 async Lookup；
5. 完整 PK `SupportsDeletePushDown`；
6. Unit、Planner、MiniCluster 和真实 PMS/Paimon E2E。

后续工作只包括第 12.6 节列出的发布加固，以及基于真实业务负载校准 batch、flush、
async thread 默认值；在有测量结果前不猜测新的默认值。

## 14. 参考实现的采用边界

本设计参考 Fluss 与 Paimon，但只复用与 PMS 语义一致的部分：

- 参考 Fluss 使用 `DataStreamSinkProvider` 插入 pre-write keyed shuffle；
- 参考 Fluss 对 `SupportsDeletePushDown` 只接受完整 PK equality，并返回 unknown
  affected row count；
- 参考 Paimon 的 row adapter 行为，但因 Flink 1.20 classpath 兼容性采用第 3.2 节的
  PMS 自有最小实现；
- 参考 Paimon 用固定 executor 将同步 Lookup 包装为异步 provider 的结构。

以下行为不复制：

- 不复制 Paimon SQL DeleteAction 的 `SELECT * WHERE ... -> DELETE rows` 扫描路径；
- 不复制 Paimon/Fluss 与自身存储协议绑定的 Exactly-Once、bucket writer、Catalog 或
  row-level update/delete 逻辑；
- 不因为 Paimon 类型转换器支持某类型，就越过 PMS row codec、primary-key codec 或
  lookup profile 的实际限制。

PMS sink 到 Paimon 时遇到 tombstone，由 `pms-sink-paimon` 的 `DeleteRowFactory` 根据
key 构造 delete row；为了绕过 Paimon 1.4.1 对非主键 `NOT NULL` 字段的提前校验，它会为
这些字段填确定性合成值。该流程不查询旧 Paimon row，非主键合成值没有业务含义。Flink
Connector 的 streaming DELETE 与 direct SQL DELETE 都只需要发送 PMS key，不允许为构造
delete 再增加一次 Lookup。
