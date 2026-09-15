[简体中文](README.md) | [English](README.en.md)

# Paimon MemTable Service

Paimon MemTable Service（PMS）是一个位于 Apache Paimon Primary Key Table 前面的轻量级实时 KV 服务。

**MemTable** 在这里是个比喻。在 LSM 存储中，新写入的数据通常先进入可变的 MemTable，随后再逐步落入不可变的 SST 文件。PMS 在一张 Paimon 表前扮演了类似的角色：新数据先写入 PMS 并立即可查，随后异步沉降到 Paimon；PMS 主要保存比 Paimon 更新的一段 recent state，而 Paimon 负责长期持久化的数据。

```text
        PUT / DELETE / GET
                │
                ▼
        ┌───────────────┐
        │      PMS      │
        │ WAL / MemTable│
        │   Local SST   │
        └───────┬───────┘
                │
              sink
                ▼
        ┌───────────────┐
        │    Paimon     │
        │   PK / LSM    │
        │ Parquet files │
        └───────────────┘
```

PMS 本地维护一个 recent state window。数据进入 Paimon 后，本地状态可以逐渐淘汰；查询则优先读取 PMS 中更新的数据，再回落到已经提交的 Paimon 数据。

从 LSM 的视角看，可以把 PMS 理解为 **Paimon LSM 上方的一层 mutable frontier**。

## Why PMS?

一种常见的实时数据湖架构是同时维护：

```text
Kafka / Flink
   ├──> KV / OLTP database
   └──> Paimon
```

这样可以获得很好的实时点查性能，但也意味着同一份逻辑数据需要长期维护两套完整存储，并处理两条写入链路之间的一致性。

PMS 想尝试另一种方式：只维护最近的状态窗口，并尽量直接复用 Paimon 已有的数据结构。

对于已经存在于 Paimon 中的 Parquet 文件，PMS 利用 Primary Key 已排序这一性质进行直接点查，而不是预先把整张表重新构建成一份 KV store。如果某些文件持续被访问，则按需构建本地 lookup SST cache：

```text
recent data   -> PMS MemTable / local SST
cold history  -> direct Parquet lookup
hot history   -> lookup SST cache
```

PMS 主要想探索一个问题：

> **If Paimon is already an LSM, how little additional storage is needed to turn it into a real-time current-state service?**

## What can PMS do?

当前已经支持：

- `PUT` / `DELETE` / batch write
- WAL、MemTable、本地 SST 和 compaction
- 后台异步 sink 到 Paimon
- 进程崩溃后的本地状态恢复
- Primary Key current-state lookup
- Paimon Parquet direct lookup
- 基于访问热度的本地 lookup SST cache
- HTTP/2 server 和 Java client
- Flink At-Least-Once Sink
- Flink synchronous / asynchronous processing-time Lookup Join
- 完整主键等值条件下的 SQL `DELETE`

例如：

```text
Paimon: K1 -> value-v1
PMS:    K1 -> value-v2
```

查询得到 `value-v2`。

如果 PMS 中保存：

```text
PMS: K1 -> DELETE
```

则不会继续读取 Paimon 中更旧的 `value-v1`。

## Paimon lookup

历史数据查询由 `pms-lookup-paimon` 提供。

它维护当前 Paimon snapshot 中各 partition / bucket 的 live data-file view，并按照 Paimon merge-tree 的文件优先级查找候选文件。

对于 Parquet 文件，默认首先使用 direct lookup：

```text
Primary Key
    │
    ▼
candidate file
    │
    ▼
locate row position
    │
    ▼
read row
```

对于持续访问的文件，PMS 会异步构建本地 Value SST cache。cache 只是性能优化；没有 cache 时仍然可以直接查询原始 Paimon Parquet 文件。

详细设计见 [docs/pms-lookup-paimon.md](docs/pms-lookup-paimon.md)。

## Build

要求：

- Java 17
- Maven 3.8.6+
- Apache Paimon 1.4.1
- Apache Flink 1.20.3

运行常规测试与打包验证：

```bash
mvn verify
```

构建 Linux 发行包：

```bash
mvn -pl pms-dist -am package
```

生成：

```text
pms-dist/target/pms-0.1-SNAPSHOT/
pms-dist/target/pms-0.1-SNAPSHOT.tar.gz
```

## Quick start

启动会自动创建测试表的开发 Server：

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.dev.PMSTestServerMain
```

正式运行时先构建发行包：

```bash
mvn -pl pms-dist -am package
```

编辑：

```text
conf/pms-server.properties
```

配置已有 Paimon 表、WAL 和 local storage 路径，然后启动：

```bash
bin/pms-server.sh
```

Server 配置和 HTTP 示例见 [pms-server/README.md](pms-server/README.md)，发行包说明见 [docs/pms-dist.md](docs/pms-dist.md)。

## Modules

```text
pms-core             local WAL / MemTable / SST / recovery
pms-codec            Paimon key/value codec
pms-protocol         network protocol
pms-client           Java client
pms-sink-paimon      Paimon sink
pms-lookup-paimon    Paimon point lookup and lookup SST cache
pms-server           standalone server
flink-connector-pms  Flink integration
pms-dist             Linux distribution
pms-tests            integration tests and benchmarks
```

更完整的架构与模块说明见 [design.md](design.md)。

## Current limitations

当前版本主要面向：

- single node
- single writer
- fixed schema
- Paimon Primary Key Table
- `deduplicate` merge engine
- `HASH_FIXED` bucket
- Parquet data files
- 分区列包含在 Primary Key 中

当前不提供 multi-node PMS、外部并发 writer、Schema evolution、MVCC / historical snapshot query、Scan Source 或 Exactly-Once Sink。

普通写入成功表示数据已经写入 PMS WAL 并对 PMS 查询可见；它不表示数据已经提交到 Paimon，普通写入也不会逐条执行 `fsync`。

更详细的支持类型、恢复语义和运行边界见 [design.md](design.md) 和 `docs/`。

## Project status

Current version: `0.1-SNAPSHOT`

## License

MIT