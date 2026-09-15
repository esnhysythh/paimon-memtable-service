[简体中文](README.md) | [English](README.en.md)

# Paimon MemTable Service

Paimon MemTable Service (PMS) is a lightweight real-time KV service that sits in front of an Apache Paimon Primary Key Table.

The name **MemTable** is a metaphor. In an LSM store, new writes usually enter a mutable MemTable first and are later flushed into immutable SST files. PMS plays a similar role for a Paimon table: new data is written to PMS and becomes queryable immediately, then sinks asynchronously into Paimon. PMS mainly keeps a recent state window that is newer than Paimon, while Paimon remains the long-term persistent store.

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

PMS keeps a recent state window locally. Once data has been persisted to Paimon, the corresponding local state can gradually be evicted. Reads check the newer PMS state first and fall back to committed Paimon data when needed.

From an LSM point of view, PMS can be thought of as **a mutable frontier above Paimon's LSM**.

## Why PMS?

A common real-time lakehouse architecture maintains two complete stores:

```text
Kafka / Flink
   ├──> KV / OLTP database
   └──> Paimon
```

This provides good point-lookup latency, but it also means maintaining two full storage representations of the same logical data and dealing with consistency across two write paths.

PMS explores a different approach: keep only a recent state window locally and reuse Paimon's existing storage structure as much as possible.

For Parquet files already committed to Paimon, PMS can perform direct point lookups by taking advantage of the Primary Key ordering instead of eagerly rebuilding the whole table into another KV store. If a file becomes hot, PMS materializes a local lookup SST cache on demand:

```text
recent data   -> PMS MemTable / local SST
cold history  -> direct Parquet lookup
hot history   -> lookup SST cache
```

PMS is built around one question:

> **If Paimon is already an LSM, how little additional storage is needed to turn it into a real-time current-state service?**

## What can PMS do?

The current version supports:

- `PUT` / `DELETE` / batch writes
- WAL, MemTable, local SSTs, and compaction
- asynchronous sinking into Paimon
- local-state recovery after process crashes
- Primary Key current-state lookup
- direct point lookup on Paimon Parquet files
- access-frequency-based local lookup SST cache
- HTTP/2 server and Java client
- Flink At-Least-Once Sink
- Flink synchronous / asynchronous processing-time Lookup Join
- SQL `DELETE` with full Primary Key equality predicates

For example:

```text
Paimon: K1 -> value-v1
PMS:    K1 -> value-v2
```

A lookup returns `value-v2`.

If PMS contains a tombstone:

```text
PMS: K1 -> DELETE
```

the lookup returns not found instead of falling through to the older `value-v1` in Paimon.

## Paimon lookup

Historical point lookups are implemented by `pms-lookup-paimon`.

It maintains a live view of the data files for each partition / bucket in the current Paimon snapshot and searches candidate files according to Paimon's merge-tree file priority.

For Parquet files, PMS first uses direct lookup:

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

For files that are accessed repeatedly, PMS asynchronously builds a local Value SST cache. The cache is only a performance optimization: without it, PMS can still query the original Paimon Parquet file directly.

See [docs/pms-lookup-paimon.md](docs/pms-lookup-paimon.md) for the detailed design.

## Build

Requirements:

- Java 17
- Maven 3.8.6+
- Apache Paimon 1.4.1
- Apache Flink 1.20.3

Run the regular test and packaging checks:

```bash
mvn verify
```

Build the Linux distribution:

```bash
mvn -pl pms-dist -am package
```

Artifacts are generated at:

```text
pms-dist/target/pms-0.1-SNAPSHOT/
pms-dist/target/pms-0.1-SNAPSHOT.tar.gz
```

## Quick start

Start the development server, which creates a test table automatically:

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.dev.PMSTestServerMain
```

For a normal deployment, build the distribution first:

```bash
mvn -pl pms-dist -am package
```

Edit:

```text
conf/pms-server.properties
```

Configure an existing Paimon table together with the WAL and local storage paths, then start PMS:

```bash
bin/pms-server.sh
```

See [pms-server/README.md](pms-server/README.md) for server configuration and HTTP examples, and [docs/pms-dist.md](docs/pms-dist.md) for distribution details.

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

See [design.md](design.md) for the overall architecture and module layout.

## Current limitations

The current version is designed around:

- a single node
- a single writer
- a fixed schema
- a Paimon Primary Key Table
- the `deduplicate` merge engine
- `HASH_FIXED` buckets
- Parquet data files
- partition columns included in the Primary Key

PMS currently does not provide multi-node operation, external concurrent writers, schema evolution, MVCC / historical snapshot queries, a Scan Source, or an Exactly-Once Sink.

A successful normal write means the data has been appended to the PMS WAL and is visible to PMS reads. It does not mean the data has already been committed to Paimon, and normal writes do not perform an `fsync` for every request.

See [design.md](design.md) and `docs/` for supported types, recovery semantics, and detailed runtime boundaries.

## Project status

Current version: `0.1-SNAPSHOT`

## License

MIT