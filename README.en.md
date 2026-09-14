[简体中文](README.md) | [English](README.en.md)

# Paimon MemTable Service

**A single-node KV service for real-time writes and current-state point lookups on Paimon tables.**

PMS acts like a MemTable layer in front of a Paimon table. As the table's sole writer, it accepts
the latest changes and asynchronously persists them to Paimon. Reads check the latest local
state before accessing historical data in Paimon. The name describes PMS's role in the overall
storage architecture.

Internally, PMS uses a **lightweight LSM KV engine with a WAL, MemTables, local SSTs, and
compaction**, providing local storage and process crash recovery. The local write layer retains
a window of the latest state, while a separate query cache stores historical Paimon files on
demand. Together, they serve current-state primary-key lookups. Compute engines can continue
to query the Paimon table for analytics.

The current version is `0.1-SNAPSHOT`. It provides a working MVP within the scope below and is
available to build and try from source.

## MVP scope

| Area | Current support |
| --- | --- |
| Deployment | Single node, bound to one existing Paimon table; PMS must be its sole writer |
| Table | Primary key + `deduplicate` + `HASH_FIXED` buckets + Parquet; partition columns must be included in the primary key |
| Schema | Must remain unchanged throughout the lifetime of the PMS local state; enforced by deployment practices |
| Primary key | Non-null `INT`, `BIGINT`, `DATE`, `STRING`, non-LTZ `TIMESTAMP(P <= 6)`, and supported combinations; see the [lookup profile](docs/pms-lookup-paimon.md#7-首期-profile) |
| Writes and recovery | Put/delete/batch, WAL, MemTables, local SSTs, asynchronous sink, SinkMeta recovery, and flow control |
| Queries | Current-state primary-key lookups, local prefix queries, direct Paimon lookups, and a local file cache; tombstones prevent reads from falling through to older data |
| Clients and integration | HTTP/2 h2c, Java client; Flink 1.20.3 At-Least-Once Sink, synchronous/asynchronous processing-time Lookup Join, and SQL DELETE with equality predicates covering the full primary key |
| Deliverables | Maven multi-module source, Flink connector JAR, and a Linux distribution with startup/shutdown scripts |

A successful ordinary write means the WAL append has completed and the data is visible in the
MemTable. **It does not perform an fsync for every write or mean the data has been committed to
Paimon.** Current validation covers process crash recovery; PMS does not guarantee that every
acknowledged write survives a machine power loss. To confirm visibility in Paimon, establish a
sink fence through the administrative API and wait for the persisted sequence boundary to reach it.

V1 does not provide distributed or multi-node service, concurrent external writers, schema
evolution, MVCC or historical snapshot reads, a full-table scan source, or Exactly-Once semantics.
Dedicated scheduling for explicit Paimon compaction is deferred. Ordinary sink commits retain
Paimon's implicit compaction and snapshot cleanup according to table configuration. See the
[codec documentation](docs/pms-codec.md) for supported row value types.

## Build and run

The build baseline is Java 17 and Maven 3.8.6, with Paimon 1.4.1 and Flink 1.20.3 pinned as
dependencies. The Maven `groupId` is `io.github.esnhysythh`; see each POM for its artifactId.

```bash
# Run regular unit/local integration tests and verify packaging; no remote HDFS access
mvn verify

# Build the Linux distribution
mvn -pl pms-dist -am package
```

The distribution is generated at `pms-dist/target/pms-0.1-SNAPSHOT/` and as a `.tar.gz` archive
with the same base name. Before starting, edit `conf/pms-server.properties` to configure the
existing table and durable WAL/storage paths, then run `bin/pms-server.sh`. See the
[distribution documentation](docs/pms-dist.md) for configuration and background process management.

For a local trial, start the development entry point, which creates a test table automatically,
and use the HTTP examples in the [server README](pms-server/README.md):

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.dev.PMSTestServerMain
```

## Validation scope

- Regular Maven tests cover components, HTTP/2 reads/writes and recovery, and end-to-end Flink
  MiniCluster integration using the local filesystem.
- The first real HDFS acceptance suite covered an independent PMS process, reads/writes/deletes,
  resuming with fresh local state, and kill/restart recovery, including three consecutive successful
  rounds. The environment used HDFS 3.4.3 with one NameNode, one DataNode, simple authentication,
  and a replication factor of one. PMS used the Hadoop 2.8.5 client. See the
  [acceptance record](docs/tests/pms-remote-hdfs-acceptance-2026-09-11.md).
- Remote acceptance used an unpartitioned table with one bucket and a BIGINT primary key.
  Multiple buckets and partitioned tables with composite primary keys already have local coverage;
  remote coverage extensions are described in the [follow-up test plan](docs/tests/pms-next-batch.md).
- S3, HDFS HA/Kerberos, long-running stress tests, and deployment of the distribution on a target
  Linux environment have not completed acceptance testing. They are outside the currently validated
  environment scope; existing results do not establish production certification or a performance SLA.

Local performance tools cover three scenarios: **core writes/queries, direct Paimon queries, and
cached Paimon queries**. See the [benchmark documentation](docs/pms-benchmark.md) for commands
and measurement semantics. The source repository contains reusable tools and documentation;
run reports remain local or are stored as CI artifacts.

## Documentation

The detailed design and module documents below are currently written in Chinese.

- [Architecture and module index](design.md)
- [Server configuration, scheduling, and recovery](docs/pms-server.md)
- [Java client](docs/pms-client.md) / [Flink connector](docs/flink-connector-pms.md)
- [Test strategy](docs/tests/pms-testing-strategy.md)
