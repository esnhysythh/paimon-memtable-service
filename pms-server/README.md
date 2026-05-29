# pms-server

`pms-server` is currently a thin, single-node runtime wrapper for development and integration testing.
It exposes a small HTTP control surface while the internal PMS flow is still being built out.

Start with a properties file:

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.PmsServerMain \
  -Dexec.args=/path/to/pms-server.properties
```

The bundled example config is at `src/main/resources/pms-server-example.properties`.

For development, start the self-contained test server. It creates a default Paimon primary-key
table and writes all state under `pms-server/target/pms-test-server/<timestamp>/`.

```bash
mvn -pl pms-server -am process-classes exec:java \
  -Dexec.mainClass=org.qwh.pms.server.dev.PMSTestServerMain
```

Manual HTTP endpoints:

```bash
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/write -d '{"id":1,"marker":"a"}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/get -d '{"id":1}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/flush -d '{}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/sink -d '{}'
curl http://127.0.0.1:19090/state
```

## Configuration

`pms-server` reads Java properties. Required values are the local WAL/storage
directories and the target Paimon table.

| Key | Default | Notes |
| --- | --- | --- |
| `pms.server.host` | `127.0.0.1` | HTTP bind host. |
| `pms.server.port` | `9090` | HTTP bind port. Use `0` in tests for a random port. |
| `pms.server.commit_user` | `pms-server` | Paimon commit user. |
| `pms.server.scheduler.enabled` | `false` | Enables the lightweight background scheduler. |
| `pms.server.scheduler.flush_interval_ms` | `0` | Scheduled freeze+flush interval. `0` disables scheduled flush. |
| `pms.server.scheduler.sink_interval_ms` | `pms.sink.interval_ms` | Scheduled sink interval. `0` disables scheduled sink. |
| `pms.wal.dir` | required | WAL directory. Must differ from `pms.storage.dir`. |
| `pms.wal.file_size_mb` | `256` | WAL segment size. |
| `pms.wal.use_mmap` | `false` | Whether WAL uses mmap writer. |
| `pms.storage.dir` | required | Local SST/state directory. |
| `pms.storage.sinked_max_size_mb` | `10240` | Local SST retention size threshold; only sinked SSTs can be evicted. |
| `pms.storage.sinked_max_count` | `100` | Local SST retention file-count threshold; only sinked SSTs can be evicted. |
| `pms.storage.local_sst_max_rows` | `0` | Local SST physical entry-count threshold. `0` disables row-based retention. |
| `pms.storage.compact_threshold_mb` | `32` | Local compaction threshold. |
| `pms.storage.compact_min_files` | `4` | Minimum files for local compaction. |
| `pms.paimon.warehouse` | required | Paimon warehouse path. |
| `pms.paimon.database` | required | Paimon database. |
| `pms.paimon.table` | required | Paimon table. |
| `pms.memtable.max_entries` | `1000000` | Current memtable entry threshold. |
| `pms.memtable.max_size_mb` | `256` | Current memtable size threshold. |
| `pms.sink.interval_ms` | `30000` | Core sink interval default and scheduler sink fallback. |
| `pms.sink.max_pending_ssts` | `8` | Pending SST threshold. |
| `pms.flowcontrol.overloaded_immutable_count` | `4` | Flow-control threshold. |
| `pms.flowcontrol.overloaded_pending_sst_count` | `16` | Flow-control threshold. |
