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

For a Linux binary distribution, build the `pms-dist` module:

```bash
mvn -pl pms-dist -am package
```

See [pms-dist](../docs/pms-dist.md) for the archive layout and runtime scripts.

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
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/getLocal -d '{"id":1}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/prefixLocal -d '{"id":1}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/flush -d '{}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/sink -d '{}'
curl http://127.0.0.1:19090/state
```

`/flush` and `/sink` are asynchronous management requests. They return HTTP 202 with a
`fenceSequenceId`; poll `/state` until `lastFlushedSequenceId` or `lastPersistedSequenceId`
respectively reaches that fence. They do not synchronously drain all data.

`/get` is the default full point lookup and may fall through to Paimon after a PMS-local miss.
`/getLocal` only reads PMS-local layers and returns `result=HIT|DELETED|MISS`.
`/prefixLocal` only reads PMS-local layers. `/prefix` is reserved for future full prefix lookup and returns `NOT_SUPPORTED` in V1.

The same port also exposes the binary protocol under `/pms/api/v1/...`.
Binary endpoints require HTTP/2 by default. For h2c clients, call `/pms/api/v1/handshake`
first and verify the response protocol before sending write/read bodies.

## Configuration

`pms-server` reads Java properties. Required values are the local WAL/storage
directories and the target Paimon table.

| Key | Default | Notes |
| --- | --- | --- |
| `pms.server.host` | `127.0.0.1` | HTTP bind host. |
| `pms.server.port` | `9090` | HTTP bind port. Use `0` in tests for a random port. |
| `pms.server.commit_user` | `pms-server` | Paimon writer identity prefix. A persisted 12-hex-digit suffix identifies each local-state lifetime; keep the prefix unchanged on restart. |
| `pms.protocol.strict_http2` | `true` | Rejects non-HTTP/2 requests on binary protocol endpoints. |
| `pms.protocol.max_key_bytes` | `65536` | Maximum encoded key bytes per protocol request item. |
| `pms.protocol.max_row_bytes` | `16777216` | Maximum encoded row bytes per protocol request item. |
| `pms.protocol.max_batch_entries` | `1024` | Maximum records per `RecordBatch` and successful prefix response; cannot exceed the core batch limit. |
| `pms.protocol.max_concurrent_streams` | `128` | Jetty h2c maximum concurrent streams. |
| `pms.protocol.max_request_body_bytes` | `33554432` | Maximum binary protocol request body bytes. |
| `pms.protocol.max_response_body_bytes` | `33554432` | Maximum binary protocol response body bytes. |
| `pms.server.scheduler.flush_reconcile_interval_ms` | `1000` | Immutable MemTable reconciliation interval. |
| `pms.server.scheduler.maintenance_reconcile_interval_ms` | `30000` | Paimon visibility and local SST maintenance interval. |
| `pms.paimon.visibility.max_delay_ms` | `600000` | Target maximum normal delay before writes become visible in Paimon. |
| `pms.wal.dir` | required | WAL directory. Must differ from `pms.storage.dir`. |
| `pms.wal.file_size_mb` | `256` | WAL segment size. |
| `pms.wal.use_mmap` | `false` | Whether WAL uses mmap writer. |
| `pms.storage.dir` | required | Local SST/state directory. |
| `pms.storage.new_sst.max_count` | `10` | Maintenance target for NEW local SST count. |
| `pms.storage.sinked_sst.max_count` | `10` | Maintenance target for retained SINKED local SST count. |
| `pms.operation.sink.batch_max_bytes_mb` | `1024` | Maximum input bytes for one Sink batch; one oversized oldest run may progress alone. |
| `pms.operation.compact.max_input_size_mb` | `1024` | Maximum input bytes for one local compaction. |
| `pms.lookup.cache.enabled` | `true` | Enables the rebuildable local value SST cache. |
| `pms.lookup.cache.dir` | JVM temp directory | Lookup cache directory; must not overlap WAL, storage, or a local warehouse. |
| `pms.lookup.cache.max_bytes` | `3 GiB` | Maximum local lookup cache size. |
| `pms.lookup.cache.build_threshold` | `3` | File access count before a cache build is considered. |
| `pms.lookup.cache.build_threads` | `2` | Cache build worker count. |
| `pms.lookup.cache.build_timeout_ms` | `30000` | Timeout for one cache build. |
| `pms.lookup.cache.retry_backoff_ms` | `60000` | Backoff after a failed cache build. |
| `pms.lookup.direct.metadata_cache_entries` | `1024` | Direct Parquet lookup metadata cache entries. |
| `pms.paimon.warehouse` | required | Paimon warehouse path. |
| `pms.paimon.database` | required | Paimon database. |
| `pms.paimon.table` | required | Paimon table. |
| `pms.paimon.cache_enabled` | `true` | Enables Paimon catalog caches, including manifest cache. |
| `pms.paimon.manifest_cache_small_file_memory` | `128mb` | Paimon manifest small-file cache memory. |
| `pms.paimon.manifest_cache_small_file_threshold` | `1mb` | Maximum manifest file size treated as small-file cache candidate. |
| `pms.paimon.manifest_cache_max_memory` | unset | Optional Paimon manifest max cache memory; when larger than small-file memory, Paimon may cache all manifest files up to this budget. |
| `pms.memtable.max_entries` | `1000000` | Current memtable entry threshold. |
| `pms.memtable.max_size_mb` | `256` | Current memtable size threshold. |
| `pms.flowcontrol.overloaded_immutable_count` | `4` | Flow-control threshold. |
| `pms.flowcontrol.overloaded_pending_sst_count` | `20` | Flow-control threshold. |

The effective Paimon commit user (for example, `pms-server-7e4c9a21b6d0`) is stored in
`pms.storage.dir/commit-user`. Preserve this file together with WAL/storage when moving the
service. Fresh local state gets a new identity so restarting sequence numbers cannot cause
Paimon to skip new data as duplicate commits. Existing state without this file is rejected;
for an upgrade from the old format, finish sinking with the old version and then start with
fresh WAL/storage/cache directories. See [writer identity and recovery](../docs/pms-server.md).
