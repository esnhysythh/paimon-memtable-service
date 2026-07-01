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
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/getLocal -d '{"id":1}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/prefixLocal -d '{"id":1}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/flush -d '{}'
curl -H 'Content-Type: application/json' -X POST http://127.0.0.1:19090/sink -d '{}'
curl http://127.0.0.1:19090/state
```

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
| `pms.server.commit_user` | `pms-server` | Paimon commit user. |
| `pms.protocol.strict_http2` | `true` | Rejects non-HTTP/2 requests on binary protocol endpoints. |
| `pms.protocol.max_key_bytes` | `65536` | Maximum encoded key bytes per protocol request item. |
| `pms.protocol.max_row_bytes` | `16777216` | Maximum encoded row bytes per protocol request item. |
| `pms.protocol.max_batch_entries` | `1024` | Maximum records per binary `RecordBatch`. |
| `pms.protocol.max_concurrent_streams` | `128` | Jetty h2c maximum concurrent streams. |
| `pms.protocol.max_request_body_bytes` | `33554432` | Maximum binary protocol request body bytes. |
| `pms.protocol.max_response_body_bytes` | `33554432` | Maximum binary protocol response body bytes. |
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
| `pms.paimon.cache_enabled` | `true` | Enables Paimon catalog caches, including manifest cache. |
| `pms.paimon.manifest_cache_small_file_memory` | `128mb` | Paimon manifest small-file cache memory. |
| `pms.paimon.manifest_cache_small_file_threshold` | `1mb` | Maximum manifest file size treated as small-file cache candidate. |
| `pms.paimon.manifest_cache_max_memory` | unset | Optional Paimon manifest max cache memory; when larger than small-file memory, Paimon may cache all manifest files up to this budget. |
| `pms.memtable.max_entries` | `1000000` | Current memtable entry threshold. |
| `pms.memtable.max_size_mb` | `256` | Current memtable size threshold. |
| `pms.sink.interval_ms` | `30000` | Core sink interval default and scheduler sink fallback. |
| `pms.sink.max_pending_ssts` | `8` | Pending SST threshold. |
| `pms.flowcontrol.overloaded_immutable_count` | `4` | Flow-control threshold. |
| `pms.flowcontrol.overloaded_pending_sst_count` | `16` | Flow-control threshold. |
