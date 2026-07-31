package org.qwh.pms.flink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowTypeJson;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.PmsTableSchema;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.WriteResult;
import org.qwh.pms.protocol.codec.KeyBatchCodec;
import org.qwh.pms.protocol.codec.LookupBatchCodec;
import org.qwh.pms.protocol.codec.RecordBatchCodec;
import org.qwh.pms.protocol.codec.WriteResultCodec;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于真实 PMS wire codec 的轻量 HTTP/1.1 测试服务.
 *
 * <p>Connector 测试把 {@code client.require-http2} 关闭, 只替换网络承载层. 握手、主键编码、
 * RowValue 编解码和写入/查询 response 仍走生产协议实现.
 */
public final class TestingPmsServer implements AutoCloseable {

    private final HttpServer server;
    private final PmsPrimaryKeyCodec keyCodec;
    private final PmsRowValueCodec valueCodec = new PmsRowValueCodec();
    private final RowType rowType;
    private final Map<String, byte[]> rows = new ConcurrentHashMap<>();
    private final Set<String> tombstones = ConcurrentHashMap.newKeySet();
    private final Queue<PmsStatus> lookupStatuses = new ConcurrentLinkedQueue<>();
    private final AtomicInteger lookupRequests = new AtomicInteger();
    private final AtomicInteger writeRequests = new AtomicInteger();
    private final AtomicInteger deleteRequests = new AtomicInteger();

    public TestingPmsServer(RowType rowType, List<String> primaryKeys) throws IOException {
        this.rowType = rowType;
        keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, primaryKeys);
        server =
                HttpServer.create(
                        new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        PmsHandshake handshake = handshake(rowType, primaryKeys);
        server.createContext(
                PmsProtocolConstants.HANDSHAKE_PATH,
                exchange ->
                        send(
                                exchange,
                                200,
                                "application/json",
                                handshake.toJson().getBytes(StandardCharsets.UTF_8)));
        server.createContext(
                PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, this::handleWrite);
        server.createContext(PmsProtocolConstants.LOCAL_DELETE_PATH, this::handleWrite);
        server.createContext(PmsProtocolConstants.LOCAL_PUT_PATH, this::handleWrite);
        server.createContext(PmsProtocolConstants.FULL_GET_PATH, this::handleLookup);
        server.createContext(PmsProtocolConstants.LOCAL_GET_PATH, this::handleLookup);
        server.start();
    }

    public URI endpoint() {
        return URI.create(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/");
    }

    public void putRow(InternalRow row) {
        String key = key(keyCodec.encodeKey(row));
        rows.put(key, valueCodec.encode(rowType, row, 0));
        tombstones.remove(key);
    }

    public void tombstoneKeyTuple(InternalRow keyTuple) {
        String key = key(keyCodec.encodeKeyTuple(keyTuple));
        rows.remove(key);
        tombstones.add(key);
    }

    public boolean containsKeyTuple(InternalRow keyTuple) {
        return rows.containsKey(key(keyCodec.encodeKeyTuple(keyTuple)));
    }

    public InternalRow rowForKeyTuple(InternalRow keyTuple) {
        byte[] encoded = rows.get(key(keyCodec.encodeKeyTuple(keyTuple)));
        return encoded == null ? null : valueCodec.decode(rowType, encoded);
    }

    public boolean isDeletedKeyTuple(InternalRow keyTuple) {
        return tombstones.contains(key(keyCodec.encodeKeyTuple(keyTuple)));
    }

    public void enqueueLookupStatus(PmsStatus status) {
        if (status == PmsStatus.OK) {
            throw new IllegalArgumentException("OK 不需要注入.");
        }
        lookupStatuses.add(status);
    }

    public int lookupRequests() {
        return lookupRequests.get();
    }

    public int writeRequests() {
        return writeRequests.get();
    }

    public int deleteRequests() {
        return deleteRequests.get();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handleWrite(HttpExchange exchange) throws IOException {
        try {
            List<RawKvEntry> entries =
                    RecordBatchCodec.decodeRequest(exchange.getRequestBody().readAllBytes());
            writeRequests.incrementAndGet();
            if (PmsProtocolConstants.LOCAL_DELETE_PATH.equals(
                    exchange.getRequestURI().getPath())) {
                deleteRequests.incrementAndGet();
            }
            for (RawKvEntry entry : entries) {
                String key = key(entry.key());
                if (entry.isDelete()) {
                    rows.remove(key);
                    tombstones.add(key);
                } else {
                    rows.put(key, entry.row());
                    tombstones.remove(key);
                }
            }
            sendBinary(
                    exchange,
                    200,
                    WriteResultCodec.encodeResponse(WriteResult.ok(entries.size())));
        } catch (RuntimeException e) {
            sendBinary(
                    exchange,
                    500,
                    WriteResultCodec.encodeResponse(
                            WriteResult.failed(PmsStatus.INTERNAL_ERROR)));
        }
    }

    private void handleLookup(HttpExchange exchange) throws IOException {
        lookupRequests.incrementAndGet();
        PmsStatus injected = lookupStatuses.poll();
        if (injected != null) {
            sendBinary(
                    exchange,
                    503,
                    LookupBatchCodec.encodeResponse(
                            RawLookupBatchResult.failed(injected)));
            return;
        }

        List<byte[]> keys =
                KeyBatchCodec.decodeRequest(exchange.getRequestBody().readAllBytes());
        String key = key(keys.get(0));
        RawLookupResult result;
        byte[] row = rows.get(key);
        if (row != null) {
            result = RawLookupResult.hit(row);
        } else if (tombstones.contains(key)) {
            result = RawLookupResult.deleted();
        } else {
            result = RawLookupResult.miss();
        }
        sendBinary(
                exchange,
                200,
                LookupBatchCodec.encodeResponse(
                        RawLookupBatchResult.single(result)));
    }

    private static PmsHandshake handshake(RowType rowType, List<String> primaryKeys) {
        PmsTableSchema tableSchema =
                PmsTableSchema.create(
                        0,
                        PmsTableSchema.ROW_TYPE_FORMAT_PAIMON_JSON_V1,
                        PmsRowTypeJson.serialize(rowType),
                        primaryKeys,
                        List.of(),
                        PmsRowValueCodec.FORMAT_VERSION,
                        PmsPrimaryKeyCodec.FORMAT_VERSION);
        return new PmsHandshake(
                PmsProtocolConstants.PROTOCOL_NAME,
                PmsProtocolConstants.PROTOCOL_VERSION,
                PmsProtocolConstants.REQUIRED_HTTP_VERSION,
                "flink-connector-test",
                1024,
                16 * 1024,
                128,
                16,
                1024 * 1024,
                1024 * 1024,
                tableSchema,
                PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES);
    }

    private static void sendBinary(
            HttpExchange exchange, int status, byte[] body) throws IOException {
        send(exchange, status, PmsProtocolConstants.CONTENT_TYPE_BINARY, body);
    }

    private static void send(
            HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("content-type", contentType);
        exchange.sendResponseHeaders(status, body.length);
        try (exchange; var output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private static String key(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }
}
