package org.qwh.pms.client;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;
import org.qwh.pms.codec.PmsPrimaryKeyCodec;
import org.qwh.pms.codec.PmsRowTypeJson;
import org.qwh.pms.codec.PmsRowValueCodec;
import org.qwh.pms.protocol.api.LookupResultType;
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

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsClientTest {

    private final RowType rowType = DataTypes.ROW(
        DataTypes.FIELD(1, "id", DataTypes.INT()),
        DataTypes.FIELD(2, "marker", DataTypes.STRING())
    );
    private final PmsPrimaryKeyCodec keyCodec = PmsPrimaryKeyCodec.forFieldNames(rowType, List.of("id"));
    private final PmsRowValueCodec valueCodec = new PmsRowValueCodec();

    @Test
    void connectFromServerSchemaInitializesRowAwareClient() {
        FakeTransport transport = new FakeTransport(handshakeWithTableSchema(3), request -> {
            assertEquals(PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, request.path());
            List<RawKvEntry> entries = RecordBatchCodec.decodeRequest(request.body());
            assertEquals(1, entries.size());
            assertPut(entries.get(0), row(9, "schema-a"));
            return okWrite(1);
        });
        PmsRawClient rawClient = new PmsRawClient(PmsClientConfig.forUri(URI.create("http://127.0.0.1:9090")), transport);

        try (PmsClient client = PmsClient.connectFromServerSchema(rawClient)) {
            assertEquals(rowType, client.rowType());
            assertEquals(List.of("id"), client.primaryKeyFieldNames());
            assertEquals(3, client.writerSchemaId());

            WriteResult result = client.write(row(9, "schema-a"));
            assertEquals(PmsStatus.OK, result.status());
            assertEquals(1, result.acceptedCount());
        }
    }

    @Test
    void writeBatchNormalizesRowKindsToRawKvEntries() {
        FakeTransport transport = new FakeTransport(handshake(), request -> {
            assertEquals(PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, request.path());
            List<RawKvEntry> entries = RecordBatchCodec.decodeRequest(request.body());

            assertEquals(3, entries.size());
            assertPut(entries.get(0), row(1, "insert-a"));
            assertPut(entries.get(1), row(RowKind.UPDATE_AFTER, 2, "update-b"));
            assertArrayEquals(keyCodec.encodeKey(row(RowKind.DELETE, 1, "ignored")), entries.get(2).key());
            assertTrue(entries.get(2).isDelete());
            return okWrite(entries.size());
        });

        try (PmsClient client = newClient(transport)) {
            WriteResult result = client.writeBatch(List.of(
                row(1, "insert-a"),
                row(RowKind.UPDATE_AFTER, 2, "update-b"),
                row(RowKind.DELETE, 1, "ignored")
            ));

            assertEquals(PmsStatus.OK, result.status());
            assertEquals(3, result.acceptedCount());
        }
    }

    @Test
    void explicitDeleteUsesPrimaryKeyTuple() {
        FakeTransport transport = new FakeTransport(handshake(), request -> {
            assertEquals(PmsProtocolConstants.LOCAL_DELETE_PATH, request.path());
            List<RawKvEntry> entries = RecordBatchCodec.decodeRequest(request.body());
            assertEquals(1, entries.size());
            assertTrue(entries.get(0).isDelete());
            assertArrayEquals(keyCodec.encodeKeyTuple(GenericRow.of(7)), entries.get(0).key());
            return okWrite(1);
        });

        try (PmsClient client = newClient(transport)) {
            WriteResult result = client.delete(GenericRow.of(7));

            assertEquals(PmsStatus.OK, result.status());
            assertEquals(1, result.acceptedCount());
        }
    }

    @Test
    void getFullDecodesHitRowsAndPreservesMiss() {
        FakeTransport transport = new FakeTransport(handshake());
        transport.enqueue(new PmsHttpResponse(
            200,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.single(RawLookupResult.hit(encodedRow(1, "hit-a"))))
        ));
        transport.enqueue(new PmsHttpResponse(
            200,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.single(RawLookupResult.miss()))
        ));

        try (PmsClient client = newClient(transport)) {
            PmsRowLookupResult hit = client.get(GenericRow.of(1));
            assertTrue(hit.found());
            assertEquals(LookupResultType.HIT, hit.type());
            assertEquals(1, hit.row().getInt(0));
            assertEquals("hit-a", hit.row().getString(1).toString());

            PmsRowLookupResult miss = client.get(GenericRow.of(2));
            assertFalse(miss.found());
            assertEquals(LookupResultType.MISS, miss.type());
        }
    }

    @Test
    void prefixLocalDecodesRowBatchAndFailedStatus() {
        FakeTransport transport = new FakeTransport(handshake());
        transport.enqueue(new PmsHttpResponse(
            200,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.ok(List.of(
                RawLookupResult.hit(encodedRow(1, "prefix-a")),
                RawLookupResult.hit(encodedRow(2, "prefix-b"))
            )))
        ));
        transport.enqueue(new PmsHttpResponse(
            503,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.failed(PmsStatus.LOOKUP_UNAVAILABLE))
        ));

        try (PmsClient client = newClient(transport)) {
            PmsRowLookupBatchResult ok = client.prefixLocal(GenericRow.of(1));
            assertEquals(PmsStatus.OK, ok.status());
            assertEquals(2, ok.results().size());
            assertEquals("prefix-a", ok.results().get(0).row().getString(1).toString());
            assertEquals("prefix-b", ok.results().get(1).row().getString(1).toString());
            assertArrayEquals(
                keyCodec.encodePrefixTuple(GenericRow.of(1)),
                KeyBatchCodec.decodeRequest(transport.requests().get(0).body()).get(0)
            );

            PmsRowLookupBatchResult failed = client.prefixLocal(GenericRow.of(1));
            assertEquals(PmsStatus.LOOKUP_UNAVAILABLE, failed.status());
            assertEquals(0, failed.results().size());
        }
    }

    private PmsClient newClient(FakeTransport transport) {
        PmsRawClient rawClient = new PmsRawClient(PmsClientConfig.forUri(URI.create("http://127.0.0.1:9090")), transport);
        return new PmsClient(rawClient, rowType, List.of("id"), 0);
    }

    private void assertPut(RawKvEntry entry, GenericRow expectedRow) {
        assertFalse(entry.isDelete());
        assertArrayEquals(keyCodec.encodeKey(expectedRow), entry.key());
        assertEquals(
            expectedRow.getString(1).toString(),
            valueCodec.decode(rowType, entry.row()).getString(1).toString()
        );
    }

    private byte[] encodedRow(int id, String marker) {
        return valueCodec.encode(rowType, row(id, marker), 0);
    }

    private static GenericRow row(int id, String marker) {
        return row(RowKind.INSERT, id, marker);
    }

    private static GenericRow row(RowKind kind, int id, String marker) {
        GenericRow row = new GenericRow(kind, 2);
        row.setField(0, id);
        row.setField(1, BinaryString.fromString(marker));
        return row;
    }

    private static PmsHttpResponse okWrite(int acceptedCount) {
        return new PmsHttpResponse(200, WriteResultCodec.encodeResponse(WriteResult.ok(acceptedCount)));
    }

    private static PmsHandshake handshake() {
        return new PmsHandshake(
            PmsProtocolConstants.PROTOCOL_NAME,
            PmsProtocolConstants.PROTOCOL_VERSION,
            PmsProtocolConstants.REQUIRED_HTTP_VERSION,
            "pms-test",
            64,
            1024,
            8,
            8,
            8192,
            8192,
            PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES
        );
    }

    private PmsHandshake handshakeWithTableSchema(long schemaId) {
        PmsTableSchema tableSchema = PmsTableSchema.create(
            schemaId,
            PmsTableSchema.ROW_TYPE_FORMAT_PAIMON_JSON_V1,
            PmsRowTypeJson.serialize(rowType),
            List.of("id"),
            List.of(),
            PmsRowValueCodec.FORMAT_VERSION,
            PmsPrimaryKeyCodec.FORMAT_VERSION
        );
        return new PmsHandshake(
            PmsProtocolConstants.PROTOCOL_NAME,
            PmsProtocolConstants.PROTOCOL_VERSION,
            PmsProtocolConstants.REQUIRED_HTTP_VERSION,
            "pms-test",
            64,
            1024,
            8,
            8,
            8192,
            8192,
            tableSchema,
            PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES
        );
    }

    private record Request(String path, byte[] body, Duration timeout) {}

    private static final class FakeTransport implements PmsTransport {
        private final PmsHandshake handshake;
        private final Function<Request, PmsHttpResponse> handler;
        private final Queue<PmsHttpResponse> responses = new ArrayDeque<>();
        private final List<Request> requests = new ArrayList<>();

        private FakeTransport(PmsHandshake handshake) {
            this(handshake, null);
        }

        private FakeTransport(PmsHandshake handshake, Function<Request, PmsHttpResponse> handler) {
            this.handshake = handshake;
            this.handler = handler;
        }

        @Override
        public PmsHandshake handshake() {
            return handshake;
        }

        @Override
        public PmsHttpResponse postBinary(String path, byte[] body, Duration timeout) {
            Request request = new Request(path, body, timeout);
            requests.add(request);
            if (handler != null) {
                return handler.apply(request);
            }
            PmsHttpResponse response = responses.poll();
            if (response == null) {
                throw new IllegalStateException("no fake response queued");
            }
            return response;
        }

        private void enqueue(PmsHttpResponse response) {
            responses.add(response);
        }

        private List<Request> requests() {
            return requests;
        }
    }
}
