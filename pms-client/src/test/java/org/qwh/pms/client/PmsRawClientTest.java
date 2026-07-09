package org.qwh.pms.client;

import org.junit.jupiter.api.Test;
import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.WriteResult;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

class PmsRawClientTest {

    @Test
    void writeBatchEncodesRecordsAndReturnsDetailedResult() {
        FakeTransport transport = new FakeTransport(handshake(8), request -> {
            List<RawKvEntry> entries = RecordBatchCodec.decodeRequest(request.body());
            assertEquals(PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, request.path());
            assertEquals(2, entries.size());
            assertArrayEquals(new byte[] {1}, entries.get(0).key());
            assertArrayEquals(new byte[] {10}, entries.get(0).row());
            assertArrayEquals(new byte[] {2}, entries.get(1).key());
            return okWrite(entries.size());
        });

        try (PmsRawClient client = newClient(transport)) {
            WriteResult result = client.writeBatchDetailed(List.of(
                RawKvEntry.put(new byte[] {1}, new byte[] {10}),
                RawKvEntry.delete(new byte[] {2})
            ));

            assertEquals(PmsStatus.OK, result.status());
            assertEquals(2, result.acceptedCount());
            assertEquals(1, transport.requests().size());
        }
    }

    @Test
    void writeBatchRetriesRetryableStatus() {
        FakeTransport transport = new FakeTransport(handshake(8));
        transport.enqueue(new PmsHttpResponse(
            503,
            WriteResultCodec.encodeResponse(WriteResult.failed(PmsStatus.OVERLOADED))
        ));
        transport.enqueue(okWrite(1));

        try (PmsRawClient client = newClient(
                transport,
                PmsClientConfig.builder(URI.create("http://127.0.0.1:9090"))
                    .writeRetryMax(1)
                    .retryInitialBackoff(Duration.ZERO)
                    .retryMaxBackoff(Duration.ZERO)
                    .build())) {
            WriteResult result = client.writeBatchDetailed(List.of(RawKvEntry.delete(new byte[] {1})));

            assertEquals(PmsStatus.OK, result.status());
            assertEquals(2, transport.requests().size());
        }
    }

    @Test
    void writeBatchValidatesHandshakeLimitsBeforeSending() {
        FakeTransport transport = new FakeTransport(handshake(1));

        try (PmsRawClient client = newClient(transport)) {
            assertThrows(
                IllegalArgumentException.class,
                () -> client.writeBatchDetailed(List.of(
                    RawKvEntry.delete(new byte[] {1}),
                    RawKvEntry.delete(new byte[] {2})
                ))
            );
            assertEquals(0, transport.requests().size());
        }
    }

    @Test
    void singleLookupRequiresOneResult() {
        FakeTransport transport = new FakeTransport(handshake(8));
        transport.enqueue(new PmsHttpResponse(
            200,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.ok(List.of()))
        ));

        try (PmsRawClient client = newClient(transport)) {
            assertThrows(PmsClientProtocolException.class, () -> client.getLocal(new byte[] {1}));
        }
    }

    @Test
    void batchWriterFlushesAtLimitAndClose() {
        FakeTransport transport = new FakeTransport(handshake(8), request -> {
            List<RawKvEntry> entries = RecordBatchCodec.decodeRequest(request.body());
            return okWrite(entries.size());
        });

        try (PmsRawClient client = newClient(transport);
             PmsRawBatchWriter writer = client.newBatchWriter(2)) {
            writer.put(new byte[] {1}, new byte[] {10});
            assertEquals(1, writer.pendingCount());

            writer.delete(new byte[] {2});
            assertEquals(0, writer.pendingCount());
            assertEquals(1, transport.requests().size());

            writer.put(new byte[] {3}, new byte[] {30});
            assertEquals(1, writer.pendingCount());
        }

        assertEquals(2, transport.requests().size());
        assertEquals(1, RecordBatchCodec.decodeRequest(transport.requests().get(1).body()).size());
    }

    @Test
    void getFullReturnsFailedLookupStatus() {
        FakeTransport transport = new FakeTransport(handshake(8));
        transport.enqueue(new PmsHttpResponse(
            503,
            LookupBatchCodec.encodeResponse(RawLookupBatchResult.failed(PmsStatus.LOOKUP_UNAVAILABLE))
        ));

        try (PmsRawClient client = newClient(transport)) {
            RawLookupResult result = client.getFull(new byte[] {1});

            assertEquals(PmsStatus.LOOKUP_UNAVAILABLE, result.status());
            assertEquals(PmsProtocolConstants.FULL_GET_PATH, transport.requests().get(0).path());
        }
    }

    @Test
    void rejectsResponseBodyAboveNegotiatedLimit() {
        FakeTransport transport = new FakeTransport(handshake(8, 4));
        transport.enqueue(new PmsHttpResponse(200, new byte[5]));

        try (PmsRawClient client = newClient(transport)) {
            assertThrows(PmsClientProtocolException.class, () -> client.getLocal(new byte[] {1}));
        }
    }

    @Test
    void configRejectsZeroConnectTimeout() {
        assertThrows(
            IllegalArgumentException.class,
            () -> PmsClientConfig.builder(URI.create("http://127.0.0.1:9090"))
                .connectTimeout(Duration.ZERO)
                .build()
        );
    }

    private static PmsRawClient newClient(FakeTransport transport) {
        return newClient(transport, PmsClientConfig.forEndpoint("127.0.0.1", 9090));
    }

    private static PmsRawClient newClient(FakeTransport transport, PmsClientConfig config) {
        return new PmsRawClient(config, transport);
    }

    private static PmsHttpResponse okWrite(int acceptedCount) {
        return new PmsHttpResponse(200, WriteResultCodec.encodeResponse(WriteResult.ok(acceptedCount)));
    }

    private static PmsHandshake handshake(int maxBatchEntries) {
        return handshake(maxBatchEntries, 8192);
    }

    private static PmsHandshake handshake(int maxBatchEntries, int maxResponseBodyBytes) {
        return new PmsHandshake(
            PmsProtocolConstants.PROTOCOL_NAME,
            PmsProtocolConstants.PROTOCOL_VERSION,
            PmsProtocolConstants.REQUIRED_HTTP_VERSION,
            "pms-test",
            64,
            1024,
            maxBatchEntries,
            8,
            8192,
            maxResponseBodyBytes,
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
        public PmsHttpResponse postBinary(
                String path,
                byte[] body,
                Duration timeout,
                int maxResponseBodyBytes) {
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
