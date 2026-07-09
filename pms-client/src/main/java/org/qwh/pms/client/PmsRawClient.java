package org.qwh.pms.client;

import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawKvStore;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.WriteResult;
import org.qwh.pms.protocol.codec.KeyBatchCodec;
import org.qwh.pms.protocol.codec.LookupBatchCodec;
import org.qwh.pms.protocol.codec.RecordBatchCodec;
import org.qwh.pms.protocol.codec.WriteResultCodec;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public final class PmsRawClient implements RawKvStore, AutoCloseable {

    private final PmsClientConfig config;
    private final PmsTransport transport;
    private final PmsHandshake handshake;
    private volatile boolean closed;

    public static PmsRawClient connect(PmsClientConfig config) {
        return new PmsRawClient(config, new JdkHttpPmsTransport(config));
    }

    public static PmsRawClient connect(URI serverUri) {
        return connect(PmsClientConfig.forUri(serverUri));
    }

    PmsRawClient(PmsClientConfig config, PmsTransport transport) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
        this.handshake = transport.handshake();
        this.handshake.requireCompatible();
    }

    public PmsHandshake handshake() {
        return handshake;
    }

    public PmsRawBatchWriter newBatchWriter() {
        return new PmsRawBatchWriter(this, handshake.maxBatchEntries());
    }

    public PmsRawBatchWriter newBatchWriter(int maxBatchEntries) {
        return new PmsRawBatchWriter(this, maxBatchEntries);
    }

    @Override
    public PmsStatus put(byte[] key, byte[] row) {
        return writeSingle(RawKvEntry.put(key, row), PmsProtocolConstants.LOCAL_PUT_PATH).status();
    }

    @Override
    public PmsStatus delete(byte[] key) {
        return writeSingle(RawKvEntry.delete(key), PmsProtocolConstants.LOCAL_DELETE_PATH).status();
    }

    @Override
    public PmsStatus writeBatch(List<RawKvEntry> entries) {
        return writeBatchDetailed(entries).status();
    }

    public WriteResult putDetailed(byte[] key, byte[] row) {
        return writeSingle(RawKvEntry.put(key, row), PmsProtocolConstants.LOCAL_PUT_PATH);
    }

    public WriteResult deleteDetailed(byte[] key) {
        return writeSingle(RawKvEntry.delete(key), PmsProtocolConstants.LOCAL_DELETE_PATH);
    }

    public WriteResult writeBatchDetailed(List<RawKvEntry> entries) {
        requireOpen();
        validateEntries(entries);
        byte[] body = RecordBatchCodec.encodeRequest(entries);
        requireRequestBodySize(body.length);
        return writeWithRetry(PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH, body, entries.size());
    }

    @Override
    public RawLookupResult getLocal(byte[] key) {
        return singleLookup(PmsProtocolConstants.LOCAL_GET_PATH, key);
    }

    @Override
    public RawLookupResult getFull(byte[] key) {
        return singleLookup(PmsProtocolConstants.FULL_GET_PATH, key);
    }

    @Override
    public RawLookupBatchResult getPrefixLocal(byte[] prefix) {
        requireOpen();
        Objects.requireNonNull(prefix, "prefix must not be null");
        requireMax(prefix.length, handshake.maxKeyBytes(), "prefix bytes");
        byte[] body = KeyBatchCodec.encodeSingle(prefix);
        requireRequestBodySize(body.length);
        PmsHttpResponse response = transport.postBinary(
            PmsProtocolConstants.LOCAL_GET_PREFIX_PATH,
            body,
            config.readTimeout(),
            handshake.maxResponseBodyBytes()
        );
        return decodeLookupResponse(response, handshake.maxBatchEntries());
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            transport.close();
        }
    }

    private WriteResult writeSingle(RawKvEntry entry, String path) {
        requireOpen();
        validateEntry(entry);
        byte[] body = RecordBatchCodec.encodeRequest(List.of(entry));
        requireRequestBodySize(body.length);
        return writeWithRetry(path, body, 1);
    }

    private WriteResult writeWithRetry(String path, byte[] body, int expectedAcceptedCount) {
        int attempt = 0;
        while (true) {
            PmsHttpResponse response = transport.postBinary(
                path,
                body,
                config.writeTimeout(),
                handshake.maxResponseBodyBytes()
            );
            WriteResult result = decodeWriteResponse(response, expectedAcceptedCount);
            if (!isRetryableWriteStatus(result.status()) || attempt >= config.writeRetryMax()) {
                return result;
            }
            sleepBeforeRetry(attempt);
            attempt++;
        }
    }

    private RawLookupResult singleLookup(String path, byte[] key) {
        requireOpen();
        Objects.requireNonNull(key, "key must not be null");
        if (key.length == 0) {
            throw new IllegalArgumentException("key must not be empty");
        }
        requireMax(key.length, handshake.maxKeyBytes(), "key bytes");
        byte[] body = KeyBatchCodec.encodeSingle(key);
        requireRequestBodySize(body.length);
        PmsHttpResponse response = transport.postBinary(
            path,
            body,
            config.readTimeout(),
            handshake.maxResponseBodyBytes()
        );
        RawLookupBatchResult batch = decodeLookupResponse(response, 1);
        if (batch.status() != PmsStatus.OK) {
            return RawLookupResult.failed(batch.status());
        }
        if (batch.results().size() != 1) {
            throw new PmsClientProtocolException(
                "single lookup expected one result but received " + batch.results().size());
        }
        return batch.results().get(0);
    }

    private WriteResult decodeWriteResponse(PmsHttpResponse response, int expectedAcceptedCount) {
        try {
            requireResponseBodySize(response.body().length);
            WriteResult result = WriteResultCodec.decodeResponse(response.body());
            requireHttpStatusConsistent(response.statusCode(), result.status());
            if (result.status() == PmsStatus.OK && result.acceptedCount() != expectedAcceptedCount) {
                throw new PmsClientProtocolException(
                    "OK write acceptedCount mismatch: expected="
                        + expectedAcceptedCount
                        + ", actual="
                        + result.acceptedCount());
            }
            return result;
        } catch (IllegalArgumentException e) {
            throw new PmsClientProtocolException("failed to decode PMS write response", e);
        }
    }

    private RawLookupBatchResult decodeLookupResponse(PmsHttpResponse response, int maxResults) {
        try {
            requireResponseBodySize(response.body().length);
            RawLookupBatchResult result = LookupBatchCodec.decodeResponse(
                response.body(),
                maxResults,
                handshake.maxRowBytes()
            );
            requireHttpStatusConsistent(response.statusCode(), result.status());
            return result;
        } catch (IllegalArgumentException e) {
            throw new PmsClientProtocolException("failed to decode PMS lookup response", e);
        }
    }

    private void validateEntries(List<RawKvEntry> entries) {
        Objects.requireNonNull(entries, "entries must not be null");
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }
        requireMax(entries.size(), handshake.maxBatchEntries(), "batch entries");
        entries.forEach(this::validateEntry);
    }

    private void validateEntry(RawKvEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        requireMax(entry.key().length, handshake.maxKeyBytes(), "key bytes");
        if (!entry.isDelete()) {
            requireMax(entry.row().length, handshake.maxRowBytes(), "row bytes");
        }
    }

    private void requireRequestBodySize(int length) {
        requireMax(length, handshake.maxRequestBodyBytes(), "request body bytes");
    }

    private void requireResponseBodySize(int length) {
        if (length > handshake.maxResponseBodyBytes()) {
            throw new PmsClientProtocolException(
                "response body bytes exceeds server limit: "
                    + length
                    + " > "
                    + handshake.maxResponseBodyBytes());
        }
    }

    private static void requireMax(int actual, int max, String name) {
        if (actual > max) {
            throw new IllegalArgumentException(name + " exceeds server limit: " + actual + " > " + max);
        }
    }

    private void requireHttpStatusConsistent(int httpStatus, PmsStatus status) {
        if (httpStatus >= 200 && httpStatus < 300 && status != PmsStatus.OK) {
            throw new PmsClientProtocolException("HTTP success carried non-OK PMS status: " + status);
        }
        if (httpStatus >= 400 && status == PmsStatus.OK) {
            throw new PmsClientProtocolException("HTTP error carried OK PMS status: statusCode=" + httpStatus);
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new PmsClientException("PMS raw client is closed");
        }
    }

    private static boolean isRetryableWriteStatus(PmsStatus status) {
        return status == PmsStatus.OVERLOADED || status == PmsStatus.SHUTTING_DOWN;
    }

    private void sleepBeforeRetry(int attempt) {
        Duration delay = retryDelay(attempt);
        if (delay.isZero()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PmsClientException("interrupted while waiting to retry PMS write", e);
        }
    }

    private Duration retryDelay(int attempt) {
        long baseMillis = config.retryInitialBackoff().toMillis();
        long maxMillis = config.retryMaxBackoff().toMillis();
        long multiplier = 1L << Math.min(attempt, 30);
        long delayMillis;
        try {
            delayMillis = Math.multiplyExact(baseMillis, multiplier);
        } catch (ArithmeticException e) {
            delayMillis = Long.MAX_VALUE;
        }
        return Duration.ofMillis(Math.min(delayMillis, maxMillis));
    }
}
