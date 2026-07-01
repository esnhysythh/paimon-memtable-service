package org.qwh.pms.server;

public record PmsProtocolConfig(
    boolean strictHttp2,
    int maxKeyBytes,
    int maxRowBytes,
    int maxBatchEntries,
    int maxConcurrentStreams,
    int maxRequestBodyBytes,
    int maxResponseBodyBytes
) {
    public static final boolean DEFAULT_STRICT_HTTP2 = true;
    public static final int DEFAULT_MAX_KEY_BYTES = 64 * 1024;
    public static final int DEFAULT_MAX_ROW_BYTES = 16 * 1024 * 1024;
    public static final int DEFAULT_MAX_BATCH_ENTRIES = 1024;
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS = 128;
    public static final int DEFAULT_MAX_REQUEST_BODY_BYTES = 32 * 1024 * 1024;
    public static final int DEFAULT_MAX_RESPONSE_BODY_BYTES = 32 * 1024 * 1024;

    public PmsProtocolConfig {
        requirePositive(maxKeyBytes, "maxKeyBytes");
        requirePositive(maxRowBytes, "maxRowBytes");
        requirePositive(maxBatchEntries, "maxBatchEntries");
        requirePositive(maxConcurrentStreams, "maxConcurrentStreams");
        requirePositive(maxRequestBodyBytes, "maxRequestBodyBytes");
        requirePositive(maxResponseBodyBytes, "maxResponseBodyBytes");
    }

    public static PmsProtocolConfig defaults() {
        return new PmsProtocolConfig(
            DEFAULT_STRICT_HTTP2,
            DEFAULT_MAX_KEY_BYTES,
            DEFAULT_MAX_ROW_BYTES,
            DEFAULT_MAX_BATCH_ENTRIES,
            DEFAULT_MAX_CONCURRENT_STREAMS,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
            DEFAULT_MAX_RESPONSE_BODY_BYTES
        );
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
