package org.qwh.pms.flink;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.qwh.pms.client.PmsClientConfig;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;

/** 已完成范围校验、可以随 Flink job graph 序列化的 Connector 配置. */
public record PmsConnectorConfig(
        String endpoint,
        Duration connectTimeout,
        Duration writeTimeout,
        Duration readTimeout,
        boolean requireHttp2,
        Integer sinkParallelism,
        int sinkBatchMaxRows,
        long sinkBatchMaxBytes,
        Duration sinkFlushInterval,
        int sinkWriteRetryMaxRetries,
        Duration sinkWriteRetryInitialBackoff,
        Duration sinkWriteRetryMaxBackoff,
        boolean lookupAsync,
        int lookupAsyncThreadNumber,
        int lookupMaxRetries,
        Duration lookupRetryInitialBackoff,
        Duration lookupRetryMaxBackoff)
        implements Serializable {

    public static PmsConnectorConfig from(ReadableConfig options) {
        PmsConnectorConfig config =
                new PmsConnectorConfig(
                        options.get(PmsConnectorOptions.ENDPOINT),
                        options.get(PmsConnectorOptions.CLIENT_CONNECT_TIMEOUT),
                        options.get(PmsConnectorOptions.CLIENT_WRITE_TIMEOUT),
                        options.get(PmsConnectorOptions.CLIENT_READ_TIMEOUT),
                        options.get(PmsConnectorOptions.CLIENT_REQUIRE_HTTP2),
                        options.getOptional(PmsConnectorOptions.SINK_PARALLELISM).orElse(null),
                        options.get(PmsConnectorOptions.SINK_BATCH_MAX_ROWS),
                        options.get(PmsConnectorOptions.SINK_BATCH_MAX_BYTES).getBytes(),
                        options.get(PmsConnectorOptions.SINK_FLUSH_INTERVAL),
                        options.get(PmsConnectorOptions.SINK_WRITE_RETRY_MAX_RETRIES),
                        options.get(PmsConnectorOptions.SINK_WRITE_RETRY_INITIAL_BACKOFF),
                        options.get(PmsConnectorOptions.SINK_WRITE_RETRY_MAX_BACKOFF),
                        options.get(PmsConnectorOptions.LOOKUP_ASYNC),
                        options.get(PmsConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER),
                        options.get(PmsConnectorOptions.LOOKUP_MAX_RETRIES),
                        options.get(PmsConnectorOptions.LOOKUP_RETRY_INITIAL_BACKOFF),
                        options.get(PmsConnectorOptions.LOOKUP_RETRY_MAX_BACKOFF));
        config.validate();
        return config;
    }

    public PmsClientConfig clientConfig() {
        return PmsClientConfig.builder(URI.create(endpoint))
                .connectTimeout(connectTimeout)
                .writeTimeout(writeTimeout)
                .readTimeout(readTimeout)
                .writeRetryMax(sinkWriteRetryMaxRetries)
                .retryInitialBackoff(sinkWriteRetryInitialBackoff)
                .retryMaxBackoff(sinkWriteRetryMaxBackoff)
                .requireHttp2(requireHttp2)
                .build();
    }

    private void validate() {
        try {
            URI uri = URI.create(endpoint);
            String scheme = uri.getScheme();
            if ((!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))
                    || uri.getHost() == null
                    || uri.getHost().isBlank()
                    || uri.getUserInfo() != null
                    || uri.getQuery() != null
                    || uri.getFragment() != null
                    || !(uri.getPath().isEmpty() || "/".equals(uri.getPath()))) {
                throw new IllegalArgumentException(
                        "endpoint 必须是无认证信息、query、fragment 和 path 的 HTTP(S) 根 URI.");
            }
        } catch (IllegalArgumentException e) {
            throw new ValidationException("无效的 PMS endpoint: " + endpoint, e);
        }
        requirePositive(connectTimeout, "client.connect-timeout");
        requireNonNegative(writeTimeout, "client.write-timeout");
        requireNonNegative(readTimeout, "client.read-timeout");
        if (sinkParallelism != null && sinkParallelism <= 0) {
            throw new ValidationException("sink.parallelism 必须大于 0.");
        }
        if (sinkBatchMaxRows <= 0) {
            throw new ValidationException("sink.batch.max-rows 必须大于 0.");
        }
        if (sinkBatchMaxBytes <= 0) {
            throw new ValidationException("sink.batch.max-bytes 必须大于 0.");
        }
        requireNonNegative(sinkFlushInterval, "sink.flush.interval");
        requireNonNegative(sinkWriteRetryMaxRetries, "sink.write-retry.max-retries");
        requireNonNegative(sinkWriteRetryInitialBackoff, "sink.write-retry.initial-backoff");
        requireNonNegative(sinkWriteRetryMaxBackoff, "sink.write-retry.max-backoff");
        if (sinkWriteRetryMaxBackoff.compareTo(sinkWriteRetryInitialBackoff) < 0) {
            throw new ValidationException(
                    "sink.write-retry.max-backoff 不能小于 sink.write-retry.initial-backoff.");
        }
        if (lookupAsyncThreadNumber <= 0) {
            throw new ValidationException("lookup.async.thread-number 必须大于 0.");
        }
        requireNonNegative(lookupMaxRetries, "lookup.max-retries");
        requireNonNegative(lookupRetryInitialBackoff, "lookup.retry.initial-backoff");
        requireNonNegative(lookupRetryMaxBackoff, "lookup.retry.max-backoff");
        if (lookupRetryMaxBackoff.compareTo(lookupRetryInitialBackoff) < 0) {
            throw new ValidationException(
                    "lookup.retry.max-backoff 不能小于 lookup.retry.initial-backoff.");
        }
    }

    private static void requirePositive(Duration value, String name) {
        requireNonNegative(value, name);
        if (value.isZero()) {
            throw new ValidationException(name + " 必须大于 0.");
        }
    }

    private static void requireNonNegative(Duration value, String name) {
        if (value.isNegative()) {
            throw new ValidationException(name + " 不能为负数.");
        }
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new ValidationException(name + " 不能为负数.");
        }
    }
}
