package org.qwh.pms.client;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

public record PmsClientConfig(
    URI serverUri,
    Duration connectTimeout,
    Duration writeTimeout,
    Duration readTimeout,
    int writeRetryMax,
    Duration retryInitialBackoff,
    Duration retryMaxBackoff,
    boolean requireHttp2
) {
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(3);
    public static final int DEFAULT_WRITE_RETRY_MAX = 10;
    public static final Duration DEFAULT_RETRY_INITIAL_BACKOFF = Duration.ofMillis(10);
    public static final Duration DEFAULT_RETRY_MAX_BACKOFF = Duration.ofMillis(640);

    public PmsClientConfig {
        Objects.requireNonNull(serverUri, "serverUri must not be null");
        Objects.requireNonNull(connectTimeout, "connectTimeout must not be null");
        Objects.requireNonNull(writeTimeout, "writeTimeout must not be null");
        Objects.requireNonNull(readTimeout, "readTimeout must not be null");
        Objects.requireNonNull(retryInitialBackoff, "retryInitialBackoff must not be null");
        Objects.requireNonNull(retryMaxBackoff, "retryMaxBackoff must not be null");
        requireHttpUri(serverUri);
        requireNonNegative(writeRetryMax, "writeRetryMax");
        requireNonNegative(connectTimeout, "connectTimeout");
        requireNonNegative(writeTimeout, "writeTimeout");
        requireNonNegative(readTimeout, "readTimeout");
        requireNonNegative(retryInitialBackoff, "retryInitialBackoff");
        requireNonNegative(retryMaxBackoff, "retryMaxBackoff");
        if (retryMaxBackoff.compareTo(retryInitialBackoff) < 0) {
            throw new IllegalArgumentException("retryMaxBackoff must be >= retryInitialBackoff");
        }
        serverUri = normalize(serverUri);
    }

    public static PmsClientConfig forEndpoint(String host, int port) {
        Objects.requireNonNull(host, "host must not be null");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
        return builder(URI.create("http://" + host + ":" + port)).build();
    }

    public static PmsClientConfig forUri(URI serverUri) {
        return builder(serverUri).build();
    }

    public static Builder builder(URI serverUri) {
        return new Builder(serverUri);
    }

    private static URI normalize(URI uri) {
        String value = uri.toString();
        if (value.endsWith("/")) {
            return uri;
        }
        return URI.create(value + "/");
    }

    private static void requireHttpUri(URI uri) {
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("serverUri must use http or https: " + uri);
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("serverUri must include host: " + uri);
        }
    }

    private static void requireNonNegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative: " + value);
        }
    }

    private static void requireNonNegative(Duration value, String name) {
        if (value.isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative: " + value);
        }
    }

    public static final class Builder {
        private final URI serverUri;
        private Duration connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private Duration writeTimeout = DEFAULT_WRITE_TIMEOUT;
        private Duration readTimeout = DEFAULT_READ_TIMEOUT;
        private int writeRetryMax = DEFAULT_WRITE_RETRY_MAX;
        private Duration retryInitialBackoff = DEFAULT_RETRY_INITIAL_BACKOFF;
        private Duration retryMaxBackoff = DEFAULT_RETRY_MAX_BACKOFF;
        private boolean requireHttp2 = true;

        private Builder(URI serverUri) {
            this.serverUri = serverUri;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder writeTimeout(Duration writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder writeRetryMax(int writeRetryMax) {
            this.writeRetryMax = writeRetryMax;
            return this;
        }

        public Builder retryInitialBackoff(Duration retryInitialBackoff) {
            this.retryInitialBackoff = retryInitialBackoff;
            return this;
        }

        public Builder retryMaxBackoff(Duration retryMaxBackoff) {
            this.retryMaxBackoff = retryMaxBackoff;
            return this;
        }

        public Builder requireHttp2(boolean requireHttp2) {
            this.requireHttp2 = requireHttp2;
            return this;
        }

        public PmsClientConfig build() {
            return new PmsClientConfig(
                serverUri,
                connectTimeout,
                writeTimeout,
                readTimeout,
                writeRetryMax,
                retryInitialBackoff,
                retryMaxBackoff,
                requireHttp2
            );
        }
    }
}
