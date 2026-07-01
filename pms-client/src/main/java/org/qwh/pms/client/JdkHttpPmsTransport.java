package org.qwh.pms.client;

import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.ProtocolNegotiationException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;

final class JdkHttpPmsTransport implements PmsTransport {

    private final PmsClientConfig config;
    private final HttpClient httpClient;

    JdkHttpPmsTransport(PmsClientConfig config) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(config.connectTimeout())
            .proxy(HttpClient.Builder.NO_PROXY)
            .build();
    }

    @Override
    public PmsHandshake handshake() {
        HttpRequest request = request(PmsProtocolConstants.HANDSHAKE_PATH, config.readTimeout())
            .GET()
            .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            requireHttp2(response.version(), PmsProtocolConstants.HANDSHAKE_PATH);
            if (response.statusCode() != 200) {
                throw new ProtocolNegotiationException(
                    "PMS handshake failed with HTTP status " + response.statusCode());
            }
            PmsHandshake handshake = PmsHandshake.fromJson(response.body());
            handshake.requireCompatible();
            return handshake;
        } catch (IOException e) {
            throw new PmsClientException("PMS handshake failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PmsClientException("PMS handshake interrupted", e);
        }
    }

    @Override
    public PmsHttpResponse postBinary(String path, byte[] body, Duration timeout) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(body, "body must not be null");
        HttpRequest request = request(path, timeout)
            .header("content-type", PmsProtocolConstants.CONTENT_TYPE_BINARY)
            .header("accept", PmsProtocolConstants.CONTENT_TYPE_BINARY)
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();
        try {
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            requireHttp2(response.version(), path);
            return new PmsHttpResponse(response.statusCode(), response.body());
        } catch (IOException e) {
            throw new PmsClientException("PMS binary request failed: path=" + path, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PmsClientException("PMS binary request interrupted: path=" + path, e);
        }
    }

    private HttpRequest.Builder request(String path, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        URI uri = config.serverUri().resolve(path);
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).version(HttpClient.Version.HTTP_2);
        if (!timeout.isZero()) {
            builder.timeout(timeout);
        }
        return builder;
    }

    private void requireHttp2(HttpClient.Version version, String path) {
        if (config.requireHttp2() && version != HttpClient.Version.HTTP_2) {
            throw new ProtocolNegotiationException(
                "PMS endpoint requires HTTP/2 but response used " + version + ": path=" + path);
        }
    }
}
