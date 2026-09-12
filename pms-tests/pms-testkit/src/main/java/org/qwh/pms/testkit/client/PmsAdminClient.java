package org.qwh.pms.testkit.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

public final class PmsAdminClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final URI baseUri;
    private final Duration timeout;
    private final Path lastStatePath;
    private final HttpClient client;

    public PmsAdminClient(URI baseUri, Duration timeout, Path lastStatePath) {
        this.baseUri = Objects.requireNonNull(baseUri, "baseUri must not be null");
        this.timeout = requirePositive(timeout);
        this.lastStatePath = Objects.requireNonNull(lastStatePath, "lastStatePath must not be null");
        this.client = HttpClient.newBuilder()
            .connectTimeout(timeout)
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    private OperationTicket flush() throws IOException {
        return operation("flush");
    }

    private OperationTicket sink() throws IOException {
        return operation("sink");
    }

    public Map<String, Object> flushAndAwait() throws IOException {
        return await(flush());
    }

    public Map<String, Object> sinkAndAwait() throws IOException {
        return await(sink());
    }

    public Map<String, Object> state() throws IOException {
        HttpRequest request = HttpRequest.newBuilder(baseUri.resolve("state"))
            .timeout(timeout)
            .GET()
            .build();
        Map<String, Object> state = sendJson(request, 200);
        writeLastState(state);
        return state;
    }

    private Map<String, Object> await(OperationTicket ticket) throws IOException {
        long deadline = System.nanoTime() + timeout.toNanos();
        Map<String, Object> lastState = Map.of();
        while (System.nanoTime() < deadline) {
            lastState = state();
            if (isComplete(lastState, ticket)) {
                return lastState;
            }
            sleep(Duration.ofMillis(200));
        }
        throw new IOException(
            "Timed out waiting for " + ticket.operation()
                + " fence=" + ticket.fenceSequenceId()
                + ", boundary=" + ticket.completionBoundary()
                + ", lastState=" + lastState
        );
    }

    private OperationTicket operation(String name) throws IOException {
        HttpRequest request = HttpRequest.newBuilder(baseUri.resolve(name))
            .timeout(timeout)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{}", StandardCharsets.UTF_8))
            .build();
        Map<String, Object> body = sendJson(request, 202);
        return parseOperationTicket(name, body);
    }

    static OperationTicket parseOperationTicket(String name, Map<String, Object> body) throws IOException {
        String operation = Objects.toString(body.get("operation"), "");
        String completionBoundary = Objects.toString(body.get("completionBoundary"), "");
        long fence = asNumber(body.get("fenceSequenceId"), "fenceSequenceId").longValue();
        if (!operation.equalsIgnoreCase(name) || completionBoundary.isBlank()) {
            throw new IOException("Invalid PMS " + name + " response: " + body);
        }
        return new OperationTicket(operation, fence, completionBoundary);
    }

    static boolean isComplete(Map<String, Object> state, OperationTicket ticket) throws IOException {
        Number boundary = asNumber(state.get(ticket.completionBoundary()), ticket.completionBoundary());
        return boundary.longValue() >= ticket.fenceSequenceId();
    }

    private Map<String, Object> sendJson(HttpRequest request, int expectedStatus) throws IOException {
        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while calling PMS admin endpoint " + request.uri(), e);
        }
        if (response.statusCode() != expectedStatus) {
            throw new IOException(
                "PMS admin request failed: uri=" + request.uri()
                    + ", status=" + response.statusCode()
                    + ", body=" + response.body()
            );
        }
        try {
            return OBJECT_MAPPER.readValue(response.body(), MAP_TYPE);
        } catch (IOException e) {
            throw new IOException("Invalid JSON from PMS admin endpoint " + request.uri(), e);
        }
    }

    private void writeLastState(Map<String, Object> state) throws IOException {
        Files.createDirectories(lastStatePath.toAbsolutePath().normalize().getParent());
        Files.writeString(
            lastStatePath,
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(state),
            StandardCharsets.UTF_8
        );
    }

    private static Number asNumber(Object value, String field) throws IOException {
        if (value instanceof Number number) {
            return number;
        }
        throw new IOException("PMS admin response field is not numeric: " + field + "=" + value);
    }

    private static Duration requirePositive(Duration value) {
        Objects.requireNonNull(value, "timeout must not be null");
        if (value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException("timeout must be positive: " + value);
        }
        return value;
    }

    private static void sleep(Duration duration) throws IOException {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for PMS admin operation", e);
        }
    }

    record OperationTicket(
            String operation,
            long fenceSequenceId,
            String completionBoundary) {}
}
