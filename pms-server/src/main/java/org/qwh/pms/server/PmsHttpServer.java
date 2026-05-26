package org.qwh.pms.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PmsHttpServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsHttpServer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final PmsServerRuntime runtime;
    private final HttpServer server;
    private final ExecutorService executor;

    public PmsHttpServer(PmsServerConfig config, PmsServerRuntime runtime) throws IOException {
        this.runtime = runtime;
        this.server = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
        this.executor = Executors.newCachedThreadPool();
        this.server.setExecutor(executor);
        registerRoutes();
    }

    public void start() {
        server.start();
        LOG.info("PMS HTTP server started on {}", server.getAddress());
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        LOG.info("Stopping PMS HTTP server");
        server.stop(0);
        executor.shutdownNow();
        LOG.info("PMS HTTP server stopped");
    }

    private void registerRoutes() {
        server.createContext("/health", exchange -> handle(exchange, "GET", () -> ok(Map.of("status", "OK"))));
        server.createContext("/write", exchange -> handle(exchange, "POST", () -> {
            runtime.write(requestObject(exchange));
            return ok(Map.of("status", "OK"));
        }));
        server.createContext("/delete", exchange -> handle(exchange, "POST", () -> {
            runtime.delete(requestObject(exchange));
            return ok(Map.of("status", "OK"));
        }));
        server.createContext("/get", exchange -> handle(exchange, "POST", () -> {
            Map<String, Object> response = new LinkedHashMap<>();
            var row = runtime.get(requestObject(exchange));
            response.put("status", "OK");
            response.put("found", row.isPresent());
            response.put("row", row.orElse(null));
            return ok(response);
        }));
        server.createContext("/prefix", exchange -> handle(exchange, "POST", () -> {
            Map<String, Object> response = new LinkedHashMap<>();
            List<Map<String, Object>> rows = runtime.prefixScan(requestObject(exchange));
            response.put("status", "OK");
            response.put("count", rows.size());
            response.put("rows", rows);
            return ok(response);
        }));
        server.createContext("/flush", exchange -> handle(exchange, "POST", () -> {
            runtime.flush();
            return ok(Map.of("status", "OK"));
        }));
        server.createContext("/sink", exchange -> handle(exchange, "POST", () -> {
            runtime.sink();
            return ok(Map.of("status", "OK"));
        }));
        server.createContext("/state", exchange -> handle(exchange, "GET", () -> ok(runtime.state())));
    }

    private Map<String, Object> requestObject(HttpExchange exchange) throws IOException {
        try (InputStream input = exchange.getRequestBody()) {
            return OBJECT_MAPPER.readValue(input, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Request body must be a JSON object", e);
        }
    }

    private static Response ok(Map<String, Object> body) {
        return new Response(200, body);
    }

    private static void handle(HttpExchange exchange, String expectedMethod, Handler handler) throws IOException {
        try {
            if (!exchange.getRequestMethod().equalsIgnoreCase(expectedMethod)) {
                send(exchange, new Response(405, Map.of("status", "METHOD_NOT_ALLOWED")));
                return;
            }
            send(exchange, handler.handle());
        } catch (PmsServiceUnavailableException e) {
            LOG.warn("PMS HTTP service unavailable: method={}, path={}, message={}", exchange.getRequestMethod(), exchange.getRequestURI(), e.getMessage());
            send(exchange, new Response(503, Map.of("status", "UNAVAILABLE", "message", e.getMessage())));
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
            LOG.warn("Bad PMS HTTP request: method={}, path={}, message={}", exchange.getRequestMethod(), exchange.getRequestURI(), e.getMessage());
            send(exchange, new Response(400, Map.of("status", "BAD_REQUEST", "message", e.getMessage())));
        } catch (Exception e) {
            LOG.error("PMS HTTP request failed: method={}, path={}", exchange.getRequestMethod(), exchange.getRequestURI(), e);
            send(exchange, new Response(500, Map.of("status", "ERROR", "message", e.getMessage())));
        }
    }

    private static void send(HttpExchange exchange, Response response) throws IOException {
        byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(response.body());
        exchange.getResponseHeaders().set("content-type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(response.statusCode(), bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    @FunctionalInterface
    private interface Handler {
        Response handle() throws Exception;
    }

    private record Response(int statusCode, Map<String, Object> body) {}
}
