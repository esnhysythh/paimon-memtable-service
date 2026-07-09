package org.qwh.pms.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.qwh.pms.core.bucket.PmsFatalWriteException;
import org.qwh.pms.protocol.api.PmsHandshake;
import org.qwh.pms.protocol.api.PmsProtocolConstants;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawKvStore;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.codec.KeyBatchCodec;
import org.qwh.pms.protocol.codec.LookupBatchCodec;
import org.qwh.pms.protocol.codec.RecordBatchCodec;
import org.qwh.pms.protocol.codec.WriteResultCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PmsHttpServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsHttpServer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final PmsServerRuntime runtime;
    private final RawKvStore rawStore;
    private final Server server;
    private final ServerConnector connector;

    public PmsHttpServer(PmsServerConfig config, PmsServerRuntime runtime) {
        this.runtime = runtime;
        this.rawStore = new PmsRuntimeRawKvStore(runtime);
        this.server = new Server();

        HttpConfiguration httpConfig = new HttpConfiguration();
        HttpConnectionFactory http1 = new HttpConnectionFactory(httpConfig);
        HTTP2CServerConnectionFactory h2c = new HTTP2CServerConnectionFactory(httpConfig);
        h2c.setMaxConcurrentStreams(config.protocol().maxConcurrentStreams());
        this.connector = new ServerConnector(server, http1, h2c);
        this.connector.setHost(config.host());
        this.connector.setPort(config.port());
        this.server.addConnector(connector);
        this.server.setHandler(new PmsHandler(config.protocol()));
    }

    public void start() throws Exception {
        server.start();
        LOG.info("PMS HTTP/2 server started on {}:{}", connector.getHost(), connector.getLocalPort());
    }

    public int port() {
        return connector.getLocalPort();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping PMS HTTP/2 server");
        server.stop();
        LOG.info("PMS HTTP/2 server stopped");
    }

    private final class PmsHandler extends Handler.Abstract {
        private final PmsProtocolConfig protocolConfig;

        private PmsHandler(PmsProtocolConfig protocolConfig) {
            this.protocolConfig = protocolConfig;
        }

        @Override
        public boolean handle(Request request, Response response, Callback callback) {
            String path = request.getHttpURI().getPath();
            if (path == null) {
                return false;
            }
            if (path.startsWith(PmsProtocolConstants.API_PREFIX)) {
                handleProtocol(request, response, callback, path);
                return true;
            }
            return handleJsonDebug(request, response, callback, path);
        }

        private void handleProtocol(Request request, Response response, Callback callback, String path) {
            try {
                if (protocolConfig.strictHttp2()
                        && request.getConnectionMetaData().getHttpVersion() != HttpVersion.HTTP_2) {
                    writeBytes(
                        response,
                        HttpStatus.HTTP_VERSION_NOT_SUPPORTED_505,
                        "text/plain; charset=utf-8",
                        "HTTP/2 is required".getBytes(StandardCharsets.UTF_8),
                        callback
                    );
                    return;
                }
                switch (path) {
                    case PmsProtocolConstants.API_PREFIX + "/health" -> handleProtocolHealth(request, response, callback);
                    case PmsProtocolConstants.HANDSHAKE_PATH -> handleHandshake(request, response, callback);
                    case PmsProtocolConstants.LOCAL_PUT_PATH -> handlePut(request, response, callback);
                    case PmsProtocolConstants.LOCAL_DELETE_PATH -> handleDelete(request, response, callback);
                    case PmsProtocolConstants.LOCAL_WRITE_BATCH_PATH -> handleWriteBatch(request, response, callback);
                    case PmsProtocolConstants.LOCAL_GET_PATH -> handleGetLocal(request, response, callback);
                    case PmsProtocolConstants.FULL_GET_PATH -> handleGetFull(request, response, callback);
                    case PmsProtocolConstants.LOCAL_GET_PREFIX_PATH -> handleGetPrefixLocal(request, response, callback);
                    default -> writeStatus(response, HttpStatus.NOT_FOUND_404, PmsStatus.BAD_REQUEST, callback);
                }
            } catch (ResponseTooLargeException e) {
                writeErrorStatus(path, response, HttpStatus.SERVICE_UNAVAILABLE_503, PmsStatus.OVERLOADED, callback);
            } catch (PmsFatalWriteException e) {
                LOG.error("Fatal PMS write failure: method={}, path={}", request.getMethod(), path, e);
                callback.failed(e);
                runtime.failFatalAsync(e);
            } catch (IllegalArgumentException | IOException e) {
                LOG.warn("Bad PMS protocol request: method={}, path={}, message={}", request.getMethod(), path, e.getMessage());
                writeErrorStatus(path, response, HttpStatus.BAD_REQUEST_400, PmsStatus.BAD_REQUEST, callback);
            } catch (Exception e) {
                LOG.error("PMS protocol request failed: method={}, path={}", request.getMethod(), path, e);
                writeErrorStatus(path, response, HttpStatus.INTERNAL_SERVER_ERROR_500, PmsStatus.INTERNAL_ERROR, callback);
            }
        }

        private boolean handleJsonDebug(Request request, Response response, Callback callback, String path) {
            try {
                switch (path) {
                    case "/health" -> handleJson(request, response, callback, "GET", () -> ok(Map.of("status", "OK")));
                    case "/write" -> handleJson(request, response, callback, "POST", () -> {
                        runtime.write(requestObject(request));
                        return ok(Map.of("status", "OK"));
                    });
                    case "/delete" -> handleJson(request, response, callback, "POST", () -> {
                        runtime.delete(requestObject(request));
                        return ok(Map.of("status", "OK"));
                    });
                    case "/get" -> handleJson(request, response, callback, "POST", () -> {
                        Map<String, Object> responseBody = new LinkedHashMap<>();
                        var row = runtime.get(requestObject(request));
                        responseBody.put("status", "OK");
                        responseBody.put("found", row.isPresent());
                        responseBody.put("row", row.orElse(null));
                        return ok(responseBody);
                    });
                    case "/getLocal" -> handleJson(request, response, callback, "POST", () -> {
                        PmsLocalLookupResult result = runtime.getLocal(requestObject(request));
                        Map<String, Object> responseBody = new LinkedHashMap<>();
                        responseBody.put("status", "OK");
                        responseBody.put("result", result.type().name());
                        responseBody.put("found", result.found());
                        responseBody.put("source", "PMS_LOCAL");
                        responseBody.put("row", result.row());
                        return ok(responseBody);
                    });
                    case "/prefix" -> handleJson(request, response, callback, "POST", () -> {
                        requestObject(request);
                        throw new PmsNotSupportedException(
                            "Full prefix lookup is not supported yet; use /prefixLocal for PMS-local prefix lookup"
                        );
                    });
                    case "/prefixLocal" -> handleJson(request, response, callback, "POST", () -> {
                        Map<String, Object> responseBody = new LinkedHashMap<>();
                        List<Map<String, Object>> rows = runtime.prefixLocal(requestObject(request));
                        responseBody.put("status", "OK");
                        responseBody.put("count", rows.size());
                        responseBody.put("source", "PMS_LOCAL");
                        responseBody.put("rows", rows);
                        return ok(responseBody);
                    });
                    case "/flush" -> handleJson(request, response, callback, "POST", () -> {
                        runtime.flush();
                        return ok(Map.of("status", "OK"));
                    });
                    case "/sink" -> handleJson(request, response, callback, "POST", () -> {
                        runtime.sink();
                        return ok(Map.of("status", "OK"));
                    });
                    case "/state" -> handleJson(request, response, callback, "GET", () -> ok(runtime.state()));
                    default -> {
                        return false;
                    }
                }
            } catch (PmsFatalWriteException e) {
                LOG.error("Fatal PMS JSON debug write failure: method={}, path={}", request.getMethod(), path, e);
                callback.failed(e);
                runtime.failFatalAsync(e);
            } catch (Exception e) {
                LOG.error("PMS JSON debug dispatch failed: method={}, path={}", request.getMethod(), path, e);
                writeJson(response, HttpStatus.INTERNAL_SERVER_ERROR_500, Map.of("status", "ERROR", "message", e.getMessage()), callback);
            }
            return true;
        }

        private void handleProtocolHealth(Request request, Response response, Callback callback) {
            if (!requireMethod(request, response, callback, "GET")) {
                return;
            }
            writeJsonBytes(response, HttpStatus.OK_200, "{\"status\":\"OK\"}".getBytes(StandardCharsets.UTF_8), callback);
        }

        private void handleHandshake(Request request, Response response, Callback callback) {
            if (!requireMethod(request, response, callback, "GET")) {
                return;
            }
            List<String> capabilities = new ArrayList<>(PmsProtocolConstants.REQUIRED_HOT_PATH_CAPABILITIES);
            if (protocolConfig.strictHttp2()) {
                capabilities.add(PmsProtocolConstants.CAPABILITY_STRICT_HTTP2);
            }
            PmsHandshake handshake = new PmsHandshake(
                PmsProtocolConstants.PROTOCOL_NAME,
                PmsProtocolConstants.PROTOCOL_VERSION,
                PmsProtocolConstants.REQUIRED_HTTP_VERSION,
                "pms",
                protocolConfig.maxKeyBytes(),
                protocolConfig.maxRowBytes(),
                protocolConfig.maxBatchEntries(),
                protocolConfig.maxConcurrentStreams(),
                protocolConfig.maxRequestBodyBytes(),
                protocolConfig.maxResponseBodyBytes(),
                runtime.protocolTableSchema(),
                capabilities
            );
            writeJsonBytes(response, HttpStatus.OK_200, handshake.toJson().getBytes(StandardCharsets.UTF_8), callback);
        }

        private void handlePut(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            RawKvEntry entry = requireSingleRecord(decodeRecordBatch(readBinaryBody(request), 1));
            if (entry.isDelete()) {
                throw new IllegalArgumentException("local/put requires PUT record");
            }
            PmsStatus status = rawStore.put(entry.key(), entry.row());
            writeWriteResult(response, status, status == PmsStatus.OK ? 1 : 0, callback);
        }

        private void handleDelete(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            RawKvEntry entry = requireSingleRecord(decodeRecordBatch(readBinaryBody(request), 1));
            if (!entry.isDelete()) {
                throw new IllegalArgumentException("local/delete requires DELETE record");
            }
            PmsStatus status = rawStore.delete(entry.key());
            writeWriteResult(response, status, status == PmsStatus.OK ? 1 : 0, callback);
        }

        private void handleWriteBatch(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            List<RawKvEntry> entries = decodeRecordBatch(readBinaryBody(request), protocolConfig.maxBatchEntries());
            PmsStatus status = rawStore.writeBatch(entries);
            writeWriteResult(response, status, status == PmsStatus.OK ? entries.size() : 0, callback);
        }

        private void handleGetLocal(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            byte[] key = requireSingleKey(decodeKeyBatch(readBinaryBody(request), 1));
            requireNonEmptyKey(key);
            writeLookupResult(response, RawLookupBatchResult.single(rawStore.getLocal(key)), callback);
        }

        private void handleGetFull(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            byte[] key = requireSingleKey(decodeKeyBatch(readBinaryBody(request), 1));
            requireNonEmptyKey(key);
            writeLookupResult(response, RawLookupBatchResult.single(rawStore.getFull(key)), callback);
        }

        private void handleGetPrefixLocal(Request request, Response response, Callback callback) throws Exception {
            if (!requireMethod(request, response, callback, "POST")) {
                return;
            }
            byte[] prefix = requireSingleKey(decodeKeyBatch(readBinaryBody(request), 1));
            writeLookupResult(response, rawStore.getPrefixLocal(prefix), callback);
        }

        private void handleJson(Request request, Response response, Callback callback, String expectedMethod, JsonHandler handler) {
            try {
                if (!request.getMethod().equalsIgnoreCase(expectedMethod)) {
                    writeJson(response, HttpStatus.METHOD_NOT_ALLOWED_405, Map.of("status", "METHOD_NOT_ALLOWED"), callback);
                    return;
                }
                writeJson(response, handler.handle(), callback);
            } catch (PmsFatalWriteException e) {
                throw e;
            } catch (PmsOverloadedException e) {
                LOG.warn("PMS JSON debug write overloaded: method={}, path={}", request.getMethod(), request.getHttpURI());
                writeJson(response, HttpStatus.SERVICE_UNAVAILABLE_503, Map.of("status", "OVERLOADED"), callback);
            } catch (PmsLookupUnavailableException e) {
                LOG.warn("PMS JSON debug lookup unavailable: method={}, path={}, message={}", request.getMethod(), request.getHttpURI(), e.getMessage());
                writeJson(response, HttpStatus.SERVICE_UNAVAILABLE_503, Map.of("status", "LOOKUP_UNAVAILABLE", "message", e.getMessage()), callback);
            } catch (PmsServiceUnavailableException e) {
                LOG.warn("PMS JSON debug service unavailable: method={}, path={}, message={}", request.getMethod(), request.getHttpURI(), e.getMessage());
                writeJson(response, HttpStatus.SERVICE_UNAVAILABLE_503, Map.of("status", "UNAVAILABLE", "message", e.getMessage()), callback);
            } catch (PmsNotSupportedException e) {
                LOG.warn("Unsupported PMS JSON debug request: method={}, path={}, message={}", request.getMethod(), request.getHttpURI(), e.getMessage());
                writeJson(response, HttpStatus.NOT_IMPLEMENTED_501, Map.of("status", "NOT_SUPPORTED", "message", e.getMessage()), callback);
            } catch (IllegalArgumentException | UnsupportedOperationException | IOException e) {
                LOG.warn("Bad PMS JSON debug request: method={}, path={}, message={}", request.getMethod(), request.getHttpURI(), e.getMessage());
                writeJson(response, HttpStatus.BAD_REQUEST_400, Map.of("status", "BAD_REQUEST", "message", e.getMessage()), callback);
            } catch (Exception e) {
                LOG.error("PMS JSON debug request failed: method={}, path={}", request.getMethod(), request.getHttpURI(), e);
                writeJson(response, HttpStatus.INTERNAL_SERVER_ERROR_500, Map.of("status", "ERROR", "message", e.getMessage()), callback);
            }
        }

        private Map<String, Object> requestObject(Request request) throws Exception {
            try {
                return OBJECT_MAPPER.readValue(readBody(request), MAP_TYPE);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Request body must be a JSON object", e);
            }
        }

        private byte[] readBinaryBody(Request request) throws Exception {
            requireBinaryContentType(request);
            return readBody(request);
        }

        private byte[] readBody(Request request) throws Exception {
            long contentLength = request.getHeaders().getLongField(HttpHeader.CONTENT_LENGTH);
            if (contentLength > protocolConfig.maxRequestBodyBytes()) {
                throw new IllegalArgumentException("request body too large: " + contentLength);
            }
            try (InputStream input = Content.Source.asInputStream(request)) {
                return readLimited(input, protocolConfig.maxRequestBodyBytes());
            }
        }

        private List<RawKvEntry> decodeRecordBatch(byte[] body, int maxEntries) {
            return RecordBatchCodec.decodeRequest(
                body,
                maxEntries,
                protocolConfig.maxKeyBytes(),
                protocolConfig.maxRowBytes()
            );
        }

        private List<byte[]> decodeKeyBatch(byte[] body, int maxKeys) {
            return KeyBatchCodec.decodeRequest(body, maxKeys, protocolConfig.maxKeyBytes());
        }

        private boolean requireMethod(Request request, Response response, Callback callback, String expected) {
            if (expected.equals(request.getMethod())) {
                return true;
            }
            writeErrorStatus(
                request.getHttpURI().getPath(),
                response,
                HttpStatus.METHOD_NOT_ALLOWED_405,
                PmsStatus.BAD_REQUEST,
                callback
            );
            return false;
        }

        private RawKvEntry requireSingleRecord(List<RawKvEntry> entries) {
            if (entries.size() != 1) {
                throw new IllegalArgumentException("endpoint requires exactly one record: " + entries.size());
            }
            return entries.get(0);
        }

        private byte[] requireSingleKey(List<byte[]> keys) {
            if (keys.size() != 1) {
                throw new IllegalArgumentException("endpoint requires exactly one key: " + keys.size());
            }
            return keys.get(0);
        }

        private void requireNonEmptyKey(byte[] key) {
            if (key.length == 0) {
                throw new IllegalArgumentException("key must not be empty");
            }
        }

        private void requireBinaryContentType(Request request) {
            String contentType = request.getHeaders().get(HttpHeader.CONTENT_TYPE);
            if (contentType == null || !isContentType(contentType, PmsProtocolConstants.CONTENT_TYPE_BINARY)) {
                throw new IllegalArgumentException("binary endpoint requires content-type " + PmsProtocolConstants.CONTENT_TYPE_BINARY);
            }
        }

        private void writeWriteResult(Response response, PmsStatus status, int acceptedCount, Callback callback) {
            writeBytes(
                response,
                httpStatus(status),
                PmsProtocolConstants.CONTENT_TYPE_BINARY,
                WriteResultCodec.encodeResponse(status, acceptedCount),
                callback
            );
        }

        private void writeLookupResult(Response response, RawLookupBatchResult result, Callback callback) {
            writeBytes(
                response,
                httpStatus(result.status()),
                PmsProtocolConstants.CONTENT_TYPE_BINARY,
                LookupBatchCodec.encodeResponse(result),
                callback
            );
        }

        private void writeStatus(Response response, int httpStatus, PmsStatus status, Callback callback) {
            writeBytes(
                response,
                httpStatus,
                PmsProtocolConstants.CONTENT_TYPE_BINARY,
                WriteResultCodec.encodeResponse(status, 0),
                callback
            );
        }

        private void writeErrorStatus(String path, Response response, int httpStatus, PmsStatus status, Callback callback) {
            byte[] body = isQueryPath(path)
                ? LookupBatchCodec.encodeResponse(RawLookupBatchResult.failed(status))
                : WriteResultCodec.encodeResponse(status, 0);
            writeBytes(response, httpStatus, PmsProtocolConstants.CONTENT_TYPE_BINARY, body, callback);
        }

        private boolean isQueryPath(String path) {
            return path.equals(PmsProtocolConstants.LOCAL_GET_PATH)
                || path.equals(PmsProtocolConstants.FULL_GET_PATH)
                || path.equals(PmsProtocolConstants.LOCAL_GET_PREFIX_PATH);
        }

        private int httpStatus(PmsStatus status) {
            return switch (status) {
                case OK -> HttpStatus.OK_200;
                case BAD_REQUEST, SCHEMA_MISMATCH -> HttpStatus.BAD_REQUEST_400;
                case OVERLOADED, SHUTTING_DOWN, LOOKUP_UNAVAILABLE -> HttpStatus.SERVICE_UNAVAILABLE_503;
                case INTERNAL_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR_500;
            };
        }

        private void writeJson(Response response, ResponseBody body, Callback callback) {
            writeJson(response, body.statusCode(), body.body(), callback);
        }

        private void writeJson(Response response, int httpStatus, Map<String, Object> body, Callback callback) {
            try {
                writeJsonBytes(response, httpStatus, OBJECT_MAPPER.writeValueAsBytes(body), callback);
            } catch (JsonProcessingException e) {
                writeBytes(
                    response,
                    HttpStatus.INTERNAL_SERVER_ERROR_500,
                    "text/plain; charset=utf-8",
                    "JSON response encoding failed".getBytes(StandardCharsets.UTF_8),
                    callback
                );
            }
        }

        private void writeJsonBytes(Response response, int httpStatus, byte[] bytes, Callback callback) {
            writeBytes(response, httpStatus, "application/json; charset=utf-8", bytes, callback);
        }

        private void writeBytes(Response response, int httpStatus, String contentType, byte[] bytes, Callback callback) {
            if (httpStatus < 400
                    && isContentType(contentType, PmsProtocolConstants.CONTENT_TYPE_BINARY)
                    && bytes.length > protocolConfig.maxResponseBodyBytes()) {
                throw new ResponseTooLargeException(bytes.length, protocolConfig.maxResponseBodyBytes());
            }
            response.setStatus(httpStatus);
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
            response.write(true, ByteBuffer.wrap(bytes), callback);
        }

        private byte[] readLimited(InputStream input, int maxBytes) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream(Math.min(maxBytes, 8192));
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if ((long) output.size() + read > maxBytes) {
                    throw new IllegalArgumentException("request body too large: > " + maxBytes);
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static ResponseBody ok(Map<String, Object> body) {
        return new ResponseBody(HttpStatus.OK_200, body);
    }

    private static boolean isContentType(String actual, String expected) {
        int semicolon = actual.indexOf(';');
        String mediaType = semicolon >= 0 ? actual.substring(0, semicolon) : actual;
        return expected.equalsIgnoreCase(mediaType.trim());
    }

    @FunctionalInterface
    private interface JsonHandler {
        ResponseBody handle() throws Exception;
    }

    private record ResponseBody(int statusCode, Map<String, Object> body) {}

    private static final class ResponseTooLargeException extends RuntimeException {
        private ResponseTooLargeException(int actualBytes, int maxBytes) {
            super("response body too large: " + actualBytes + " > " + maxBytes);
        }
    }
}
