package org.qwh.pms.server.dev;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.qwh.pms.server.ConfigManager;
import org.qwh.pms.server.PmsLocalLookupResult;
import org.qwh.pms.server.PmsServerConfig;
import org.qwh.pms.server.PmsServerRuntime;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

public final class PMSTestServer implements AutoCloseable {
    public static final String DEFAULT_DATABASE = "pms_db";
    public static final String DEFAULT_TABLE = "server_pk";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final Catalog catalog;
    private final Table table;
    private final PmsServerConfig config;

    private PmsServerRuntime runtime;

    private PMSTestServer(PmsServerConfig config, Schema schema) throws Exception {
        this.config = config;
        this.catalog = openCatalog(config);
        Identifier identifier = Identifier.create(config.database(), config.table());
        this.catalog.createDatabase(config.database(), true);
        this.catalog.createTable(identifier, schema, true);
        this.table = catalog.getTable(identifier);
    }

    public static PMSTestServer create(java.nio.file.Path rootDir, Schema schema) throws Exception {
        return new PMSTestServer(config(rootDir, DEFAULT_DATABASE, DEFAULT_TABLE), schema);
    }

    public static PMSTestServer create(PmsServerConfig config, Schema schema) throws Exception {
        return new PMSTestServer(config, schema);
    }

    public PMSTestServer start() throws Exception {
        if (runtime != null) {
            throw new IllegalStateException("PMSTestServer is already started");
        }
        runtime = new PmsServerRuntime(config).start();
        return this;
    }

    public void restart() throws Exception {
        stopRuntime();
        start();
    }

    public void abortAndRestart() throws Exception {
        abortRuntime();
        start();
    }

    public void abortRuntime() throws Exception {
        if (runtime != null) {
            runtime.abort();
            runtime = null;
        }
    }

    public PmsServerRuntime runtime() {
        if (runtime == null) {
            throw new IllegalStateException("PMSTestServer is not started");
        }
        return runtime;
    }

    public URI baseUri() {
        if (runtime == null) {
            throw new IllegalStateException("PMSTestServer HTTP server is not started");
        }
        return URI.create("http://127.0.0.1:" + runtime.port());
    }

    public Table table() {
        return table;
    }

    public PmsServerConfig config() {
        return config;
    }

    public String post(String path, String body) throws Exception {
        return postResult(path, body).requireOk().body();
    }

    public HttpResult postResult(String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(baseUri().resolve(path))
            .header("content-type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();
        HttpResponse<String> response = HttpClient.newHttpClient()
            .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return new HttpResult(response.statusCode(), response.body());
    }

    public String get(String path) throws Exception {
        return getResult(path).requireOk().body();
    }

    public HttpResult getResult(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(baseUri().resolve(path)).GET().build();
        HttpResponse<String> response = HttpClient.newHttpClient()
            .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return new HttpResult(response.statusCode(), response.body());
    }

    public Map<String, Object> postJson(String path, String body) throws Exception {
        return postResult(path, body).requireOk().jsonObject();
    }

    public Map<String, Object> getJson(String path) throws Exception {
        return getResult(path).requireOk().jsonObject();
    }

    public void write(Map<String, Object> row) {
        runtime().write(row);
    }

    public void delete(Map<String, Object> primaryKey) {
        runtime().delete(primaryKey);
    }

    public Optional<Map<String, Object>> get(Map<String, Object> primaryKey) {
        return runtime().get(primaryKey);
    }

    public PmsLocalLookupResult getLocal(Map<String, Object> primaryKey) {
        return runtime().getLocal(primaryKey);
    }

    public List<Map<String, Object>> prefixLocal(Map<String, Object> primaryKeyPrefix) {
        return runtime().prefixLocal(primaryKeyPrefix);
    }

    public void flush() {
        runtime().flush();
    }

    public void sink() {
        runtime().sink();
    }

    public Map<Integer, String> readIntStringRows() throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        Map<Integer, String> rows = new TreeMap<>();
        try (RecordReader<InternalRow> reader =
                 readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            reader.forEachRemaining(row -> rows.put(row.getInt(0), row.getString(1).toString()));
        }
        return rows;
    }

    @Override
    public void close() throws Exception {
        stopRuntime();
        catalog.close();
    }

    private void stopRuntime() throws Exception {
        if (runtime != null) {
            runtime.close();
            runtime = null;
        }
    }

    private static PmsServerConfig config(
            java.nio.file.Path rootDir,
            String database,
            String tableName) {
        Properties props = new Properties();
        props.setProperty("pms.server.host", "127.0.0.1");
        props.setProperty("pms.server.port", "0");
        props.setProperty("pms.paimon.warehouse", rootDir.resolve("warehouse").toUri().toString());
        props.setProperty("pms.paimon.database", database);
        props.setProperty("pms.paimon.table", tableName);
        props.setProperty("pms.wal.dir", rootDir.resolve("wal").toString());
        props.setProperty("pms.storage.dir", rootDir.resolve("storage").toString());
        props.setProperty("pms.memtable.max_entries", "1000000");
        return new ConfigManager().from(props);
    }

    private static Catalog openCatalog(PmsServerConfig config) {
        String warehouse = config.coreConfig().paimon().warehouse();
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalArgumentException("Missing required config key: pms.paimon.warehouse");
        }
        return CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse)));
    }

    public record HttpResult(int statusCode, String body) {
        public HttpResult requireOk() {
            if (statusCode != 200) {
                throw new IllegalStateException(
                    "HTTP request failed with status " + statusCode + ": " + body
                );
            }
            return this;
        }

        public Map<String, Object> jsonObject() throws Exception {
            return OBJECT_MAPPER.readValue(body, MAP_TYPE);
        }
    }
}
