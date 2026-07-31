package org.qwh.pms.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.List;
import java.util.Map;

/** Connector 测试中构造 schema 和配置的最小公共夹具. */
public final class PmsFlinkTestUtils {

    private PmsFlinkTestUtils() {}

    public static RowType simpleFlinkRowType() {
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.INT().notNull()),
                                DataTypes.FIELD("marker", DataTypes.STRING()))
                        .notNull()
                        .getLogicalType();
    }

    public static org.apache.paimon.types.RowType simplePaimonRowType() {
        return org.apache.paimon.types.DataTypes.ROW(
                        org.apache.paimon.types.DataTypes.FIELD(
                                1,
                                "id",
                                org.apache.paimon.types.DataTypes.INT().notNull()),
                        org.apache.paimon.types.DataTypes.FIELD(
                                2,
                                "marker",
                                org.apache.paimon.types.DataTypes.STRING()))
                .notNull();
    }

    public static PmsFlinkTableSchema simpleTableSchema() {
        return tableSchema(simpleFlinkRowType(), List.of("id"));
    }

    public static PmsFlinkTableSchema tableSchema(
            RowType rowType, List<String> primaryKeys) {
        List<Column> columns =
                rowType.getFields().stream()
                        .map(
                                field ->
                                        (Column)
                                                Column.physical(
                                                        field.getName(),
                                                        TypeConversions.fromLogicalToDataType(
                                                                field.getType())))
                        .toList();
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        columns,
                        List.of(),
                        UniqueConstraint.primaryKey("pk", primaryKeys));
        Schema unresolvedSchema =
                Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();
        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(unresolvedSchema)
                        .options(Map.of())
                        .build();
        return PmsFlinkTableSchema.from(
                new ResolvedCatalogTable(catalogTable, resolvedSchema));
    }

    public static PmsConnectorConfig connectorConfig(
            String endpoint, boolean asyncLookup) {
        Configuration options = new Configuration();
        options.set(PmsConnectorOptions.ENDPOINT, endpoint);
        options.set(PmsConnectorOptions.CLIENT_REQUIRE_HTTP2, false);
        options.set(PmsConnectorOptions.SINK_FLUSH_INTERVAL, java.time.Duration.ZERO);
        options.set(PmsConnectorOptions.LOOKUP_ASYNC, asyncLookup);
        options.set(PmsConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER, 2);
        options.set(PmsConnectorOptions.LOOKUP_RETRY_INITIAL_BACKOFF, java.time.Duration.ZERO);
        options.set(PmsConnectorOptions.LOOKUP_RETRY_MAX_BACKOFF, java.time.Duration.ZERO);
        return PmsConnectorConfig.from(options);
    }
}
