package org.qwh.pms.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.qwh.pms.flink.sink.PmsDynamicTableSink;
import org.qwh.pms.flink.source.PmsDynamicTableSource;

import java.util.Set;

/** `connector = pms` 的 Flink 1.20 DynamicTable factory. */
public final class PmsDynamicTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "pms";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        PmsConnectorConfig config = PmsConnectorConfig.from(helper.getOptions());
        PmsFlinkTableSchema schema = PmsFlinkTableSchema.from(context.getCatalogTable());
        return new PmsDynamicTableSource(config, schema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        PmsConnectorConfig config = PmsConnectorConfig.from(helper.getOptions());
        PmsFlinkTableSchema schema = PmsFlinkTableSchema.from(context.getCatalogTable());
        return new PmsDynamicTableSink(config, schema);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(PmsConnectorOptions.ENDPOINT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                PmsConnectorOptions.CLIENT_CONNECT_TIMEOUT,
                PmsConnectorOptions.CLIENT_WRITE_TIMEOUT,
                PmsConnectorOptions.CLIENT_READ_TIMEOUT,
                PmsConnectorOptions.CLIENT_REQUIRE_HTTP2,
                PmsConnectorOptions.SINK_PARALLELISM,
                PmsConnectorOptions.SINK_BATCH_MAX_ROWS,
                PmsConnectorOptions.SINK_BATCH_MAX_BYTES,
                PmsConnectorOptions.SINK_FLUSH_INTERVAL,
                PmsConnectorOptions.SINK_WRITE_RETRY_MAX_RETRIES,
                PmsConnectorOptions.SINK_WRITE_RETRY_INITIAL_BACKOFF,
                PmsConnectorOptions.SINK_WRITE_RETRY_MAX_BACKOFF,
                PmsConnectorOptions.LOOKUP_ASYNC,
                PmsConnectorOptions.LOOKUP_ASYNC_THREAD_NUMBER,
                PmsConnectorOptions.LOOKUP_MAX_RETRIES,
                PmsConnectorOptions.LOOKUP_RETRY_INITIAL_BACKOFF,
                PmsConnectorOptions.LOOKUP_RETRY_MAX_BACKOFF);
    }
}
