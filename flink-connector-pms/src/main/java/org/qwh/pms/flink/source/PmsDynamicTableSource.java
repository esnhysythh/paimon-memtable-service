package org.qwh.pms.flink.source;

import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;

/** PMS processing-time Lookup Source. */
public final class PmsDynamicTableSource implements LookupTableSource {

    private final PmsConnectorConfig config;
    private final PmsFlinkTableSchema schema;

    public PmsDynamicTableSource(PmsConnectorConfig config, PmsFlinkTableSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        PmsLookupPlan plan = PmsLookupPlan.create(context.getKeys(), schema);
        if (config.lookupAsync()) {
            return AsyncLookupFunctionProvider.of(
                    new PmsAsyncLookupFunction(config, schema, plan));
        }
        return LookupFunctionProvider.of(new PmsLookupFunction(config, schema, plan));
    }

    @Override
    public PmsDynamicTableSource copy() {
        return new PmsDynamicTableSource(config, schema);
    }

    @Override
    public String asSummaryString() {
        return "PMS Lookup Source";
    }
}
