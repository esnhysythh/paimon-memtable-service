package org.qwh.pms.flink.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;

import java.io.IOException;
import java.util.Collection;

/** 同步 PMS Lookup Function. */
public final class PmsLookupFunction extends LookupFunction {

    private final PmsConnectorConfig config;
    private final PmsFlinkTableSchema schema;
    private final PmsLookupPlan plan;
    private transient PmsLookupMetrics metrics;
    private transient PmsLookupRunner runner;

    PmsLookupFunction(
            PmsConnectorConfig config, PmsFlinkTableSchema schema, PmsLookupPlan plan) {
        this.config = config;
        this.schema = schema;
        this.plan = plan;
    }

    @Override
    public void open(FunctionContext context) {
        metrics = new PmsLookupMetrics(context.getMetricGroup());
        runner = new PmsLookupRunner(config, schema, plan, metrics);
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        requireOpen();
        RowData copiedKey = plan.copyKey(keyRow);
        long startNanos = metrics.beginLookup();
        try {
            return runner.lookup(copiedKey);
        } finally {
            metrics.finishLookup(startNanos);
        }
    }

    @Override
    public void close() {
        if (runner != null) {
            runner.close();
            runner = null;
        }
    }

    private void requireOpen() throws IOException {
        if (runner == null) {
            throw new IOException("PMS Lookup Function 尚未 open 或已经 close.");
        }
    }
}
