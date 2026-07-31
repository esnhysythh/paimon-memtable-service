package org.qwh.pms.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;

import java.io.IOException;

/** 无 writer state 和 committer 的 At-Least-Once PMS Sink V2. */
final class PmsSink implements Sink<RowData> {

    private final PmsConnectorConfig config;
    private final PmsFlinkTableSchema schema;

    PmsSink(PmsConnectorConfig config, PmsFlinkTableSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    /**
     * Flink 1.20 的新 createWriter 方法仍通过这个兼容入口委托, 因此这里必须实现旧签名.
     * 该限制来自 Flink 1.20 API, 不是 PMS 自行保留的兼容层.
     */
    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        return new PmsSinkWriter(config, schema, context);
    }
}
