package org.qwh.pms.flink.sink;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTypeAdapter;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.WriteResult;

import java.util.List;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** PMS latest-state DynamicTable Sink. */
public final class PmsDynamicTableSink implements DynamicTableSink, SupportsDeletePushDown {

    private final PmsConnectorConfig config;
    private final PmsFlinkTableSchema schema;
    private PmsDeletePlan deletePlan;

    public PmsDynamicTableSink(PmsConnectorConfig config, PmsFlinkTableSchema schema) {
        this(config, schema, null);
    }

    private PmsDynamicTableSink(
            PmsConnectorConfig config,
            PmsFlinkTableSchema schema,
            PmsDeletePlan deletePlan) {
        this.config = config;
        this.schema = schema;
        this.deletePlan = deletePlan;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        validateFullTargetColumns(context);
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                PmsPrimaryKeySelector keySelector =
                        new PmsPrimaryKeySelector(
                                schema.rowType(),
                                schema.primaryKeyIndexes(),
                                schema.primaryKeyRowType());
                DataStream<RowData> keyed =
                        dataStream.keyBy(
                                keySelector, InternalTypeInfo.of(schema.primaryKeyRowType()));
                DataStreamSink<RowData> sink = keyed.sinkTo(new PmsSink(config, schema));
                sink.name("PMS Sink");
                if (config.sinkParallelism() != null) {
                    sink.setParallelism(config.sinkParallelism());
                }
                return sink;
            }

            @Override
            public Optional<Integer> getParallelism() {
                return Optional.ofNullable(config.sinkParallelism());
            }
        };
    }

    @Override
    public PmsDynamicTableSink copy() {
        return new PmsDynamicTableSink(config, schema, deletePlan);
    }

    @Override
    public String asSummaryString() {
        return "PMS Sink";
    }

    @Override
    public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
        Optional<PmsDeletePlan> accepted = PmsDeletePlan.tryCreate(filters, schema);
        deletePlan = accepted.orElse(null);
        return accepted.isPresent();
    }

    @Override
    public Optional<Long> executeDeletion() {
        if (deletePlan == null) {
            throw new IllegalStateException(
                    "Flink 未成功下推完整 primary key DELETE, 不应执行 PMS deletion.");
        }

        // DELETE pushdown 直接编码主键并调用 PMS, 不读取 Paimon 或 PMS 当前值.
        try (PmsClient client = PmsClient.connect(config.clientConfig())) {
            PmsFlinkTypeAdapter.validateServerSchema(
                    schema, client.rowType(), client.primaryKeyFieldNames());
            WriteResult result = client.delete(deletePlan.toPrimaryKeyTuple(schema));
            if (result.status() != PmsStatus.OK) {
                throw new TableException(
                        "PMS 拒绝 DELETE pushdown: endpoint="
                                + config.endpoint()
                                + ", status="
                                + result.status());
            }
            // PMS 的 latest-state delete 不承诺目标此前存在, 因此无法返回精确删除行数.
            return Optional.empty();
        } catch (TableException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new TableException(
                    "执行 PMS DELETE pushdown 失败: endpoint=" + config.endpoint(), e);
        }
    }

    private void validateFullTargetColumns(Context context) {
        Optional<int[][]> targetColumns = context.getTargetColumns();
        if (targetColumns.isEmpty()) {
            return;
        }
        int[][] paths = targetColumns.get();
        Set<Integer> topLevelFields = new HashSet<>();
        for (int[] path : paths) {
            if (path.length != 1) {
                throw new ValidationException("PMS Sink 不支持 nested target column.");
            }
            topLevelFields.add(path[0]);
        }
        if (topLevelFields.size() != schema.rowType().getFieldCount()) {
            throw new ValidationException("PMS Sink 要求写入完整物理行, 不支持 partial upsert.");
        }
    }
}
