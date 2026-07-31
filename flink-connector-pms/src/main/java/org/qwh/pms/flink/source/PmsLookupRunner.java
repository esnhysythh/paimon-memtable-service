package org.qwh.pms.flink.source;

import org.apache.flink.table.data.RowData;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.client.PmsClientException;
import org.qwh.pms.client.PmsClientProtocolException;
import org.qwh.pms.client.PmsRowLookupResult;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTypeAdapter;
import org.qwh.pms.flink.adapter.PmsFlinkRowData;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/** 单线程拥有的 PMS Client 和一次 Lookup 的 retry/result 语义. */
final class PmsLookupRunner implements AutoCloseable {

    private final PmsConnectorConfig config;
    private final PmsLookupPlan plan;
    private final PmsLookupMetrics metrics;
    private final PmsClient client;

    PmsLookupRunner(
            PmsConnectorConfig config,
            PmsFlinkTableSchema schema,
            PmsLookupPlan plan,
            PmsLookupMetrics metrics) {
        this.config = config;
        this.plan = plan;
        this.metrics = metrics;
        PmsClient openedClient = null;
        try {
            openedClient = PmsClient.connect(config.clientConfig());
            PmsFlinkTypeAdapter.validateServerSchema(
                    schema, openedClient.rowType(), openedClient.primaryKeyFieldNames());
        } catch (RuntimeException e) {
            if (openedClient != null) {
                openedClient.close();
            }
            throw e;
        }
        client = openedClient;
    }

    Collection<RowData> lookup(RowData copiedKey) throws IOException {
        if (plan.hasNullKey(copiedKey)) {
            metrics.recordNullKey();
            return List.of();
        }

        int retries = 0;
        while (true) {
            metrics.recordRequest();
            try {
                PmsRowLookupResult result = client.get(plan.toPrimaryKeyTuple(copiedKey));
                if (result.status() == PmsStatus.OK) {
                    return mapResult(copiedKey, result);
                }
                if (!isRetryable(result.status()) || retries >= config.lookupMaxRetries()) {
                    metrics.recordFailure();
                    throw new IOException(
                            "PMS Lookup 失败: endpoint="
                                    + config.endpoint()
                                    + ", status="
                                    + result.status());
                }
            } catch (PmsClientProtocolException e) {
                metrics.recordFailure();
                throw new IOException(
                        "PMS Lookup 协议错误: endpoint=" + config.endpoint(), e);
            } catch (PmsClientException e) {
                if (retries >= config.lookupMaxRetries()) {
                    metrics.recordFailure();
                    throw new IOException(
                            "PMS Lookup transport 重试耗尽: endpoint="
                                    + config.endpoint(),
                            e);
                }
            }
            sleepBeforeRetry(retries);
            retries++;
            metrics.recordRetry();
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private Collection<RowData> mapResult(RowData copiedKey, PmsRowLookupResult result) {
        if (result.type() == LookupResultType.MISS) {
            metrics.recordMiss();
            return List.of();
        }
        if (result.type() == LookupResultType.DELETED) {
            metrics.recordDeleted();
            return List.of();
        }
        PmsFlinkRowData row = new PmsFlinkRowData(result.row());
        if (!plan.matchesExtraConditions(copiedKey, row)) {
            metrics.recordMiss();
            return List.of();
        }
        metrics.recordHit();
        return List.of(row);
    }

    private void sleepBeforeRetry(int retryIndex) throws IOException {
        Duration delay =
                retryDelay(
                        config.lookupRetryInitialBackoff(),
                        config.lookupRetryMaxBackoff(),
                        retryIndex);
        if (delay.isZero()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("PMS Lookup 重试被中断.", e);
        }
    }

    private static boolean isRetryable(PmsStatus status) {
        return status == PmsStatus.LOOKUP_UNAVAILABLE
                || status == PmsStatus.OVERLOADED
                || status == PmsStatus.SHUTTING_DOWN;
    }

    private static Duration retryDelay(Duration initial, Duration maximum, int retryIndex) {
        long multiplier = 1L << Math.min(retryIndex, 30);
        long delay;
        try {
            delay = Math.multiplyExact(initial.toMillis(), multiplier);
        } catch (ArithmeticException e) {
            delay = Long.MAX_VALUE;
        }
        return Duration.ofMillis(Math.min(delay, maximum.toMillis()));
    }
}
