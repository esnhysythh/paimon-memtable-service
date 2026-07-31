package org.qwh.pms.flink.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.qwh.pms.client.PmsClient;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTypeAdapter;
import org.qwh.pms.flink.adapter.PmsFlinkRowWrapper;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.WriteResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

/** 将 Flink changelog 编码为有序 PMS RawKvEntry batch. */
final class PmsSinkWriter implements SinkWriter<RowData> {

    private static final int MAX_VAR_INT_BYTES = 5;
    private static final int RECORD_LENGTH_BYTES = Integer.BYTES;
    private static final int BATCH_COUNT_MAX_BYTES = MAX_VAR_INT_BYTES;

    private final PmsClient client;
    private final MailboxExecutor mailboxExecutor;
    private final ProcessingTimeService processingTimeService;
    private final String endpoint;
    private final long flushIntervalMillis;
    private final int maxBatchRows;
    private final long maxBatchBytes;
    private final List<RawKvEntry> buffer = new ArrayList<>();
    private final PmsSinkMetrics metrics;

    private long bufferedBytes;
    private long currentSendTimeMillis;
    private volatile long scheduledDeadline = Long.MIN_VALUE;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduledFlush;

    @SuppressWarnings("deprecation")
    PmsSinkWriter(
            PmsConnectorConfig config,
            PmsFlinkTableSchema schema,
            Sink.InitContext context)
            throws IOException {
        endpoint = config.endpoint();
        PmsClient openedClient = null;
        try {
            openedClient = PmsClient.connect(config.clientConfig());
            PmsFlinkTypeAdapter.validateServerSchema(
                    schema, openedClient.rowType(), openedClient.primaryKeyFieldNames());
        } catch (RuntimeException e) {
            if (openedClient != null) {
                openedClient.close();
            }
            throw new IOException(
                    "无法初始化 PMS Sink writer: endpoint=" + endpoint, e);
        }
        client = openedClient;
        mailboxExecutor = context.getMailboxExecutor();
        processingTimeService = context.getProcessingTimeService();
        flushIntervalMillis = config.sinkFlushInterval().toMillis();
        maxBatchRows =
                Math.min(config.sinkBatchMaxRows(), client.handshake().maxBatchEntries());
        maxBatchBytes =
                Math.min(config.sinkBatchMaxBytes(), client.handshake().maxRequestBodyBytes());
        metrics =
                new PmsSinkMetrics(
                        context.metricGroup(),
                        () -> buffer.size(),
                        () -> bufferedBytes,
                        () -> currentSendTimeMillis);
    }

    @Override
    public void write(RowData row, Context context) throws IOException {
        requireOpen();
        RawKvEntry entry = encode(row);
        long entryBytes = estimateEntryBytes(entry);

        // 单条记录允许超过 Connector 的软 batch 阈值, 但仍由 Client 严格检查 Server 上限.
        if (!buffer.isEmpty()
                && BATCH_COUNT_MAX_BYTES + bufferedBytes + entryBytes > maxBatchBytes) {
            flushBuffer();
        }
        buffer.add(entry);
        bufferedBytes += entryBytes;
        scheduleFlushIfNeeded();

        if (buffer.size() >= maxBatchRows
                || BATCH_COUNT_MAX_BYTES + bufferedBytes >= maxBatchBytes
                || flushIntervalMillis == 0) {
            flushBuffer();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        requireOpen();
        flushBuffer();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        // cancellation 路径不能提交 checkpoint 尚未覆盖的数据, close 只释放资源.
        closed = true;
        cancelScheduledFlush();
        client.close();
    }

    private RawKvEntry encode(RowData row) throws IOException {
        try {
            PmsFlinkRowWrapper wrapped = new PmsFlinkRowWrapper(row);
            byte[] key = client.encodeKey(wrapped);
            RowKind rowKind = row.getRowKind();
            if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
                metrics.recordPut();
                return RawKvEntry.put(key, client.encodeRowValue(wrapped));
            }
            if (rowKind == RowKind.DELETE || rowKind == RowKind.UPDATE_BEFORE) {
                metrics.recordDelete();
                return RawKvEntry.delete(key);
            }
            throw new IOException("不支持的 Flink RowKind: " + rowKind);
        } catch (RuntimeException e) {
            throw new IOException("无法将 Flink RowData 编码为 PMS KV.", e);
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer.isEmpty()) {
            cancelScheduledFlush();
            return;
        }
        cancelScheduledFlush();
        metrics.recordFlush();
        long startNanos = System.nanoTime();
        currentSendTimeMillis = 0;
        try {
            WriteResult result = client.rawClient().writeBatchDetailed(List.copyOf(buffer));
            currentSendTimeMillis = elapsedMillis(startNanos);
            if (result.status() != PmsStatus.OK) {
                if (result.status() == PmsStatus.OVERLOADED
                        || result.status() == PmsStatus.SHUTTING_DOWN) {
                    metrics.recordRejected(buffer.size());
                } else {
                    metrics.recordFailure(buffer.size());
                }
                throw new IOException(
                        "PMS 拒绝 Sink batch: endpoint="
                                + endpoint
                                + ", status="
                                + result.status());
            }
            int recordCount = buffer.size();
            long bytes = BATCH_COUNT_MAX_BYTES + bufferedBytes;
            metrics.recordSuccess(recordCount, bytes, currentSendTimeMillis);
            buffer.clear();
            bufferedBytes = 0;
        } catch (IOException e) {
            throw e;
        } catch (RuntimeException e) {
            currentSendTimeMillis = elapsedMillis(startNanos);
            metrics.recordFailure(buffer.size());
            throw new IOException(
                    "PMS Sink batch 写入结果未知, 交由 Flink failover 从 checkpoint 重放:"
                            + " endpoint="
                            + endpoint,
                    e);
        }
    }

    private void scheduleFlushIfNeeded() {
        if (flushIntervalMillis == 0
                || buffer.isEmpty()
                || scheduledDeadline != Long.MIN_VALUE) {
            return;
        }
        long deadline = processingTimeService.getCurrentProcessingTime() + flushIntervalMillis;
        scheduledDeadline = deadline;
        scheduledFlush =
                processingTimeService.registerTimer(
                        deadline,
                        timestamp -> {
                            if (closed || scheduledDeadline != timestamp) {
                                return;
                            }
                            mailboxExecutor.execute(
                                    () -> {
                                        if (!closed && scheduledDeadline == timestamp) {
                                            flushBuffer();
                                        }
                                    },
                                    "Flush PMS Sink batch.");
                        });
    }

    private void cancelScheduledFlush() {
        scheduledDeadline = Long.MIN_VALUE;
        if (scheduledFlush != null) {
            scheduledFlush.cancel(false);
            scheduledFlush = null;
        }
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException("PMS Sink writer 已关闭.");
        }
    }

    private static long estimateEntryBytes(RawKvEntry entry) {
        long rowBytes = entry.isDelete() ? 0 : entry.row().length;
        // 使用 varint 最大宽度形成保守上界, 保证达到阈值时实际 request 不会更大.
        return RECORD_LENGTH_BYTES
                + MAX_VAR_INT_BYTES
                + MAX_VAR_INT_BYTES
                + entry.key().length
                + rowBytes;
    }

    private static long elapsedMillis(long startNanos) {
        return Math.max(0, (System.nanoTime() - startNanos) / 1_000_000);
    }
}
