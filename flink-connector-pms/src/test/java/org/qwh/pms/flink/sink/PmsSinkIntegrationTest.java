package org.qwh.pms.flink.sink;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.junit.jupiter.api.Test;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;
import org.qwh.pms.flink.PmsFlinkTestUtils;
import org.qwh.pms.flink.TestingPmsServer;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PmsSinkIntegrationTest {

    @Test
    void writesUpsertsAndDeletesThroughRealPmsCodecs() throws Exception {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        try (TestingPmsServer server =
                        new TestingPmsServer(
                                PmsFlinkTestUtils.simplePaimonRowType(),
                                schema.primaryKeyNames());
                TestInitContext context = new TestInitContext()) {
            PmsConnectorConfig config =
                    PmsFlinkTestUtils.connectorConfig(
                            server.endpoint().toString(), true);
            SinkWriter<org.apache.flink.table.data.RowData> writer =
                    new PmsSink(config, schema).createWriter(context);

            writer.write(flinkRow(RowKind.INSERT, 1, "first"), null);
            assertEquals(
                    "first",
                    server.rowForKeyTuple(GenericRow.of(1))
                            .getString(1)
                            .toString());

            writer.write(flinkRow(RowKind.UPDATE_AFTER, 1, "updated"), null);
            assertEquals(
                    "updated",
                    server.rowForKeyTuple(GenericRow.of(1))
                            .getString(1)
                            .toString());

            writer.write(flinkRow(RowKind.DELETE, 1, "ignored"), null);
            assertFalse(server.containsKeyTuple(GenericRow.of(1)));
            assertTrue(server.isDeletedKeyTuple(GenericRow.of(1)));
            assertEquals(3, server.writeRequests());
            writer.close();
        }
    }

    @Test
    void checkpointFlushSendsBufferedRows() throws Exception {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        try (TestingPmsServer server =
                        new TestingPmsServer(
                                PmsFlinkTestUtils.simplePaimonRowType(),
                                schema.primaryKeyNames());
                TestInitContext context = new TestInitContext()) {
            PmsConnectorConfig defaults =
                    PmsFlinkTestUtils.connectorConfig(
                            server.endpoint().toString(), true);
            PmsConnectorConfig buffered =
                    withSinkBatching(defaults, 100, Duration.ofHours(1));
            SinkWriter<org.apache.flink.table.data.RowData> writer =
                    new PmsSink(buffered, schema).createWriter(context);

            GenericRowData reusedRow =
                    flinkRow(RowKind.INSERT, 1, "buffered");
            writer.write(reusedRow, null);
            // Flink 开启 object reuse 后可以立即改写输入对象. Writer 必须在 write() 内完成编码,
            // 不能把 RowData 引用留到 checkpoint flush.
            reusedRow.setField(0, 2);
            reusedRow.setField(1, StringData.fromString("mutated"));
            assertEquals(0, server.writeRequests());

            writer.flush(false);
            assertEquals(1, server.writeRequests());
            assertEquals(
                    "buffered",
                    server.rowForKeyTuple(GenericRow.of(1))
                            .getString(1)
                            .toString());
            writer.close();
        }
    }

    @Test
    void processingTimeTimerFlushesBufferedRowsThroughMailbox() throws Exception {
        PmsFlinkTableSchema schema = PmsFlinkTestUtils.simpleTableSchema();
        try (TestingPmsServer server =
                        new TestingPmsServer(
                                PmsFlinkTestUtils.simplePaimonRowType(),
                                schema.primaryKeyNames());
                TestInitContext context = new TestInitContext()) {
            PmsConnectorConfig defaults =
                    PmsFlinkTestUtils.connectorConfig(
                            server.endpoint().toString(), true);
            PmsConnectorConfig buffered =
                    withSinkBatching(defaults, 100, Duration.ofMillis(50));
            SinkWriter<org.apache.flink.table.data.RowData> writer =
                    new PmsSink(buffered, schema).createWriter(context);

            writer.write(flinkRow(RowKind.INSERT, 1, "timer"), null);
            assertEquals(0, server.writeRequests());

            context.fireTimer();
            assertEquals(1, server.writeRequests());
            assertEquals(
                    "timer",
                    server.rowForKeyTuple(GenericRow.of(1))
                            .getString(1)
                            .toString());
            writer.close();
        }
    }

    private static PmsConnectorConfig withSinkBatching(
            PmsConnectorConfig defaults, int maxRows, Duration flushInterval) {
        return new PmsConnectorConfig(
                defaults.endpoint(),
                defaults.connectTimeout(),
                defaults.writeTimeout(),
                defaults.readTimeout(),
                defaults.requireHttp2(),
                defaults.sinkParallelism(),
                maxRows,
                defaults.sinkBatchMaxBytes(),
                flushInterval,
                defaults.sinkWriteRetryMaxRetries(),
                defaults.sinkWriteRetryInitialBackoff(),
                defaults.sinkWriteRetryMaxBackoff(),
                defaults.lookupAsync(),
                defaults.lookupAsyncThreadNumber(),
                defaults.lookupMaxRetries(),
                defaults.lookupRetryInitialBackoff(),
                defaults.lookupRetryMaxBackoff());
    }

    private static GenericRowData flinkRow(RowKind kind, int id, String marker) {
        GenericRowData row =
                GenericRowData.of(id, StringData.fromString(marker));
        row.setRowKind(kind);
        return row;
    }

    /**
     * SinkWriter 只依赖 mailbox、processing-time service 和 metrics. 测试夹具保持这些
     * 契约真实, 其余 job metadata 不参与被测行为.
     */
    private static final class TestInitContext
            implements Sink.InitContext, AutoCloseable {

        private final SinkWriterMetricGroup metricGroup =
                UnregisteredMetricsGroup.createSinkWriterMetricGroup();
        private final MailboxExecutor mailboxExecutor =
                new MailboxExecutor() {
                    @Override
                    public void execute(
                            MailOptions mailOptions,
                            ThrowingRunnable<? extends Exception> command,
                            String descriptionFormat,
                            Object... descriptionArgs) {
                        try {
                            command.run();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void yield() {}

                    @Override
                    public boolean tryYield() {
                        return false;
                    }

                    @Override
                    public boolean shouldInterrupt() {
                        return false;
                    }
                };
        private final TestProcessingTimeService processingTimeService =
                new TestProcessingTimeService();

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public JobInfo getJobInfo() {
            return null;
        }

        @Override
        public TaskInfo getTaskInfo() {
            return null;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return mailboxExecutor;
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return processingTimeService;
        }

        private void fireTimer() throws Exception {
            processingTimeService.fireTimer();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return null;
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return true;
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return null;
        }

        @Override
        public void close() {}
    }

    private static final class TestProcessingTimeService implements ProcessingTimeService {

        private long timestamp;
        private ProcessingTimeCallback callback;
        private NeverScheduledFuture future;

        @Override
        public long getCurrentProcessingTime() {
            return System.currentTimeMillis();
        }

        @Override
        public ScheduledFuture<?> registerTimer(
                long timestamp, ProcessingTimeCallback callback) {
            this.timestamp = timestamp;
            this.callback = callback;
            future = new NeverScheduledFuture();
            return future;
        }

        private void fireTimer() throws Exception {
            if (callback == null || future == null || future.isCancelled()) {
                throw new IllegalStateException("没有可触发的 processing-time timer.");
            }
            callback.onProcessingTime(timestamp);
        }
    }

    private static final class NeverScheduledFuture implements ScheduledFuture<Object> {

        private boolean cancelled;

        @Override
        public long getDelay(TimeUnit unit) {
            return Long.MAX_VALUE;
        }

        @Override
        public int compareTo(Delayed other) {
            return 1;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }
    }
}
