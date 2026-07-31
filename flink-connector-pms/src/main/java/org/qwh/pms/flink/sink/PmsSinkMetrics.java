package org.qwh.pms.flink.sink;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.qwh.pms.flink.metrics.PmsSlidingHistogram;

import java.util.function.LongSupplier;

/** PmsSinkWriter 的 Flink metrics 集合. */
final class PmsSinkMetrics {

    private final Counter records;
    private final Counter puts;
    private final Counter deletes;
    private final Counter batches;
    private final Counter batchBytes;
    private final Counter flushes;
    private final Counter writeRejected;
    private final Counter writeFailures;
    private final Histogram writeLatency;
    private final SinkWriterMetricGroup sinkMetricGroup;

    PmsSinkMetrics(
            SinkWriterMetricGroup sinkMetricGroup,
            LongSupplier bufferedRecords,
            LongSupplier bufferedBytes,
            LongSupplier currentSendTime) {
        this.sinkMetricGroup = sinkMetricGroup;
        MetricGroup group = sinkMetricGroup.addGroup("pms");
        records = group.counter("records");
        puts = group.counter("puts");
        deletes = group.counter("deletes");
        batches = group.counter("batches");
        batchBytes = group.counter("batchBytes");
        flushes = group.counter("flushes");
        writeRejected = group.counter("writeRejected");
        writeFailures = group.counter("writeFailures");
        writeLatency = group.histogram("writeLatency", new PmsSlidingHistogram(1024));
        group.gauge("bufferedRecords", bufferedRecords::getAsLong);
        group.gauge("bufferedBytes", bufferedBytes::getAsLong);
        sinkMetricGroup.setCurrentSendTimeGauge(currentSendTime::getAsLong);
    }

    void recordPut() {
        records.inc();
        puts.inc();
    }

    void recordDelete() {
        records.inc();
        deletes.inc();
    }

    void recordFlush() {
        flushes.inc();
    }

    void recordSuccess(int recordCount, long bytes, long latencyMillis) {
        batches.inc();
        batchBytes.inc(bytes);
        writeLatency.update(latencyMillis);
        sinkMetricGroup.getNumRecordsSendCounter().inc(recordCount);
        sinkMetricGroup.getNumBytesSendCounter().inc(bytes);
    }

    void recordRejected(int recordCount) {
        writeRejected.inc();
        recordFailure(recordCount);
    }

    void recordFailure(int recordCount) {
        writeFailures.inc();
        sinkMetricGroup.getNumRecordsSendErrorsCounter().inc(recordCount);
    }
}
