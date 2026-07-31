package org.qwh.pms.flink.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.Arrays;

/**
 * 固定窗口的轻量 latency histogram.
 *
 * <p>这里不依赖 Flink runtime 的 internal histogram, 避免 Connector 锁定非公共 API.
 */
public final class PmsSlidingHistogram implements Histogram {

    private final long[] values;
    private long count;
    private int size;
    private int next;

    public PmsSlidingHistogram(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive.");
        }
        this.values = new long[windowSize];
    }

    @Override
    public synchronized void update(long value) {
        values[next] = value;
        next = (next + 1) % values.length;
        if (size < values.length) {
            size++;
        }
        count++;
    }

    @Override
    public synchronized long getCount() {
        return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        long[] snapshot = Arrays.copyOf(values, size);
        Arrays.sort(snapshot);
        return new Snapshot(snapshot);
    }

    private static final class Snapshot extends HistogramStatistics {

        private final long[] sorted;

        private Snapshot(long[] sorted) {
            this.sorted = sorted;
        }

        @Override
        public double getQuantile(double quantile) {
            if (quantile < 0 || quantile > 1) {
                throw new IllegalArgumentException("quantile must be in [0, 1].");
            }
            if (sorted.length == 0) {
                return 0;
            }
            int index = (int) Math.ceil(quantile * sorted.length) - 1;
            return sorted[Math.max(0, index)];
        }

        @Override
        public long[] getValues() {
            return sorted.clone();
        }

        @Override
        public int size() {
            return sorted.length;
        }

        @Override
        public double getMean() {
            if (sorted.length == 0) {
                return 0;
            }
            double sum = 0;
            for (long value : sorted) {
                sum += value;
            }
            return sum / sorted.length;
        }

        @Override
        public double getStdDev() {
            if (sorted.length <= 1) {
                return 0;
            }
            double mean = getMean();
            double sum = 0;
            for (long value : sorted) {
                double delta = value - mean;
                sum += delta * delta;
            }
            return Math.sqrt(sum / (sorted.length - 1));
        }

        @Override
        public long getMax() {
            return sorted.length == 0 ? 0 : sorted[sorted.length - 1];
        }

        @Override
        public long getMin() {
            return sorted.length == 0 ? 0 : sorted[0];
        }
    }
}
