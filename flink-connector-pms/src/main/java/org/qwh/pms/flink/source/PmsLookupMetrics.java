package org.qwh.pms.flink.source;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.qwh.pms.flink.metrics.PmsAtomicCounter;
import org.qwh.pms.flink.metrics.PmsSlidingHistogram;

import java.util.concurrent.atomic.AtomicLong;

/** 同步和异步 Lookup 共用的线程安全 metrics. */
final class PmsLookupMetrics {

    private final Counter requests;
    private final Counter hits;
    private final Counter misses;
    private final Counter deleted;
    private final Counter retries;
    private final Counter failures;
    private final Counter nullKeys;
    private final Histogram latency;
    private final AtomicLong inFlight = new AtomicLong();

    PmsLookupMetrics(MetricGroup parent) {
        MetricGroup group = parent.addGroup("pms");
        requests = group.counter("lookupRequests", new PmsAtomicCounter());
        hits = group.counter("lookupHits", new PmsAtomicCounter());
        misses = group.counter("lookupMisses", new PmsAtomicCounter());
        deleted = group.counter("lookupDeleted", new PmsAtomicCounter());
        retries = group.counter("lookupRetries", new PmsAtomicCounter());
        failures = group.counter("lookupFailures", new PmsAtomicCounter());
        nullKeys = group.counter("lookupNullKeys", new PmsAtomicCounter());
        latency = group.histogram("lookupLatency", new PmsSlidingHistogram(1024));
        group.gauge("lookupInFlight", inFlight::get);
    }

    long beginLookup() {
        inFlight.incrementAndGet();
        return System.nanoTime();
    }

    void finishLookup(long startNanos) {
        inFlight.decrementAndGet();
        latency.update(Math.max(0, (System.nanoTime() - startNanos) / 1_000_000));
    }

    void recordRequest() {
        requests.inc();
    }

    void recordHit() {
        hits.inc();
    }

    void recordMiss() {
        misses.inc();
    }

    void recordDeleted() {
        deleted.inc();
    }

    void recordRetry() {
        retries.inc();
    }

    void recordFailure() {
        failures.inc();
    }

    void recordNullKey() {
        nullKeys.inc();
    }
}
