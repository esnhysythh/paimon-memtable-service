package org.qwh.pms.flink.metrics;

import org.apache.flink.metrics.Counter;

import java.util.concurrent.atomic.AtomicLong;

/** async Lookup worker 之间共享的线程安全 Flink Counter. */
public final class PmsAtomicCounter implements Counter {

    private final AtomicLong value = new AtomicLong();

    @Override
    public void inc() {
        value.incrementAndGet();
    }

    @Override
    public void inc(long amount) {
        value.addAndGet(amount);
    }

    @Override
    public void dec() {
        value.decrementAndGet();
    }

    @Override
    public void dec(long amount) {
        value.addAndGet(-amount);
    }

    @Override
    public long getCount() {
        return value.get();
    }
}
