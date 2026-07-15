package org.qwh.pms.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PmsServerSchedulerTest {

    @Test
    void injectedTimeSourceDrivesExistingFlushTaskState() throws Exception {
        Instant now = Instant.parse("2026-01-02T03:04:05Z");
        AtomicLong monotonicNanos = new AtomicLong(1_000_000_000L);
        SchedulerTimeSource timeSource = new SchedulerTimeSource() {
            @Override
            public Instant wallClockNow() {
                return now;
            }

            @Override
            public long monotonicNanos() {
                return monotonicNanos.getAndAdd(TimeUnit.MILLISECONDS.toNanos(5));
            }
        };
        CountDownLatch flushed = new CountDownLatch(1);

        PmsServerScheduler scheduler = new PmsServerScheduler(
            flushed::countDown,
            () -> { },
            new PmsSchedulerConfig(true, 1, 0),
            timeSource
        );
        try {
            scheduler.start();
            assertTrue(flushed.await(5, TimeUnit.SECONDS));
        } finally {
            scheduler.close();
        }

        Map<String, Object> state = scheduler.state();
        assertTrue((Long) state.get("flushSuccessCount") > 0);
        assertEquals(now.toString(), state.get("lastFlushStartedAt"));
        assertEquals(now.toString(), state.get("lastFlushCompletedAt"));
        assertEquals(5L, state.get("lastFlushDurationMs"));
    }
}
