package org.qwh.pms.server;

import java.time.Instant;

interface SchedulerTimeSource {

    SchedulerTimeSource SYSTEM = new SchedulerTimeSource() {
        @Override
        public Instant wallClockNow() {
            return Instant.now();
        }

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }
    };

    Instant wallClockNow();

    long monotonicNanos();
}
