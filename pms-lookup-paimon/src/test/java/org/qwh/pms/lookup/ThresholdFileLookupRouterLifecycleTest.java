package org.qwh.pms.lookup;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.junit.jupiter.api.Test;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.cache.LocalCacheBuildContext;
import org.qwh.pms.lookup.cache.LocalCacheBuilder;
import org.qwh.pms.lookup.cache.LocalCacheEntry;
import org.qwh.pms.lookup.cache.LocalCacheMode;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouter;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouterOptions;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThresholdFileLookupRouterLifecycleTest {

    private static final FileLookupContext CONTEXT =
            new FileLookupContext(BinaryRow.EMPTY_ROW, 0);
    private static final LookupRequest REQUEST = LookupRequest.fullRow(BinaryRow.EMPTY_ROW);

    @Test
    void retriesFailedBuildAfterBackoff() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        AtomicInteger attempts = new AtomicInteger();
        LocalCacheEntry readyEntry = new TestEntry(8);
        LocalCacheBuilder builder =
                new TestBuilder(
                        () -> {
                            if (attempts.getAndIncrement() == 0) {
                                throw new IOException("first build fails");
                            }
                            return readyEntry;
                        });

        try (ThresholdFileLookupRouter router =
                new ThresholdFileLookupRouter(
                        new DirectLookup(),
                        builder,
                        executor,
                        new ThresholdFileLookupRouterOptions(
                                1, 1, 100, Duration.ofMinutes(1), Duration.ZERO))) {
            DataFileMeta file = file("retry");
            router.lookup(CONTEXT, file, REQUEST);
            executor.runNext();
            assertEquals(1, router.stats().buildsFailed());

            router.lookup(CONTEXT, file, REQUEST);
            executor.runNext();
            assertEquals(2, router.stats().buildsScheduled());

            router.lookup(CONTEXT, file, REQUEST);
            assertEquals(1, router.stats().localLookups());
            assertEquals(1, router.stats().buildsSucceeded());
        }
    }

    @Test
    void limitsQueuedBuildsAndEvictsLeastRecentlyUsedReadyEntry() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        TestEntry firstEntry = new TestEntry(7);
        TestEntry secondEntry = new TestEntry(7);
        Queue<LocalCacheEntry> entries = new ArrayDeque<>();
        entries.add(firstEntry);
        entries.add(secondEntry);
        LocalCacheBuilder builder = new TestBuilder(entries::remove);

        try (ThresholdFileLookupRouter router =
                new ThresholdFileLookupRouter(
                        new DirectLookup(),
                        builder,
                        executor,
                        new ThresholdFileLookupRouterOptions(
                                1, 1, 10, Duration.ofMinutes(1), Duration.ZERO))) {
            DataFileMeta first = file("first");
            DataFileMeta second = file("second");
            router.lookup(CONTEXT, first, REQUEST);
            assertEquals(1, executor.pendingTaskCount());
            router.lookup(CONTEXT, second, REQUEST);
            assertEquals(1, executor.pendingTaskCount());
            assertEquals(1, router.stats().buildsRejected());

            executor.runNext();
            router.lookup(CONTEXT, second, REQUEST);
            assertEquals(1, executor.pendingTaskCount());
            executor.runNext();

            assertTrue(firstEntry.closed);
            assertFalse(secondEntry.closed);
            assertEquals(1, router.stats().entriesEvicted());
            assertEquals(1, router.stats().readyEntries());
            assertEquals(7, router.stats().cacheBytes());
        }
    }

    @Test
    void cancelsTimedOutBuildAndAllowsRetry() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        try (ThresholdFileLookupRouter router =
                new ThresholdFileLookupRouter(
                        new DirectLookup(),
                        new TestBuilder(() -> new TestEntry(1)),
                        executor,
                        new ThresholdFileLookupRouterOptions(
                                1, 1, 10, Duration.ofMillis(1), Duration.ZERO))) {
            DataFileMeta file = file("timeout");
            router.lookup(CONTEXT, file, REQUEST);
            Thread.sleep(5);
            router.lookup(CONTEXT, file, REQUEST);

            assertEquals(1, router.stats().buildsTimedOut());
            assertEquals(2, router.stats().buildsScheduled());
            assertEquals(1, router.stats().inFlightBuilds());

            executor.runNext();
            executor.runNext();
            router.lookup(CONTEXT, file, REQUEST);
            assertEquals(1, router.stats().localLookups());
        }
    }

    @Test
    void timesOutIdleBuildBeforeAnotherFileNeedsAdmission() throws Exception {
        ManualExecutor executor = new ManualExecutor();
        try (ThresholdFileLookupRouter router =
                new ThresholdFileLookupRouter(
                        new DirectLookup(),
                        new TestBuilder(() -> new TestEntry(1)),
                        executor,
                        new ThresholdFileLookupRouterOptions(
                                1, 1, 10, Duration.ofMillis(10), Duration.ZERO))) {
            router.lookup(CONTEXT, file("idle-timeout"), REQUEST);
            waitFor(() -> router.stats().buildsTimedOut() == 1);
            assertEquals(0, router.stats().inFlightBuilds());

            router.lookup(CONTEXT, file("next-hot-file"), REQUEST);
            assertEquals(2, router.stats().buildsScheduled());
            assertEquals(0, router.stats().buildsRejected());
        }
    }

    @Test
    void staleTimedOutBuildDoesNotReleaseRetryAdmissionSlot() throws Exception {
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch releaseSecond = new CountDownLatch(1);
        CountDownLatch firstFinished = new CountDownLatch(1);
        CountDownLatch secondFinished = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        LocalCacheBuilder builder =
                new TestBuilder(
                        () -> {
                            if (attempts.incrementAndGet() == 1) {
                                firstStarted.countDown();
                                awaitIgnoringInterrupts(releaseFirst);
                                firstFinished.countDown();
                                return new TestEntry(1);
                            }
                            secondStarted.countDown();
                            awaitIgnoringInterrupts(releaseSecond);
                            secondFinished.countDown();
                            return new TestEntry(1);
                        });

        // Give the retry build a real scheduling window after the first build times out.
        try (ThresholdFileLookupRouter router =
                new ThresholdFileLookupRouter(
                        new DirectLookup(),
                        builder,
                        executor,
                        new ThresholdFileLookupRouterOptions(
                                1, 1, 10, Duration.ofMillis(100), Duration.ZERO))) {
            DataFileMeta file = file("late-timeout");
            router.lookup(CONTEXT, file, REQUEST);
            assertTrue(firstStarted.await(1, TimeUnit.SECONDS));
            waitFor(() -> router.stats().buildsTimedOut() == 1);
            router.lookup(CONTEXT, file, REQUEST);
            assertTrue(secondStarted.await(1, TimeUnit.SECONDS));
            assertEquals(1, router.stats().inFlightBuilds());

            releaseFirst.countDown();
            assertTrue(firstFinished.await(1, TimeUnit.SECONDS));
            assertEquals(1, router.stats().inFlightBuilds());

            releaseSecond.countDown();
            assertTrue(secondFinished.await(1, TimeUnit.SECONDS));
            waitFor(() -> router.stats().inFlightBuilds() == 0);
        } finally {
            executor.shutdownNow();
        }
    }

    private static DataFileMeta file(String name) {
        return DataFileMeta.create(
                name,
                1L,
                1L,
                BinaryRow.EMPTY_ROW,
                BinaryRow.EMPTY_ROW,
                null,
                null,
                0L,
                0L,
                0L,
                0,
                0L,
                null,
                null,
                null,
                null,
                null);
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("condition was not met within one second");
            }
            Thread.sleep(5);
        }
    }

    private static final class DirectLookup implements DataFileLookup {

        @Override
        public LookupResult lookup(
                FileLookupContext context, DataFileMeta file, LookupRequest request) {
            return LookupResult.miss();
        }

        @Override
        public void invalidate(FileLookupContext context, DataFileMeta file) {}
    }

    private static final class TestBuilder implements LocalCacheBuilder {

        private final BuildAction action;

        private TestBuilder(BuildAction action) {
            this.action = action;
        }

        @Override
        public LocalCacheMode mode() {
            return LocalCacheMode.VALUE_SST;
        }

        @Override
        public LocalCacheEntry build(DataFileMeta file, LocalCacheBuildContext context)
                throws IOException {
            return action.build();
        }
    }

    @FunctionalInterface
    private interface BuildAction {
        LocalCacheEntry build() throws IOException;
    }

    private static final class TestEntry implements LocalCacheEntry {

        private final long bytes;
        private boolean closed;

        private TestEntry(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public LocalCacheMode mode() {
            return LocalCacheMode.VALUE_SST;
        }

        @Override
        public LookupResult lookup(LookupRequest request) {
            return LookupResult.miss();
        }

        @Override
        public long sizeBytes() {
            return bytes;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class ManualExecutor implements Executor {

        private final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        private int pendingTaskCount() {
            return tasks.size();
        }

        private void runNext() {
            tasks.remove().run();
        }
    }
}
