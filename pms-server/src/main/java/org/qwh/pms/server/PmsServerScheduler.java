package org.qwh.pms.server;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PmsServerScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerScheduler.class);
    private static final AtomicInteger THREAD_ID = new AtomicInteger();

    private final PmsTableService service;
    private final PmsSchedulerConfig config;
    private final ScheduledExecutorService executor;

    private volatile boolean running;
    private volatile String lastFlushStartedAt;
    private volatile String lastFlushCompletedAt;
    private volatile long lastFlushDurationMs;
    private volatile long flushSuccessCount;
    private volatile long flushFailureCount;
    private volatile boolean flushRunning;
    private volatile String lastSinkStartedAt;
    private volatile String lastSinkCompletedAt;
    private volatile long lastSinkDurationMs;
    private volatile long sinkSuccessCount;
    private volatile long sinkFailureCount;
    private volatile boolean sinkRunning;
    private volatile String lastErrorAt;
    private volatile String lastErrorTask;
    private volatile String lastErrorMessage;

    public PmsServerScheduler(PmsTableService service, PmsSchedulerConfig config) {
        if (service == null) {
            throw new IllegalArgumentException("service must not be null");
        }
        this.service = service;
        this.config = config == null ? PmsSchedulerConfig.disabled(0) : config;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pms-server-scheduler-" + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        if (!config.enabled()) {
            LOG.info("PMS server scheduler disabled");
            return;
        }
        if (config.flushIntervalMs() > 0) {
            executor.scheduleWithFixedDelay(
                () -> runTask("flush", this::flush),
                config.flushIntervalMs(),
                config.flushIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        }
        if (config.sinkIntervalMs() > 0) {
            executor.scheduleWithFixedDelay(
                () -> runTask("sink", this::sink),
                config.sinkIntervalMs(),
                config.sinkIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        }
        running = true;
        LOG.info(
            "PMS server scheduler started: flushIntervalMs={}, sinkIntervalMs={}",
            config.flushIntervalMs(),
            config.sinkIntervalMs()
        );
    }

    public Map<String, Object> state() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("enabled", config.enabled());
        result.put("running", running);
        result.put("flushIntervalMs", config.flushIntervalMs());
        result.put("sinkIntervalMs", config.sinkIntervalMs());
        result.put("flushRunning", flushRunning);
        result.put("lastFlushStartedAt", lastFlushStartedAt);
        result.put("lastFlushCompletedAt", lastFlushCompletedAt);
        result.put("lastFlushDurationMs", lastFlushDurationMs);
        result.put("flushSuccessCount", flushSuccessCount);
        result.put("flushFailureCount", flushFailureCount);
        result.put("sinkRunning", sinkRunning);
        result.put("lastSinkStartedAt", lastSinkStartedAt);
        result.put("lastSinkCompletedAt", lastSinkCompletedAt);
        result.put("lastSinkDurationMs", lastSinkDurationMs);
        result.put("sinkSuccessCount", sinkSuccessCount);
        result.put("sinkFailureCount", sinkFailureCount);
        result.put("lastErrorAt", lastErrorAt);
        result.put("lastErrorTask", lastErrorTask);
        result.put("lastErrorMessage", lastErrorMessage);
        return result;
    }

    @Override
    public synchronized void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("PMS server scheduler did not stop within timeout; forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        LOG.info("PMS server scheduler stopped");
    }

    private void runTask(String taskName, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            lastErrorAt = now();
            lastErrorTask = taskName;
            lastErrorMessage = e.getMessage();
            if ("flush".equals(taskName)) {
                flushFailureCount++;
            } else if ("sink".equals(taskName)) {
                sinkFailureCount++;
            }
            LOG.error("PMS scheduled {} failed", taskName, e);
        }
    }

    private void flush() {
        long startedNanos = System.nanoTime();
        flushRunning = true;
        lastFlushStartedAt = now();
        LOG.debug("Scheduled PMS flush started");
        try {
            service.flush();
            flushSuccessCount++;
            LOG.debug("Scheduled PMS flush completed");
        } finally {
            lastFlushDurationMs = elapsedMillis(startedNanos);
            lastFlushCompletedAt = now();
            flushRunning = false;
        }
    }

    private void sink() {
        long startedNanos = System.nanoTime();
        sinkRunning = true;
        lastSinkStartedAt = now();
        LOG.debug("Scheduled PMS sink started");
        try {
            service.sink();
            sinkSuccessCount++;
            LOG.debug("Scheduled PMS sink completed");
        } finally {
            lastSinkDurationMs = elapsedMillis(startedNanos);
            lastSinkCompletedAt = now();
            sinkRunning = false;
        }
    }

    private static String now() {
        return Instant.now().toString();
    }

    private static long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
    }
}
