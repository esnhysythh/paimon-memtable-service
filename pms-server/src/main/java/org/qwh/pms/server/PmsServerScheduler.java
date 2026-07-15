package org.qwh.pms.server;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PmsServerScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerScheduler.class);
    private static final AtomicInteger THREAD_ID = new AtomicInteger();

    private final Runnable flushAction;
    private final Runnable sinkAction;
    private final PmsSchedulerConfig config;
    private final SchedulerTimeSource timeSource;
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
        this(requireService(service), config, SchedulerTimeSource.SYSTEM);
    }

    private PmsServerScheduler(
            PmsTableService service,
            PmsSchedulerConfig config,
            SchedulerTimeSource timeSource) {
        this(
            service::flush,
            service::sink,
            config,
            timeSource
        );
    }

    PmsServerScheduler(
            Runnable flushAction,
            Runnable sinkAction,
            PmsSchedulerConfig config,
            SchedulerTimeSource timeSource) {
        this.flushAction = Objects.requireNonNull(flushAction, "flushAction must not be null");
        this.sinkAction = Objects.requireNonNull(sinkAction, "sinkAction must not be null");
        this.config = config == null ? PmsSchedulerConfig.disabled(0) : config;
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource must not be null");
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "pms-server-scheduler-" + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    private static PmsTableService requireService(PmsTableService service) {
        if (service == null) {
            throw new IllegalArgumentException("service must not be null");
        }
        return service;
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
        long startedNanos = timeSource.monotonicNanos();
        flushRunning = true;
        lastFlushStartedAt = now();
        LOG.debug("Scheduled PMS flush started");
        try {
            flushAction.run();
            flushSuccessCount++;
            LOG.debug("Scheduled PMS flush completed");
        } finally {
            lastFlushDurationMs = elapsedMillis(startedNanos);
            lastFlushCompletedAt = now();
            flushRunning = false;
        }
    }

    private void sink() {
        long startedNanos = timeSource.monotonicNanos();
        sinkRunning = true;
        lastSinkStartedAt = now();
        LOG.debug("Scheduled PMS sink started");
        try {
            sinkAction.run();
            sinkSuccessCount++;
            LOG.debug("Scheduled PMS sink completed");
        } finally {
            lastSinkDurationMs = elapsedMillis(startedNanos);
            lastSinkCompletedAt = now();
            sinkRunning = false;
        }
    }

    private String now() {
        return timeSource.wallClockNow().toString();
    }

    private long elapsedMillis(long startedNanos) {
        return TimeUnit.NANOSECONDS.toMillis(timeSource.monotonicNanos() - startedNanos);
    }
}
