package org.qwh.pms.server;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PmsServerRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerRuntime.class);

    private final PmsServerConfig config;
    private final TableServiceOpener tableServiceOpener;

    private volatile PmsRuntimeStatus status = PmsRuntimeStatus.NEW;
    private volatile PmsTableService service;
    private volatile PmsServerScheduler scheduler;
    private volatile PmsHttpServer httpServer;
    private volatile String startedAt;
    private volatile String stoppingAt;
    private volatile String stoppedAt;
    private volatile String failedAt;
    private volatile String lastFailureMessage;
    private volatile Map<String, Object> recoverySummary = Map.of();

    public PmsServerRuntime(PmsServerConfig config) {
        this(config, PmsTableService::open);
    }

    PmsServerRuntime(PmsServerConfig config, PmsTableService.SinkManagerFactory sinkManagerFactory) {
        this(config, serverConfig -> PmsTableService.open(serverConfig, sinkManagerFactory));
    }

    private PmsServerRuntime(PmsServerConfig config, TableServiceOpener tableServiceOpener) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (tableServiceOpener == null) {
            throw new IllegalArgumentException("tableServiceOpener must not be null");
        }
        this.config = config;
        this.tableServiceOpener = tableServiceOpener;
    }

    public synchronized PmsServerRuntime start() throws Exception {
        if (status == PmsRuntimeStatus.RUNNING) {
            return this;
        }
        if (status != PmsRuntimeStatus.NEW && status != PmsRuntimeStatus.STOPPED) {
            throw new IllegalStateException("Cannot start PMS server runtime from status " + status);
        }
        try {
            service = tableServiceOpener.open(config);
            scheduler = new PmsServerScheduler(service, config.scheduler());
            httpServer = new PmsHttpServer(config, this);
            startedAt = now();
            stoppingAt = null;
            stoppedAt = null;
            failedAt = null;
            lastFailureMessage = null;
            recoverySummary = service.recoverySummary();
            status = PmsRuntimeStatus.RUNNING;
            httpServer.start();
            scheduler.start();
            LOG.info("PMS server recovery summary: {}", recoverySummary);
            LOG.info("PMS server runtime started at http://{}:{}", config.host(), port());
            return this;
        } catch (Exception e) {
            markFailed(e);
            closeQuietly(httpServer);
            closeQuietly(scheduler);
            closeQuietly(service);
            httpServer = null;
            scheduler = null;
            service = null;
            throw e;
        }
    }

    public int port() {
        if (httpServer == null) {
            throw new IllegalStateException("PMS HTTP server is not started");
        }
        return httpServer.port();
    }

    public PmsRuntimeStatus status() {
        return status;
    }

    public PmsTableService service() {
        if (service == null) {
            throw new IllegalStateException("PMS table service is not started");
        }
        return service;
    }

    public void write(Map<String, Object> rowValues) {
        requireAcceptingWrites();
        service().write(rowValues);
    }

    public void delete(Map<String, Object> primaryKeyValues) {
        requireAcceptingWrites();
        service().delete(primaryKeyValues);
    }

    public Optional<Map<String, Object>> get(Map<String, Object> primaryKeyValues) {
        return service().get(primaryKeyValues);
    }

    public void flush() {
        requireStarted();
        service().flush();
    }

    public void sink() {
        requireStarted();
        service().sink();
    }

    public Map<String, Object> state() {
        Map<String, Object> result = service == null ? new LinkedHashMap<>() : new LinkedHashMap<>(service.state());
        Map<String, Object> runtime = new LinkedHashMap<>();
        runtime.put("status", status.name());
        runtime.put("acceptingWrites", acceptingWrites());
        runtime.put("startedAt", startedAt);
        runtime.put("stoppingAt", stoppingAt);
        runtime.put("stoppedAt", stoppedAt);
        runtime.put("failedAt", failedAt);
        runtime.put("lastFailureMessage", lastFailureMessage);
        runtime.put("recovery", recoverySummary);
        result.put("runtime", runtime);
        if (scheduler != null) {
            result.put("scheduler", scheduler.state());
        }
        return result;
    }

    @Override
    public synchronized void close() throws Exception {
        if (status == PmsRuntimeStatus.STOPPED || status == PmsRuntimeStatus.NEW) {
            return;
        }
        LOG.info("PMS server runtime shutdown requested, status={}", status);
        status = PmsRuntimeStatus.DRAINING;
        stoppingAt = now();
        Exception failure = null;

        closeQuietly(httpServer);
        httpServer = null;
        closeQuietly(scheduler);
        scheduler = null;

        if (service != null) {
            try {
                LOG.info("PMS server runtime final flush/sink started");
                service.flush();
                service.sink();
                LOG.info("PMS server runtime final flush/sink completed");
            } catch (Exception e) {
                failure = e;
                markFailed(e);
                LOG.error("PMS server runtime final flush/sink failed", e);
            }
            try {
                service.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
                markFailed(e);
            } finally {
                service = null;
            }
        }

        if (failure != null) {
            throw failure;
        }
        status = PmsRuntimeStatus.STOPPED;
        stoppedAt = now();
        LOG.info("PMS server runtime shutdown completed");
    }

    /**
     * Simulates abrupt process loss for recovery tests and development fault injection.
     */
    public synchronized void abort() throws Exception {
        if (status == PmsRuntimeStatus.STOPPED || status == PmsRuntimeStatus.NEW) {
            return;
        }
        LOG.warn("PMS server runtime abort requested, status={}", status);
        closeQuietly(httpServer);
        httpServer = null;
        closeQuietly(scheduler);
        scheduler = null;
        if (service != null) {
            service.close();
            service = null;
        }
        status = PmsRuntimeStatus.STOPPED;
        stoppedAt = now();
        LOG.warn("PMS server runtime aborted without final flush/sink");
    }

    private boolean acceptingWrites() {
        return status == PmsRuntimeStatus.RUNNING;
    }

    private void requireAcceptingWrites() {
        if (!acceptingWrites()) {
            throw new PmsServiceUnavailableException("PMS server is not accepting writes, status=" + status);
        }
    }

    private void requireStarted() {
        if (service == null || status == PmsRuntimeStatus.NEW || status == PmsRuntimeStatus.STOPPED) {
            throw new PmsServiceUnavailableException("PMS server is not running, status=" + status);
        }
    }

    private void markFailed(Exception e) {
        status = PmsRuntimeStatus.FAILED;
        failedAt = now();
        lastFailureMessage = e.getMessage();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("Failed to close PMS runtime component", e);
        }
    }

    private static String now() {
        return Instant.now().toString();
    }

    @FunctionalInterface
    private interface TableServiceOpener {
        PmsTableService open(PmsServerConfig config) throws Exception;
    }
}
