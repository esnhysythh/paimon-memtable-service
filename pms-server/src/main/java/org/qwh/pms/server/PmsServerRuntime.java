package org.qwh.pms.server;

import java.time.Instant;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.qwh.pms.core.bucket.PmsFatalException;
import org.qwh.pms.core.bucket.PmsWriteOverloadedException;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.PmsTableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PmsServerRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(PmsServerRuntime.class);

    private final PmsServerConfig config;
    private final TableServiceOpener tableServiceOpener;
    // Resource cleanup may wait for workers; fatal publication never acquires this mutex.
    private final Object closeMutex = new Object();

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

    public PmsServerRuntime start() throws Exception {
        synchronized (closeMutex) {
            try {
                return startServices();
            } catch (Exception e) {
                // Startup cleanup also releases the state monitor before waiting for requests/workers.
                closeQuietly(this);
                throw e;
            }
        }
    }

    private synchronized PmsServerRuntime startServices() throws Exception {
        if (status == PmsRuntimeStatus.RUNNING) {
            return this;
        }
        if (status != PmsRuntimeStatus.NEW && status != PmsRuntimeStatus.STOPPED) {
            throw new IllegalStateException("Cannot start PMS server runtime from status " + status);
        }
        try {
            service = tableServiceOpener.open(config);
            scheduler = new PmsServerScheduler(service, config.scheduler(), this::failFatalAsync);
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
        PmsTableService current = service;
        if (current == null) {
            throw new PmsServiceUnavailableException("PMS table service is not available, status=" + status);
        }
        return current;
    }

    public PmsTableSchema protocolTableSchema() {
        return requireRunning().protocolTableSchema();
    }

    public void write(Map<String, Object> rowValues) {
        try {
            requireAcceptingWrites().write(rowValues);
        } catch (PmsWriteOverloadedException e) {
            throw overloaded(e);
        } catch (PmsFatalException e) {
            failFatalAsync(e);
            throw e;
        }
    }

    public void delete(Map<String, Object> primaryKeyValues) {
        try {
            requireAcceptingWrites().delete(primaryKeyValues);
        } catch (PmsWriteOverloadedException e) {
            throw overloaded(e);
        } catch (PmsFatalException e) {
            failFatalAsync(e);
            throw e;
        }
    }

    public void writeRawBatch(List<RawKvEntry> entries) {
        try {
            requireAcceptingWrites().writeRawBatch(entries);
        } catch (PmsWriteOverloadedException e) {
            throw overloaded(e);
        } catch (PmsFatalException e) {
            failFatalAsync(e);
            throw e;
        }
    }

    public Optional<Map<String, Object>> get(Map<String, Object> primaryKeyValues) {
        return requireRunning().get(primaryKeyValues);
    }

    public RawLookupResult getLocalRaw(byte[] key) {
        return requireRunning().getLocalRaw(key);
    }

    public RawLookupResult getFullRaw(byte[] key) {
        return requireRunning().getFullRaw(key);
    }

    public PmsLocalLookupResult getLocal(Map<String, Object> primaryKeyValues) {
        return requireRunning().getLocal(primaryKeyValues);
    }

    public List<Map<String, Object>> prefixScan(Map<String, Object> primaryKeyPrefixValues) {
        return requireRunning().prefixScan(primaryKeyPrefixValues);
    }

    public List<Map<String, Object>> prefixLocal(Map<String, Object> primaryKeyPrefixValues) {
        return requireRunning().prefixLocal(primaryKeyPrefixValues);
    }

    public RawLookupBatchResult prefixLocalRaw(byte[] prefix) {
        return requireRunning().prefixLocalRaw(prefix, config.protocol().maxBatchEntries());
    }

    public long flush() {
        requireRunning();
        return scheduler().requestFlushToCurrent();
    }

    public long sink() {
        requireRunning();
        return scheduler().requestSinkToCurrent();
    }

    public Map<String, Object> state() {
        PmsTableService current = service;
        Map<String, Object> result = new LinkedHashMap<>();
        if (status == PmsRuntimeStatus.RUNNING && current != null && current.fatalFailure() == null) {
            result.putAll(current.state());
        }
        Map<String, Object> runtime = new LinkedHashMap<>();
        runtime.put("status", status.name());
        runtime.put("acceptingWrites", isHealthy());
        runtime.put("startedAt", startedAt);
        runtime.put("stoppingAt", stoppingAt);
        runtime.put("stoppedAt", stoppedAt);
        runtime.put("failedAt", failedAt);
        runtime.put("lastFailureMessage", lastFailureMessage);
        runtime.put("recovery", recoverySummary);
        result.put("runtime", runtime);
        PmsServerScheduler currentScheduler = scheduler;
        if (currentScheduler != null) {
            result.put("scheduler", currentScheduler.state());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        synchronized (closeMutex) {
            synchronized (this) {
                if (status == PmsRuntimeStatus.STOPPED || status == PmsRuntimeStatus.NEW) {
                    return;
                }
                LOG.info("PMS server runtime shutdown requested, status={}", status);
                if (status != PmsRuntimeStatus.FAILED) {
                    status = PmsRuntimeStatus.SHUTTING_DOWN;
                    stoppingAt = now();
                }
            }
            closeQuietly(httpServer);
            httpServer = null;
            closeQuietly(scheduler);
            scheduler = null;

            try {
                if (service != null) {
                    service.close();
                }
            } catch (Exception e) {
                synchronized (this) {
                    markFailed(e);
                }
                throw e;
            } finally {
                service = null;
            }
            synchronized (this) {
                if (status != PmsRuntimeStatus.FAILED) {
                    status = PmsRuntimeStatus.STOPPED;
                    stoppedAt = now();
                }
            }
            LOG.info("PMS server runtime shutdown completed, status={}", status);
        }
    }

    public boolean isHealthy() {
        PmsTableService current = service;
        return status == PmsRuntimeStatus.RUNNING && current != null && current.fatalFailure() == null;
    }

    private PmsTableService requireAcceptingWrites() {
        PmsTableService current = requireRunning();
        if (current.isWriteOverloaded()) {
            throw new PmsOverloadedException("PMS write backlog reached the configured overload watermark");
        }
        return current;
    }

    private PmsTableService requireRunning() {
        PmsTableService current = service;
        if (status != PmsRuntimeStatus.RUNNING || current == null || current.fatalFailure() != null) {
            throw new PmsServiceUnavailableException("PMS server is not running, status=" + status);
        }
        return current;
    }

    private static PmsOverloadedException overloaded(PmsWriteOverloadedException cause) {
        return new PmsOverloadedException(cause.getMessage(), cause);
    }

    private PmsServerScheduler scheduler() {
        PmsServerScheduler current = scheduler;
        if (current == null) {
            throw new PmsServiceUnavailableException("PMS server scheduler is not available, status=" + status);
        }
        return current;
    }

    void failFatalAsync(Throwable failure) {
        synchronized (this) {
            if (status == PmsRuntimeStatus.FAILED || status == PmsRuntimeStatus.STOPPED) {
                return;
            }
            PmsTableService current = service;
            if (current != null) {
                failure = current.markFatal(failure);
            }
            markFailed(failure);
        }
        Thread cleanup = new Thread(this::closeAfterFatal, "pms-fatal-shutdown");
        cleanup.setDaemon(false);
        cleanup.start();
    }

    private void closeAfterFatal() {
        LOG.error("PMS server runtime is stopping after a fatal failure: {}", lastFailureMessage);
        closeQuietly(this);
    }

    private void markFailed(Throwable e) {
        if (status != PmsRuntimeStatus.FAILED) {
            failedAt = now();
            lastFailureMessage = e.getMessage();
            status = PmsRuntimeStatus.FAILED;
        }
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
