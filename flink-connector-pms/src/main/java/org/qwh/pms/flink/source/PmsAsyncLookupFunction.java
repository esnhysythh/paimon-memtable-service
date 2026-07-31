package org.qwh.pms.flink.source;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.qwh.pms.flink.PmsConnectorConfig;
import org.qwh.pms.flink.PmsFlinkTableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 使用固定单线程 worker slot 包装同步 PmsClient 的 Async Lookup Function.
 *
 * <p>每个 slot 独占 PmsClient, 既提供并发, 又不把尚未声明的 Client thread-safety 当作前提.
 */
public final class PmsAsyncLookupFunction extends AsyncLookupFunction {

    private final PmsConnectorConfig config;
    private final PmsFlinkTableSchema schema;
    private final PmsLookupPlan plan;
    private final AtomicInteger nextWorker = new AtomicInteger();
    private transient PmsLookupMetrics metrics;
    private transient List<Worker> workers;

    PmsAsyncLookupFunction(
            PmsConnectorConfig config, PmsFlinkTableSchema schema, PmsLookupPlan plan) {
        this.config = config;
        this.schema = schema;
        this.plan = plan;
    }

    @Override
    public void open(FunctionContext context) {
        metrics = new PmsLookupMetrics(context.getMetricGroup());
        List<Worker> opened = new ArrayList<>();
        try {
            for (int i = 0; i < config.lookupAsyncThreadNumber(); i++) {
                String threadName = "pms-lookup-" + i;
                ExecutorService executor =
                        Executors.newSingleThreadExecutor(
                                runnable -> {
                                    Thread thread = new Thread(runnable, threadName);
                                    thread.setDaemon(true);
                                    thread.setContextClassLoader(context.getUserCodeClassLoader());
                                    return thread;
                                });
                try {
                    opened.add(
                            new Worker(
                                    executor,
                                    new PmsLookupRunner(config, schema, plan, metrics)));
                } catch (RuntimeException e) {
                    // Runner 初始化失败时, 当前 executor 尚未进入 opened 列表, 需要单独回收.
                    executor.shutdownNow();
                    throw e;
                }
            }
            workers = List.copyOf(opened);
        } catch (RuntimeException e) {
            opened.forEach(Worker::close);
            throw e;
        }
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        if (workers == null || workers.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IOException("PMS Async Lookup Function 尚未 open 或已经 close."));
        }

        // 复制必须发生在调用线程, 不能把 Flink 可复用 RowData 交给 worker.
        final RowData copiedKey;
        try {
            copiedKey = plan.copyKey(keyRow);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        Worker worker =
                workers.get(Math.floorMod(nextWorker.getAndIncrement(), workers.size()));
        try {
            return CompletableFuture.supplyAsync(
                    () -> {
                        long startNanos = metrics.beginLookup();
                        try {
                            return worker.runner.lookup(copiedKey);
                        } catch (IOException e) {
                            throw new CompletionException(e);
                        } finally {
                            metrics.finishLookup(startNanos);
                        }
                    },
                    worker.executor);
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() {
        if (workers == null) {
            return;
        }
        workers.forEach(Worker::close);
        workers = null;
    }

    private static final class Worker {

        private final ExecutorService executor;
        private final PmsLookupRunner runner;

        private Worker(ExecutorService executor, PmsLookupRunner runner) {
            this.executor = executor;
            this.runner = runner;
        }

        private void close() {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                runner.close();
            }
        }
    }
}
