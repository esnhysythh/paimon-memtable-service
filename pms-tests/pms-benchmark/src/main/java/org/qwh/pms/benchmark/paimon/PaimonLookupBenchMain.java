package org.qwh.pms.benchmark.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.KeyComparatorSupplier;
import org.qwh.pms.lookup.PaimonKeyValueLookupService;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.cache.LocalCacheDirectory;
import org.qwh.pms.lookup.parquet.PaimonKeyValueParquetLookup;
import org.qwh.pms.lookup.routing.ThresholdFileLookupRouter;
import org.qwh.pms.lookup.view.CandidatePlanner;
import org.qwh.pms.lookup.view.LiveFileIndex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/** Query-only benchmark over real local Paimon files, using the production lookup stack. */
public final class PaimonLookupBenchMain {
    private PaimonLookupBenchMain() {}

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        // Parquet/Hadoop log per-read INFO messages; console IO must not dominate measurements.
        if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        }
        var rootLogger = java.util.logging.Logger.getLogger("");
        rootLogger.setLevel(java.util.logging.Level.WARNING);
        for (var handler : rootLogger.getHandlers()) handler.setLevel(java.util.logging.Level.WARNING);
        Files.createDirectories(config.db());
        Path run = Files.createTempDirectory(config.db(), "paimon-bench-");
        System.err.println("config=" + config + ", run=" + run);
        System.err.println("java=" + System.getProperty("java.version") + ", os="
            + System.getProperty("os.name") + ", arch=" + System.getProperty("os.arch")
            + ", cpus=" + Runtime.getRuntime().availableProcessors());
        boolean success = false;
        try {
            var data = new LocalPaimonData(run.resolve("warehouse"), config.num(), config.valueSize());
            System.err.printf(Locale.ROOT, "rows=%d, files=%d, parquet_bytes=%d%n",
                data.rows, data.files.size(), data.files.get(0).fileSize());
            try (Session session = new Session(data, run.resolve("cache"), config.mode())) {
                session.verify();
                System.out.println("mode,case,phase,iteration,threads,rows,payload_bytes,operations,seconds,ops_per_sec,p99_us,errors,direct_lookups,cached_lookups");
                for (boolean hit : new boolean[]{true, false}) {
                    // Identical keys and static worker assignment across modes and iterations.
                    int[] keys = new int[config.reads()];
                    var random = new SplittableRandom(config.seed());
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = random.nextInt(hit ? data.rows : data.rows - 1) * 2 + (hit ? 0 : 1);
                    }
                    for (int iteration = -config.warmupRuns(); iteration < config.runs(); iteration++) {
                        Result result = measure(session, keys, hit, config.threads());
                        System.out.printf(Locale.ROOT, "%s,%s,%s,%d,%d,%d,%d,%d,%.6f,%.2f,%.2f,0,%d,%d%n",
                            config.mode(), hit ? "hit" : "miss", iteration < 0 ? "warmup" : "measurement",
                            iteration < 0 ? iteration + config.warmupRuns() : iteration,
                            config.threads(), data.rows, data.valueSize, keys.length,
                            result.seconds(), keys.length / result.seconds(), result.p99Nanos() / 1000.0,
                            result.direct(), result.cached());
                        System.out.flush();
                    }
                }
            }
            success = true;
        } finally {
            if (success && config.deleteTempDb()) {
                try (var paths = Files.walk(run)) {
                    for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
                }
            } else {
                System.err.println("Preserved run directory: " + run);
            }
        }
    }

    static Result measure(Session session, int[] keys, boolean hit, int threads) throws Exception {
        var executor = Executors.newFixedThreadPool(threads);
        var ready = new CountDownLatch(threads);
        var start = new CountDownLatch(1);
        List<Future<long[]>> futures = new ArrayList<>();
        long directBefore = session.directCount.sum();
        long cachedBefore = session.cachedCount();
        int stride = Math.max(1, (keys.length + 9999) / 10000);
        try {
            for (int t = 0; t < threads; t++) {
                int from = (int) ((long) keys.length * t / threads);
                int to = (int) ((long) keys.length * (t + 1) / threads);
                futures.add(executor.submit(() -> {
                    var row = GenericRow.of(0);
                    var request = LookupRequest.fullRow(row);
                    long[] samples = new long[(to - from) / stride + 2];
                    int count = 0;
                    ready.countDown();
                    start.await();
                    for (int i = from; i < to; i++) {
                        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                        row.setField(0, keys[i]);
                        boolean sample = i % stride == 0;
                        long before = sample ? System.nanoTime() : 0;
                        LookupResult result = session.lookup(request);
                        if (sample) samples[count++] = System.nanoTime() - before;
                        requireKind(result, hit);
                    }
                    return Arrays.copyOf(samples, count);
                }));
            }
            if (!ready.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Workers did not start");
            long begin = System.nanoTime();
            start.countDown();
            List<long[]> samples = new ArrayList<>();
            long deadline = begin + TimeUnit.MINUTES.toNanos(10);
            for (var future : futures) {
                samples.add(future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
            }
            double seconds = (System.nanoTime() - begin) / 1e9;
            long[] latency = samples.stream().flatMapToLong(Arrays::stream).sorted().toArray();
            long direct = session.directCount.sum() - directBefore;
            long cached = session.cachedCount() - cachedBefore;
            // The fixture has one candidate file for every HIT and every in-range MISS.
            if (session.router == null ? direct != keys.length || cached != 0
                    : direct != 0 || cached != keys.length) {
                throw new IllegalStateException("Unexpected query route: direct=" + direct + ", cached=" + cached);
            }
            return new Result(seconds, latency[(int) Math.ceil(latency.length * .99) - 1], direct, cached);
        } finally {
            start.countDown();
            futures.forEach(future -> future.cancel(true));
            executor.shutdownNow();
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Benchmark workers did not terminate");
            }
        }
    }

    private static void requireKind(LookupResult result, boolean hit) {
        if (result.kind() != (hit ? LookupResult.Kind.HIT : LookupResult.Kind.MISS)) {
            throw new IllegalStateException("Unexpected lookup result: " + result.kind() + ", expected hit=" + hit);
        }
    }

    record Result(double seconds, long p99Nanos, long direct, long cached) {}

    static final class Session implements AutoCloseable {
        final LocalPaimonData data;
        final LongAdder directCount = new LongAdder();
        final PaimonKeyValueParquetLookup direct;
        final ThresholdFileLookupRouter router;
        final LocalCacheDirectory directory;
        final PaimonKeyValueLookupService service;

        Session(LocalPaimonData data, Path cachePath, String mode) throws Exception {
            this.data = data;
            direct = new PaimonKeyValueParquetLookup(LocalPaimonData.ROW_TYPE, new int[]{0},
                    data.table.schema().id(), data.resolver()) {
                @Override
                public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request)
                        throws IOException {
                    directCount.increment();
                    return super.lookup(context, file, request);
                }
            };
            directory = mode.equals("cached") ? new LocalCacheDirectory(cachePath) : null;
            ThresholdFileLookupRouter created = null;
            try {
                // Build synchronously during preparation; measured queries use the normal READY path.
                created = directory == null ? null : new ThresholdFileLookupRouter(
                    direct, data.cacheBuilder(directory), Runnable::run, 1);
                router = created;
                Comparator<InternalRow> comparator = new KeyComparatorSupplier(LocalPaimonData.KEY_TYPE).get();
                service = new PaimonKeyValueLookupService(new LiveFileIndex(comparator, data.table.coreOptions().numLevels()),
                    new CandidatePlanner(comparator, 0), router == null ? direct : router, data.table.schema().id());
                service.installSnapshot(BinaryRow.EMPTY_ROW, 0, data.files);
                if (router != null) {
                    requireKind(lookup(LookupRequest.fullRow(GenericRow.of(0))), true);
                    var stats = router.stats();
                    if (stats.readyEntries() != data.files.size() || stats.inFlightBuilds() != 0
                            || stats.buildsFailed() != 0 || stats.buildsTimedOut() != 0) {
                        throw new IllegalStateException("Cache preparation failed: " + stats);
                    }
                    System.err.println("cache_ready=" + stats.readyEntries() + ", cache_bytes=" + stats.cacheBytes());
                }
            } catch (Exception | Error e) {
                if (created != null) created.close();
                if (directory != null) directory.close();
                throw e;
            }
        }

        LookupResult lookup(LookupRequest request) throws IOException {
            return service.lookup(BinaryRow.EMPTY_ROW, 0, request);
        }

        long cachedCount() { return router == null ? 0 : router.stats().localLookups(); }

        void verify() throws IOException {
            // Deterministic values catch wrong-row and payload errors, not just false HITs.
            for (int i = 0; i < Math.min(data.rows, 64); i++) {
                int key = (int) ((long) i * (data.rows - 1) / Math.max(1, Math.min(data.rows, 64) - 1)) * 2;
                LookupResult hit = lookup(LookupRequest.fullRow(GenericRow.of(key)));
                requireKind(hit, true);
                InternalRow row = hit.row().orElseThrow();
                if (row.getInt(0) != key || !row.getString(1).toString().equals(LocalPaimonData.payload(key, data.valueSize))) {
                    throw new IllegalStateException("Wrong value for key " + key);
                }
                if (key < (data.rows - 1) * 2) {
                    requireKind(lookup(LookupRequest.fullRow(GenericRow.of(key + 1))), false);
                }
            }
        }

        @Override public void close() throws Exception {
            try { if (router != null) router.close(); }
            finally { if (directory != null) directory.close(); }
        }
    }

    record Config(String mode, int num, int reads, int threads, int valueSize, int warmupRuns,
                  int runs, long seed, Path db, boolean deleteTempDb) {
        static Config parse(String[] args) {
            Map<String, String> values = new HashMap<>();
            var allowed = List.of("mode", "num", "reads", "threads", "value_size", "warmup_runs", "runs", "seed", "db", "delete_temp_db");
            for (String arg : args) {
                int split = arg.indexOf('=');
                if (!arg.startsWith("--") || split < 3) throw new IllegalArgumentException("Expected --name=value: " + arg);
                String name = arg.substring(2, split);
                if (!allowed.contains(name) || values.putIfAbsent(name, arg.substring(split + 1)) != null) {
                    throw new IllegalArgumentException("Unknown or duplicate option: " + name);
                }
            }
            var c = new Config(values.getOrDefault("mode", "direct"),
                Integer.parseInt(values.getOrDefault("num", "100000")),
                Integer.parseInt(values.getOrDefault("reads", "20000")),
                Integer.parseInt(values.getOrDefault("threads", "1")),
                Integer.parseInt(values.getOrDefault("value_size", "100")),
                Integer.parseInt(values.getOrDefault("warmup_runs", "1")),
                Integer.parseInt(values.getOrDefault("runs", "3")),
                Long.parseLong(values.getOrDefault("seed", "24301")),
                Path.of(values.getOrDefault("db", "pms-tests/pms-benchmark/target/paimon-data")).toAbsolutePath(),
                parseBoolean(values.getOrDefault("delete_temp_db", "true")));
            if (!List.of("direct", "cached").contains(c.mode) || c.num < 2 || c.num > Integer.MAX_VALUE / 2
                    || c.threads < 1 || c.reads < c.threads || c.valueSize < 1 || c.warmupRuns < 0 || c.runs < 1) {
                throw new IllegalArgumentException("Invalid benchmark configuration: " + c);
            }
            return c;
        }

        private static boolean parseBoolean(String value) {
            if (!value.equals("true") && !value.equals("false")) throw new IllegalArgumentException("Expected true/false: " + value);
            return Boolean.parseBoolean(value);
        }
    }
}
