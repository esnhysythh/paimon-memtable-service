package org.qwh.pms.benchmark.core;

import org.qwh.pms.core.bucket.BucketStateSnapshot;
import org.qwh.pms.core.bucket.LocalRunSnapshot;
import org.qwh.pms.core.bucket.PMSBucketDirectorImpl;
import org.qwh.pms.core.bucket.operation.CompactionSelection;
import org.qwh.pms.core.bucket.operation.SinkSelection;
import org.qwh.pms.core.config.FlowControlConfig;
import org.qwh.pms.core.config.MemTableConfig;
import org.qwh.pms.core.config.PMSConfig;
import org.qwh.pms.core.config.PaimonConfig;
import org.qwh.pms.core.config.SinkConfig;
import org.qwh.pms.core.config.StorageConfig;
import org.qwh.pms.core.config.WalConfig;
import org.qwh.pms.core.storage.SSTState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.management.ManagementFactory;
import java.util.Set;

/** A small db_bench-like macro benchmark for the byte-oriented pms-core path. */
public final class CoreDbBenchMain {

    private static final String DEFAULT_BENCHMARKS = "fillrandom,readrandom";
    private static final int DEFAULT_NUM = 100_000;
    private static final int DEFAULT_THREADS = 1;
    private static final int DEFAULT_KEY_SIZE = 16;
    private static final int DEFAULT_VALUE_SIZE = 100;
    private static final int DEFAULT_LATENCY_SAMPLES = 10_000;
    private static final int OP_CHUNK = 256;

    private static volatile long blackhole;

    private CoreDbBenchMain() {}

    public static void main(String[] args) throws Exception {
        System.setProperty(
            "org.slf4j.simpleLogger.defaultLogLevel",
            System.getProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn")
        );
        BenchOptions options = BenchOptions.parse(args);
        // Always allocate a new child: a user-supplied --db is a parent, never a deletion target.
        Path dbDir;
        if (options.dbDir() == null) {
            dbDir = Files.createTempDirectory("pms-dbbench-");
        } else {
            Files.createDirectories(options.dbDir());
            dbDir = Files.createTempDirectory(options.dbDir(), "pms-dbbench-");
        }
        System.err.println("db=" + dbDir.toAbsolutePath());
        System.err.println(options);
        System.err.println("java=" + System.getProperty("java.runtime.version")
            + " os=" + System.getProperty("os.name") + " arch=" + System.getProperty("os.arch")
            + " processors=" + Runtime.getRuntime().availableProcessors()
            + " jvmArgs=" + ManagementFactory.getRuntimeMXBean().getInputArguments());
        System.out.println("phase,iteration,benchmark,ops,seconds,ops_per_sec,p50_ms,p95_ms,p99_ms,max_sample_ms,db_bytes,new_ssts,sinked_ssts,immutable_count,cur_entries");
        CoreDbBenchMain runner = new CoreDbBenchMain();
        for (int iteration = 0; iteration < options.warmupRuns() + options.runs(); iteration++) {
            String phase = iteration < options.warmupRuns() ? "warmup" : "measure";
            int phaseIteration = iteration < options.warmupRuns() ? iteration + 1 : iteration - options.warmupRuns() + 1;
            Path iterationDir = Files.createDirectory(dbDir.resolve(phase + "-" + phaseIteration));
            for (String benchmark : options.benchmarks()) {
                BenchmarkResult result = runner.runBenchmark(benchmark, options, iterationDir);
                System.out.println(phase + "," + phaseIteration + "," + result.toCsv());
            }
            // Each iteration owns this directory. Keep failed iterations for diagnosis.
            if (options.deleteTempDb()) {
                deleteRecursively(iterationDir);
            }
        }
        if (options.deleteTempDb()) {
            Files.delete(dbDir);
        }
    }

    private BenchmarkResult runBenchmark(String benchmarkName, BenchOptions options, Path dbDir) throws Exception {
        String normalized = benchmarkName.toLowerCase(Locale.ROOT).trim();
        return switch (normalized) {
            case "fillseq" -> runFill(options, dbDir, normalized, false);
            case "fillrandom" -> runFill(options, dbDir, normalized, true);
            case "overwrite" -> runOverwrite(options, dbDir);
            case "readrandom" -> runReadRandom(options, dbDir);
            case "readmissing" -> runReadMissing(options, dbDir);
            case "readwhilewriting" -> runReadWhileWriting(options, dbDir);
            case "deleterandom" -> runDeleteRandom(options, dbDir);
            case "flush" -> runFlush(options, dbDir);
            case "compact" -> runCompact(options, dbDir);
            case "recover" -> runRecover(options, dbDir);
            default -> throw new IllegalArgumentException("Unknown benchmark: " + benchmarkName);
        };
    }

    private BenchmarkResult runFill(BenchOptions options, Path dbDir, String name, boolean randomKeys) throws Exception {
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            TimedRun timed = runConcurrent(options.num(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                long keyId = randomKeys ? mix64(opIndex + options.seed()) : opIndex;
                handle.director().put(keyFor(keyId, options.keySize()), valueFor(keyId, options.valueSize()));
                flushPreparedIfNeeded(handle.director(), opIndex);
                return keyId;
            });
            verifySample(handle.director(), options, options.num(), randomKeys, false);
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runOverwrite(BenchOptions options, Path dbDir) throws Exception {
        String name = "overwrite";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preload(handle.director(), options, options.num(), false, options.prepareMode());
            TimedRun timed = runConcurrent(options.num(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                long keyId = randomKeyId(opIndex, options.seed()) % options.num();
                handle.director().put(keyFor(keyId, options.keySize()), valueFor(opIndex + options.num(), options.valueSize()));
                flushPreparedIfNeeded(handle.director(), opIndex);
                return keyId;
            });
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runReadRandom(BenchOptions options, Path dbDir) throws Exception {
        String name = "readrandom";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preload(handle.director(), options, options.num(), false, options.prepareMode());
            TimedRun timed = runConcurrent(options.reads(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                long keyId = randomKeyId(opIndex, options.seed()) % options.num();
                Optional<byte[]> value = handle.director().get(keyFor(keyId, options.keySize()));
                if (value.isEmpty()) {
                    throw new IllegalStateException("Expected HIT for key=" + keyId);
                }
                return firstByte(value.orElseThrow());
            });
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runReadMissing(BenchOptions options, Path dbDir) throws Exception {
        String name = "readmissing";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preloadEvenKeys(handle.director(), options, options.num() + 1, options.prepareMode());
            TimedRun timed = runConcurrent(options.reads(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                long keyId = 2 * (randomKeyId(opIndex, options.seed()) % options.num()) + 1;
                Optional<byte[]> value = handle.director().get(keyFor(keyId, options.keySize()));
                if (value.isPresent()) {
                    throw new IllegalStateException("Expected MISS for key=" + keyId);
                }
                return 0;
            });
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runReadWhileWriting(BenchOptions options, Path dbDir) throws Exception {
        String name = "readwhilewriting";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preload(handle.director(), options, options.num(), false, options.prepareMode());
            TimedRun timed = runConcurrent(options.num(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                if ((opIndex & 1) == 0) {
                    long keyId = randomKeyId(opIndex, options.seed()) % options.num();
                    Optional<byte[]> value = handle.director().get(keyFor(keyId, options.keySize()));
                    if (value.isEmpty()) {
                        throw new IllegalStateException("Expected HIT for key=" + keyId);
                    }
                    return firstByte(value.orElseThrow());
                }
                long keyId = options.num() + opIndex;
                handle.director().put(keyFor(keyId, options.keySize()), valueFor(opIndex, options.valueSize()));
                flushPreparedIfNeeded(handle.director(), opIndex);
                return keyId;
            });
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runDeleteRandom(BenchOptions options, Path dbDir) throws Exception {
        String name = "deleterandom";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preload(handle.director(), options, options.num(), false, options.prepareMode());
            TimedRun timed = runConcurrent(options.num(), options.threads(), options.latencySamples(), (opIndex, threadId) -> {
                long keyId = randomKeyId(opIndex, options.seed()) % options.num();
                handle.director().delete(keyFor(keyId, options.keySize()));
                flushPreparedIfNeeded(handle.director(), opIndex);
                return keyId;
            });
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runFlush(BenchOptions options, Path dbDir) throws Exception {
        String name = "flush";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            LatencySampler sampler = new LatencySampler(options.num(), options.latencySamples());
            long elapsedNanos = 0;
            for (long i = 0; i < options.num(); i++) {
                handle.director().put(keyFor(i, options.keySize()), valueFor(i, options.valueSize()));
                if ((i & 1023) == 1023 && handle.director().stateSnapshot().immutableMemTableCount() > 0) {
                    elapsedNanos += flushAllMeasured(handle.director(), sampler);
                }
            }
            handle.director().freezeCurMemTable();
            elapsedNanos += flushAllMeasured(handle.director(), sampler);
            TimedRun timed = new TimedRun(options.num(), elapsedNanos, sampler.snapshot());
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runCompact(BenchOptions options, Path dbDir) throws Exception {
        String name = "compact";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preloadManySsts(handle.director(), options, options.num());
            LatencySampler sampler = new LatencySampler(options.num(), options.latencySamples());
            long start = System.nanoTime();
            int previousSstCount;
            do {
                previousSstCount = handle.director().stateSnapshot().newSSTCount();
                long opStart = System.nanoTime();
                compactAllNewRuns(handle.director());
                sampler.record(0, System.nanoTime() - opStart);
            } while (handle.director().stateSnapshot().newSSTCount() < previousSstCount
                && handle.director().stateSnapshot().newSSTCount() >= options.compactMinFiles());
            TimedRun timed = new TimedRun(options.num(), System.nanoTime() - start, sampler.snapshot());
            return result(name, timed, handle.director(), caseDir);
        }
    }

    private BenchmarkResult runRecover(BenchOptions options, Path dbDir) throws Exception {
        String name = "recover";
        Path caseDir = prepareCaseDir(options, dbDir, name);
        try (DirectorHandle handle = openDirector(options, caseDir)) {
            preload(handle.director(), options, options.num(), false, PrepareMode.MEMTABLE);
        }

        LatencySampler sampler = new LatencySampler(options.num(), options.latencySamples());
        long opStart = System.nanoTime();
        long start = opStart;
        try (DirectorHandle recovered = openDirector(options, caseDir)) {
            sampler.record(0, System.nanoTime() - opStart);
            TimedRun timed = new TimedRun(options.num(), System.nanoTime() - start, sampler.snapshot());
            return result(name, timed, recovered.director(), caseDir);
        }
    }

    private static DirectorHandle openDirector(BenchOptions options, Path caseDir) throws IOException {
        Files.createDirectories(caseDir.resolve("wal"));
        Files.createDirectories(caseDir.resolve("storage"));
        PMSConfig config = new PMSConfig(
            new MemTableConfig(options.memtableMaxEntries(), options.memtableMaxSizeMb()),
            new WalConfig(caseDir.resolve("wal").toString(), options.walFileSizeMb(), options.useMmap()),
            new StorageConfig(
                caseDir.resolve("storage").toString(),
                options.sinkedMaxSizeMb(),
                options.sinkedMaxCount(),
                options.localSstMaxRows(),
                options.compactThresholdMb(),
                options.compactMinFiles()
            ),
            new SinkConfig(0, 0),
            new FlowControlConfig(0, 0),
            new PaimonConfig("benchmark-table", null)
        );
        PMSBucketDirectorImpl director = new PMSBucketDirectorImpl(
            config,
            storage -> new BenchmarkSinkManager()
        );
        director.init();
        return new DirectorHandle(director);
    }

    private static void preload(
            PMSBucketDirectorImpl director,
            BenchOptions options,
            long records,
            boolean randomKeys,
            PrepareMode prepareMode) {
        for (long i = 0; i < records; i++) {
            long keyId = randomKeys ? randomKeyId(i, options.seed()) : i;
            director.put(keyFor(keyId, options.keySize()), valueFor(keyId, options.valueSize()));
            if (shouldFlushPreparedImmutables(prepareMode, i)) {
                flushAll(director);
            }
        }
        if (prepareMode == PrepareMode.IMMUTABLE || prepareMode == PrepareMode.SST || prepareMode == PrepareMode.SINKED) {
            director.freezeCurMemTable();
        }
        if (prepareMode == PrepareMode.SST || prepareMode == PrepareMode.SINKED) {
            flushAll(director);
        }
        if (prepareMode == PrepareMode.SINKED) {
            director.sinkToPaimon(new SinkSelection(director.stateSnapshot().lastAssignedSequenceId(), Long.MAX_VALUE));
        }
        requirePreparedLayer(director.stateSnapshot(), prepareMode);
        verifySample(director, options, records, randomKeys, false);
    }

    private static void preloadEvenKeys(
            PMSBucketDirectorImpl director,
            BenchOptions options,
            long records,
            PrepareMode prepareMode) {
        for (long i = 0; i < records; i++) {
            long keyId = i * 2;
            director.put(keyFor(keyId, options.keySize()), valueFor(keyId, options.valueSize()));
            if (shouldFlushPreparedImmutables(prepareMode, i)) {
                flushAll(director);
            }
        }
        if (prepareMode == PrepareMode.IMMUTABLE || prepareMode == PrepareMode.SST || prepareMode == PrepareMode.SINKED) {
            director.freezeCurMemTable();
        }
        if (prepareMode == PrepareMode.SST || prepareMode == PrepareMode.SINKED) {
            flushAll(director);
        }
        if (prepareMode == PrepareMode.SINKED) {
            director.sinkToPaimon(new SinkSelection(director.stateSnapshot().lastAssignedSequenceId(), Long.MAX_VALUE));
        }
        requirePreparedLayer(director.stateSnapshot(), prepareMode);
        verifySample(director, options, records, false, true);
    }

    private static void requirePreparedLayer(BucketStateSnapshot state, PrepareMode mode) {
        boolean valid = switch (mode) {
            case MEMTABLE -> state.immutableMemTableCount() == 0 && state.newSSTCount() == 0
                && state.curMemTableEstimatedEntryCount() > 0;
            case IMMUTABLE -> state.curMemTableEstimatedEntryCount() == 0 && state.newSSTCount() == 0
                && state.immutableMemTableCount() > 0;
            case SST -> state.curMemTableEstimatedEntryCount() == 0 && state.immutableMemTableCount() == 0
                && state.newSSTCount() > 0;
            case SINKED -> state.curMemTableEstimatedEntryCount() == 0 && state.immutableMemTableCount() == 0
                && state.newSSTCount() == 0 && state.sinkedSSTCount() > 0;
        };
        if (!valid) throw new IllegalStateException("Requested prepare=" + mode + " but actual state=" + state);
    }

    private static void verifySample(PMSBucketDirectorImpl director, BenchOptions options,
            long records, boolean random, boolean even) {
        for (long i = 0; i < Math.min(records, 128); i++) {
            long index = i * (records - 1) / Math.max(1, Math.min(records, 128) - 1);
            long keyId = random ? mix64(index + options.seed()) : even ? index * 2 : index;
            byte[] actual = director.get(keyFor(keyId, options.keySize())).orElseThrow(
                () -> new IllegalStateException("Verification MISS for key=" + keyId));
            if (!Arrays.equals(valueFor(keyId, options.valueSize()), actual)) {
                throw new IllegalStateException("Verification value mismatch for key=" + keyId);
            }
        }
    }

    private static void compactAllNewRuns(PMSBucketDirectorImpl director) {
        List<Long> runIds = director.stateSnapshot().localRuns().stream()
            .filter(run -> run.state() == SSTState.NEW)
            .map(LocalRunSnapshot::runId)
            .toList();
        if (runIds.size() >= 2) {
            director.compactLocalSSTs(new CompactionSelection(SSTState.NEW, runIds));
        }
    }

    private static boolean shouldFlushPreparedImmutables(PrepareMode prepareMode, long recordIndex) {
        return (prepareMode == PrepareMode.SST || prepareMode == PrepareMode.SINKED)
            && (recordIndex & 1023) == 1023;
    }

    private static void flushPreparedIfNeeded(PMSBucketDirectorImpl director, long opIndex) {
        if ((opIndex & 1023) == 1023 && director.stateSnapshot().immutableMemTableCount() > 0) {
            flushAll(director);
        }
    }

    private static void preloadManySsts(PMSBucketDirectorImpl director, BenchOptions options, long records) {
        int sstCountTarget = Math.max(options.compactMinFiles(), 4);
        long recordsPerSst = Math.max(1, records / sstCountTarget);
        long written = 0;
        while (written < records) {
            long limit = Math.min(records, written + recordsPerSst);
            for (long i = written; i < limit; i++) {
                director.put(keyFor(i, options.keySize()), valueFor(i, options.valueSize()));
            }
            director.freezeCurMemTable();
            flushAll(director);
            written = limit;
        }
    }

    private static void flushAll(PMSBucketDirectorImpl director) {
        while (director.stateSnapshot().immutableMemTableCount() > 0) {
            director.flushImmutableMemTable();
        }
    }

    private static long flushAllMeasured(PMSBucketDirectorImpl director, LatencySampler sampler) {
        long elapsedNanos = 0;
        while (director.stateSnapshot().immutableMemTableCount() > 0) {
            long start = System.nanoTime();
            director.flushImmutableMemTable();
            long elapsed = System.nanoTime() - start;
            elapsedNanos += elapsed;
            sampler.record(0, elapsed);
        }
        return elapsedNanos;
    }

    static TimedRun runConcurrent(
            long totalOps,
            int threads,
            int latencySamples,
            BenchOperation operation) throws InterruptedException {
        if (totalOps <= 0) {
            return new TimedRun(0, 0, LatencySnapshot.empty());
        }
        int workerCount = Math.max(1, threads);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch doneGate = new CountDownLatch(workerCount);
        AtomicLong nextOp = new AtomicLong();
        AtomicLong checksum = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        LatencySampler sampler = new LatencySampler(totalOps, latencySamples);

        for (int threadId = 0; threadId < workerCount; threadId++) {
            int currentThreadId = threadId;
            executor.submit(() -> {
                try {
                    startGate.await();
                    while (failure.get() == null && !Thread.currentThread().isInterrupted()) {
                        long startIndex = nextOp.getAndAdd(OP_CHUNK);
                        if (startIndex >= totalOps) {
                            return;
                        }
                        long endIndex = Math.min(totalOps, startIndex + OP_CHUNK);
                        long localChecksum = 0;
                        for (long opIndex = startIndex; opIndex < endIndex; opIndex++) {
                            if (sampler.shouldSample(opIndex)) {
                                long start = System.nanoTime();
                                localChecksum += operation.run(opIndex, currentThreadId);
                                sampler.record(opIndex, System.nanoTime() - start);
                            } else {
                                localChecksum += operation.run(opIndex, currentThreadId);
                            }
                        }
                        checksum.addAndGet(localChecksum);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    doneGate.countDown();
                }
            });
        }

        long start = System.nanoTime();
        startGate.countDown();
        long elapsed;
        try {
            doneGate.await();
            elapsed = System.nanoTime() - start;
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        blackhole ^= checksum.get();
        if (failure.get() != null) {
            throw new IllegalStateException("Benchmark worker failed", failure.get());
        }
        return new TimedRun(totalOps, elapsed, sampler.snapshot());
    }

    private static BenchmarkResult result(String name, TimedRun timed, PMSBucketDirectorImpl director, Path caseDir) {
        BucketStateSnapshot state = director.stateSnapshot();
        return new BenchmarkResult(
            name,
            timed.operations(),
            timed.elapsedNanos(),
            timed.latency(),
            directorySize(caseDir),
            state
        );
    }

    private static Path prepareCaseDir(BenchOptions options, Path dbDir, String benchmarkName) throws IOException {
        return Files.createDirectory(dbDir.resolve(benchmarkName));
    }

    private static byte[] keyFor(long id, int keySize) {
        byte[] key = new byte[keySize];
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(id);
        byte[] idBytes = buffer.array();
        int copy = Math.min(key.length, idBytes.length);
        System.arraycopy(idBytes, idBytes.length - copy, key, key.length - copy, copy);
        return key;
    }

    private static byte[] valueFor(long id, int valueSize) {
        byte[] value = new byte[valueSize];
        long x = mix64(id);
        for (int i = 0; i < value.length; i++) {
            if ((i & 7) == 0) {
                x = mix64(x + i);
            }
            value[i] = (byte) (x >>> ((i & 7) * 8));
        }
        return value;
    }

    private static int firstByte(byte[] bytes) {
        return bytes.length == 0 ? 0 : bytes[0] & 0xFF;
    }

    private static long randomKeyId(long opIndex, long seed) {
        return Long.remainderUnsigned(mix64(opIndex + seed), Long.MAX_VALUE);
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    private static long directorySize(Path dir) {
        if (!Files.exists(dir)) {
            return 0;
        }
        try (var stream = Files.walk(dir)) {
            return stream
                .filter(Files::isRegularFile)
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .sum();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        Path normalized = path.toAbsolutePath().normalize();
        if (normalized.getParent() == null) {
            throw new IllegalArgumentException("Refuse to delete root directory: " + path);
        }
        try (var stream = Files.walk(normalized)) {
            List<Path> paths = stream.sorted(Comparator.reverseOrder()).toList();
            for (Path candidate : paths) {
                Files.deleteIfExists(candidate);
            }
        }
    }

    private enum PrepareMode {
        MEMTABLE,
        IMMUTABLE,
        SST,
        SINKED;

        static PrepareMode parse(String value) {
            return Arrays.stream(values())
                .filter(mode -> mode.name().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown prepare mode: " + value));
        }
    }

    interface BenchOperation {
        long run(long opIndex, int threadId);
    }

    private record TimedRun(long operations, long elapsedNanos, LatencySnapshot latency) {}

    private record BenchmarkResult(
        String name,
        long operations,
        long elapsedNanos,
        LatencySnapshot latency,
        long dbBytes,
        BucketStateSnapshot state
    ) {
        String toCsv() {
            double seconds = elapsedNanos / 1_000_000_000.0;
            double opsPerSec = seconds == 0 ? 0 : operations / seconds;
            return String.format(
                Locale.ROOT,
                "%s,%d,%.6f,%.2f,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%d,%d",
                name,
                operations,
                seconds,
                opsPerSec,
                latency.p50Millis(),
                latency.p95Millis(),
                latency.p99Millis(),
                latency.maxMillis(),
                dbBytes,
                state.newSSTCount(),
                state.sinkedSSTCount(),
                state.immutableMemTableCount(),
                state.curMemTableEstimatedEntryCount()
            );
        }
    }

    private static final class DirectorHandle implements AutoCloseable {
        private final PMSBucketDirectorImpl director;

        private DirectorHandle(PMSBucketDirectorImpl director) {
            this.director = director;
        }

        PMSBucketDirectorImpl director() {
            return director;
        }

        @Override
        public void close() {
            director.close();
        }
    }

    private static final class LatencySampler {
        private final long sampleEvery;
        private final long[] samples;
        private final AtomicInteger nextSample = new AtomicInteger();

        LatencySampler(long totalOps, int maxSamples) {
            int sampleCount = (int) Math.max(1, Math.min(totalOps, Math.max(1, maxSamples)));
            this.sampleEvery = Math.max(1, (long) Math.ceil(totalOps / (double) sampleCount));
            this.samples = new long[sampleCount];
        }

        boolean shouldSample(long opIndex) {
            return opIndex % sampleEvery == 0;
        }

        void record(long opIndex, long elapsedNanos) {
            int index = nextSample.getAndIncrement();
            if (index < samples.length) {
                samples[index] = elapsedNanos;
            }
        }

        LatencySnapshot snapshot() {
            int count = Math.min(nextSample.get(), samples.length);
            if (count == 0) {
                return LatencySnapshot.empty();
            }
            long[] copy = Arrays.copyOf(samples, count);
            Arrays.sort(copy);
            return new LatencySnapshot(
                millis(copy[percentileIndex(count, 0.50)]),
                millis(copy[percentileIndex(count, 0.95)]),
                millis(copy[percentileIndex(count, 0.99)]),
                millis(copy[count - 1])
            );
        }

        private static int percentileIndex(int count, double percentile) {
            return Math.min(count - 1, Math.max(0, (int) Math.ceil(count * percentile) - 1));
        }

        private static double millis(long nanos) {
            return nanos / 1_000_000.0;
        }
    }

    private record LatencySnapshot(double p50Millis, double p95Millis, double p99Millis, double maxMillis) {
        static LatencySnapshot empty() {
            return new LatencySnapshot(0, 0, 0, 0);
        }
    }

    private record BenchOptions(
        List<String> benchmarks,
        Path dbDir,
        boolean deleteTempDb,
        long num,
        long reads,
        int warmupRuns,
        int runs,
        int threads,
        int keySize,
        int valueSize,
        int latencySamples,
        PrepareMode prepareMode,
        int memtableMaxEntries,
        int memtableMaxSizeMb,
        int walFileSizeMb,
        boolean useMmap,
        long sinkedMaxSizeMb,
        int sinkedMaxCount,
        long localSstMaxRows,
        int compactThresholdMb,
        int compactMinFiles,
        long seed
    ) {
        static BenchOptions parse(String[] args) {
            Map<String, String> values = parseArgs(args);
            String benchmarkCsv = values.getOrDefault("benchmarks", DEFAULT_BENCHMARKS);
            List<String> benchmarks = Arrays.stream(benchmarkCsv.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .toList();
            if (benchmarks.isEmpty()) {
                throw new IllegalArgumentException("At least one benchmark must be configured");
            }

            Set<String> known = Set.of("benchmarks", "db", "delete_temp_db", "num", "reads", "warmup_runs", "runs",
                "threads", "key_size", "value_size", "latency_samples", "prepare", "memtable_max_entries",
                "memtable_max_size_mb", "wal_file_size_mb", "use_mmap", "sinked_max_size_mb", "sinked_max_count",
                "local_sst_max_rows", "compact_threshold_mb", "compact_min_files", "seed");
            for (String key : values.keySet()) {
                if (!known.contains(key)) throw new IllegalArgumentException("Unknown option: " + key);
            }
            Set<String> cases = Set.of("fillseq", "fillrandom", "overwrite", "readrandom", "readmissing",
                "readwhilewriting", "deleterandom", "flush", "compact", "recover");
            if (!cases.containsAll(benchmarks) || Set.copyOf(benchmarks).size() != benchmarks.size()) {
                throw new IllegalArgumentException("Unknown or duplicate benchmark: " + benchmarks);
            }
            if (positiveInt(values, "key_size", DEFAULT_KEY_SIZE) < Long.BYTES) {
                throw new IllegalArgumentException("key_size must be >= 8 to preserve unique keys");
            }
            String dbValue = values.get("db");
            Path dbDir = dbValue == null ? null : Path.of(dbValue);

            return new BenchOptions(
                benchmarks,
                dbDir,
                bool(values, "delete_temp_db", false),
                positiveLong(values, "num", DEFAULT_NUM),
                positiveLong(values, "reads", positiveLong(values, "num", DEFAULT_NUM)),
                nonNegativeInt(values, "warmup_runs", 1),
                positiveInt(values, "runs", 3),
                positiveInt(values, "threads", DEFAULT_THREADS),
                positiveInt(values, "key_size", DEFAULT_KEY_SIZE),
                nonNegativeInt(values, "value_size", DEFAULT_VALUE_SIZE),
                positiveInt(values, "latency_samples", DEFAULT_LATENCY_SAMPLES),
                PrepareMode.parse(values.getOrDefault("prepare", "sst")),
                positiveInt(values, "memtable_max_entries", MemTableConfig.DEFAULT_MAX_ENTRIES),
                positiveInt(values, "memtable_max_size_mb", MemTableConfig.DEFAULT_MAX_SIZE_MB),
                positiveInt(values, "wal_file_size_mb", WalConfig.DEFAULT_FILE_SIZE_MB),
                bool(values, "use_mmap", WalConfig.DEFAULT_USE_MMAP),
                positiveLong(values, "sinked_max_size_mb", StorageConfig.DEFAULT_SINKED_MAX_SIZE_MB),
                positiveInt(values, "sinked_max_count", StorageConfig.DEFAULT_SINKED_MAX_COUNT),
                nonNegativeLong(values, "local_sst_max_rows", StorageConfig.DEFAULT_LOCAL_SST_MAX_ROWS),
                positiveInt(values, "compact_threshold_mb", StorageConfig.DEFAULT_COMPACT_THRESHOLD_MB),
                positiveInt(values, "compact_min_files", StorageConfig.DEFAULT_COMPACT_MIN_FILES),
                longValue(values, "seed", 0x5eedL)
            );
        }

        String summary() {
            return "benchmarks=" + benchmarks
                + " num=" + num
                + " threads=" + threads
                + " key_size=" + keySize
                + " value_size=" + valueSize
                + " prepare=" + prepareMode
                + " memtable_max_entries=" + memtableMaxEntries
                + " compact_min_files=" + compactMinFiles;
        }

        private static Map<String, String> parseArgs(String[] args) {
            Map<String, String> result = new LinkedHashMap<>();
            for (String arg : args) {
                if (Objects.equals(arg, "--help") || Objects.equals(arg, "-h")) {
                    printHelpAndExit();
                }
                if (!arg.startsWith("--")) {
                    throw new IllegalArgumentException("Arguments must use --key=value form: " + arg);
                }
                int equals = arg.indexOf('=');
                if (equals < 0) {
                    result.put(arg.substring(2), "true");
                } else {
                    result.put(arg.substring(2, equals), arg.substring(equals + 1));
                }
            }
            return result;
        }

        private static void printHelpAndExit() {
            String help = """
                Usage:
                  java -jar pms-tests/pms-benchmark/target/pms-benchmark-0.1-SNAPSHOT-runner.jar --benchmarks=fillrandom,readrandom

                Common options:
                  --benchmarks=fillseq,fillrandom,overwrite,readrandom,readmissing,readwhilewriting,deleterandom,flush,compact,recover
                  --db=/path/to/db
                  --delete_temp_db=true|false
                  --warmup_runs=1
                  --runs=3
                  --reads=1000000
                  --num=100000
                  --threads=1
                  --key_size=16
                  --value_size=100
                  --prepare=memtable|immutable|sst|sinked
                  --memtable_max_entries=1000000
                  --compact_min_files=4
                """;
            System.out.println(help);
            System.exit(0);
        }

        private static boolean bool(Map<String, String> values, String key, boolean defaultValue) {
            return values.containsKey(key) ? Boolean.parseBoolean(values.get(key)) : defaultValue;
        }

        private static int positiveInt(Map<String, String> values, String key, int defaultValue) {
            int value = intValue(values, key, defaultValue);
            if (value <= 0) {
                throw new IllegalArgumentException(key + " must be > 0");
            }
            return value;
        }

        private static int nonNegativeInt(Map<String, String> values, String key, int defaultValue) {
            int value = intValue(values, key, defaultValue);
            if (value < 0) {
                throw new IllegalArgumentException(key + " must be >= 0");
            }
            return value;
        }

        private static int intValue(Map<String, String> values, String key, int defaultValue) {
            return values.containsKey(key) ? Integer.parseInt(values.get(key)) : defaultValue;
        }

        private static long positiveLong(Map<String, String> values, String key, long defaultValue) {
            long value = longValue(values, key, defaultValue);
            if (value <= 0) {
                throw new IllegalArgumentException(key + " must be > 0");
            }
            return value;
        }

        private static long nonNegativeLong(Map<String, String> values, String key, long defaultValue) {
            long value = longValue(values, key, defaultValue);
            if (value < 0) {
                throw new IllegalArgumentException(key + " must be >= 0");
            }
            return value;
        }

        private static long longValue(Map<String, String> values, String key, long defaultValue) {
            return values.containsKey(key) ? Long.parseLong(values.get(key)) : defaultValue;
        }
    }
}
