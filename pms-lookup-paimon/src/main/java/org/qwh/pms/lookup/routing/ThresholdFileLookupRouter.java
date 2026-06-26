package org.qwh.pms.lookup.routing;

import org.apache.paimon.io.DataFileMeta;
import org.qwh.pms.lookup.api.DataFileLookup;
import org.qwh.pms.lookup.api.FileLookupContext;
import org.qwh.pms.lookup.api.LookupRequest;
import org.qwh.pms.lookup.api.LookupResult;
import org.qwh.pms.lookup.cache.LocalCacheBuildContext;
import org.qwh.pms.lookup.cache.LocalCacheBuilder;
import org.qwh.pms.lookup.cache.LocalCacheEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Routes candidate files to direct lookup until the access threshold triggers a bounded,
 * asynchronous local cache build.
 */
public final class ThresholdFileLookupRouter implements DataFileLookup, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ThresholdFileLookupRouter.class);
    public static final int DEFAULT_BUILD_THRESHOLD = 3;

    private final DataFileLookup directLookup;
    private final LocalCacheBuilder localCacheBuilder;
    private final Executor buildExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final boolean ownsTimeoutExecutor;
    private final ThresholdFileLookupRouterOptions options;
    private final Object stateLock = new Object();
    private final Map<FileCacheKey, FileCacheState> states = new HashMap<>();

    private long directLookups;
    private long localLookups;
    private long buildsScheduled;
    private long buildsSucceeded;
    private long buildsFailed;
    private long buildsTimedOut;
    private long buildsRejected;
    private long entriesEvicted;
    private long cacheBytes;
    private long inFlightBuilds;
    private boolean closed;

    public ThresholdFileLookupRouter(
            DataFileLookup directLookup, LocalCacheBuilder localCacheBuilder, Executor buildExecutor) {
        this(
                directLookup,
                localCacheBuilder,
                buildExecutor,
                ThresholdFileLookupRouterOptions.defaults());
    }

    public ThresholdFileLookupRouter(
            DataFileLookup directLookup,
            LocalCacheBuilder localCacheBuilder,
            Executor buildExecutor,
            int buildThreshold) {
        this(
                directLookup,
                localCacheBuilder,
                buildExecutor,
                ThresholdFileLookupRouterOptions.defaults().withBuildThreshold(buildThreshold));
    }

    public ThresholdFileLookupRouter(
            DataFileLookup directLookup,
            LocalCacheBuilder localCacheBuilder,
            Executor buildExecutor,
            ThresholdFileLookupRouterOptions options) {
        this(
                directLookup,
                localCacheBuilder,
                buildExecutor,
                newTimeoutExecutor(),
                options,
                true);
    }

    public ThresholdFileLookupRouter(
            DataFileLookup directLookup,
            LocalCacheBuilder localCacheBuilder,
            Executor buildExecutor,
            ScheduledExecutorService timeoutExecutor,
            ThresholdFileLookupRouterOptions options) {
        this(directLookup, localCacheBuilder, buildExecutor, timeoutExecutor, options, false);
    }

    private ThresholdFileLookupRouter(
            DataFileLookup directLookup,
            LocalCacheBuilder localCacheBuilder,
            Executor buildExecutor,
            ScheduledExecutorService timeoutExecutor,
            ThresholdFileLookupRouterOptions options,
            boolean ownsTimeoutExecutor) {
        this.directLookup = Objects.requireNonNull(directLookup, "directLookup");
        this.localCacheBuilder = Objects.requireNonNull(localCacheBuilder, "localCacheBuilder");
        this.buildExecutor = Objects.requireNonNull(buildExecutor, "buildExecutor");
        this.timeoutExecutor = Objects.requireNonNull(timeoutExecutor, "timeoutExecutor");
        this.ownsTimeoutExecutor = ownsTimeoutExecutor;
        this.options = Objects.requireNonNull(options, "options");
    }

    @Override
    public LookupResult lookup(FileLookupContext context, DataFileMeta file, LookupRequest request)
            throws IOException {
        FileCacheKey key = FileCacheKey.of(context, file);
        BuildSubmission buildSubmission = null;
        LocalCacheEntry readyEntry;
        long now = System.nanoTime();

        synchronized (stateLock) {
            ensureOpen();
            FileCacheState state = states.computeIfAbsent(key, ignored -> new FileCacheState());
            if (state.status == Status.FAILED && now >= state.retryAfterNanos) {
                state.status = Status.ABSENT;
            }
            if (state.status == Status.ABSENT) {
                state.candidateAccessCount++;
                if (state.candidateAccessCount >= options.buildThreshold()) {
                    buildSubmission = tryStartBuild(key, state, context, file, now);
                }
            }
            readyEntry = state.readyEntry;
            if (readyEntry != null) {
                state.lastAccessNanos = now;
            }
        }

        if (buildSubmission != null) {
            submitBuild(buildSubmission);
        }

        if (readyEntry == null) {
            incrementDirectLookups();
            LOG.debug(
                    "Routing Paimon lookup to direct path: partition={}, bucket={}, file={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName());
            return directLookup.lookup(context, file, request);
        }

        try {
            LookupResult result = readyEntry.lookup(request);
            incrementLocalLookups();
            if (result.kind() != LookupResult.Kind.UNKNOWN) {
                LOG.debug(
                        "Routing Paimon lookup to local cache: partition={}, bucket={}, file={}, result={}",
                        context.partition(),
                        context.bucket(),
                        file.fileName(),
                        result.kind());
                return result;
            }
        } catch (IOException | RuntimeException e) {
            // A local cache failure can safely fall back to the immutable source data file.
            LOG.warn(
                    "Paimon local cache lookup failed; falling back to direct path: partition={}, bucket={}, file={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName(),
                    e);
        }

        discardReadyEntry(key, readyEntry, now);
        incrementDirectLookups();
        return directLookup.lookup(context, file, request);
    }

    @Override
    public void invalidate(FileLookupContext context, DataFileMeta file) {
        StateRemoval removal;
        synchronized (stateLock) {
            FileCacheState state = states.remove(FileCacheKey.of(context, file));
            removal = removeState(state);
        }
        try {
            directLookup.invalidate(context, file);
        } finally {
            cancelQuietly(removal.buildTask());
            closeQuietly(removal.entry());
        }
    }

    @Override
    public void invalidateBucket(FileLookupContext context) {
        List<StateRemoval> removals = new ArrayList<>();
        synchronized (stateLock) {
            Iterator<Map.Entry<FileCacheKey, FileCacheState>> iterator = states.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<FileCacheKey, FileCacheState> entry = iterator.next();
                if (entry.getKey().context.equals(context)) {
                    iterator.remove();
                    removals.add(removeState(entry.getValue()));
                }
            }
        }
        try {
            directLookup.invalidateBucket(context);
        } finally {
            removals.forEach(removal -> cancelQuietly(removal.buildTask()));
            removals.forEach(removal -> closeQuietly(removal.entry()));
        }
    }

    public ThresholdFileLookupRouterStats stats() {
        synchronized (stateLock) {
            long readyEntries = states.values().stream().filter(state -> state.readyEntry != null).count();
            return new ThresholdFileLookupRouterStats(
                    directLookups,
                    localLookups,
                    buildsScheduled,
                    buildsSucceeded,
                    buildsFailed,
                    buildsTimedOut,
                    buildsRejected,
                    entriesEvicted,
                    readyEntries,
                    cacheBytes,
                    inFlightBuilds);
        }
    }

    @Override
    public void close() {
        List<StateRemoval> removals = new ArrayList<>();
        synchronized (stateLock) {
            if (closed) {
                return;
            }
            closed = true;
            for (FileCacheState state : states.values()) {
                removals.add(removeState(state));
            }
            states.clear();
        }
        removals.forEach(removal -> cancelQuietly(removal.buildTask()));
        removals.forEach(removal -> closeQuietly(removal.entry()));
        if (ownsTimeoutExecutor) {
            timeoutExecutor.shutdownNow();
        }
    }

    private BuildSubmission tryStartBuild(
            FileCacheKey key,
            FileCacheState state,
            FileLookupContext context,
            DataFileMeta file,
            long now) {
        if (inFlightBuilds >= options.maxInFlightBuilds()) {
            markBuildFailed(state, now);
            buildsRejected++;
            LOG.debug(
                    "Reject Paimon local cache build because in-flight limit is reached: partition={}, bucket={}, file={}, maxInFlight={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName(),
                    options.maxInFlightBuilds());
            return null;
        }
        state.status = Status.BUILDING;
        state.buildStartedNanos = now;
        long generation = ++state.generation;
        BuildSubmission submission = new BuildSubmission(key, state, context, file, generation);
        state.build = submission;
        submission.slotHeld = true;
        inFlightBuilds++;
        buildsScheduled++;
        try {
            submission.timeoutFuture =
                    timeoutExecutor.schedule(
                            () -> timeoutBuild(submission),
                            options.buildTimeout().toNanos(),
                            TimeUnit.NANOSECONDS);
            LOG.info(
                    "Scheduled Paimon local cache build: partition={}, bucket={}, file={}, threshold={}, inFlight={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName(),
                    options.buildThreshold(),
                    inFlightBuilds);
        } catch (RuntimeException e) {
            state.build = null;
            releaseBuildSlot(submission);
            markBuildFailed(state, now);
            buildsFailed++;
            LOG.warn(
                    "Failed to schedule Paimon local cache build: partition={}, bucket={}, file={}",
                    context.partition(),
                    context.bucket(),
                    file.fileName(),
                    e);
            return null;
        }
        return submission;
    }

    private void submitBuild(BuildSubmission submission) {
        try {
            buildExecutor.execute(submission.task());
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to submit Paimon local cache build: partition={}, bucket={}, file={}",
                    submission.context.partition(),
                    submission.context.bucket(),
                    submission.file.fileName(),
                    e);
            completeBuild(submission, null, true);
        }
    }

    private void buildAndPublish(BuildSubmission submission) {
        LocalCacheEntry entry = null;
        boolean failed = false;
        try {
            entry =
                    localCacheBuilder.build(
                            submission.file,
                            new LocalCacheBuildContext(
                                    submission.context.partition(), submission.context.bucket()));
        } catch (IOException | RuntimeException e) {
            failed = true;
            LOG.warn(
                    "Paimon local cache build failed: partition={}, bucket={}, file={}",
                    submission.context.partition(),
                    submission.context.bucket(),
                    submission.file.fileName(),
                    e);
        }
        completeBuild(submission, entry, failed);
    }

    private void completeBuild(
            BuildSubmission submission, LocalCacheEntry builtEntry, boolean failed) {
        List<LocalCacheEntry> entriesToClose = new ArrayList<>();
        synchronized (stateLock) {
            if (submission.state.build == submission) {
                submission.state.build = null;
            }
            releaseBuildSlot(submission);
            cancelTimeout(submission);
            FileCacheState state = states.get(submission.key);
            if (failed
                    || builtEntry == null
                    || state != submission.state
                    || state.status != Status.BUILDING
                    || state.generation != submission.generation) {
                if (state == submission.state
                        && state.status == Status.BUILDING
                        && state.generation == submission.generation) {
                    markBuildFailed(state, System.nanoTime());
                    buildsFailed++;
                }
                if (builtEntry != null) {
                    entriesToClose.add(builtEntry);
                }
            } else {
                long entryBytes = Math.max(0L, builtEntry.sizeBytes());
                if (entryBytes > options.maxCacheBytes()) {
                    markBuildFailed(state, System.nanoTime());
                    buildsRejected++;
                    entriesToClose.add(builtEntry);
                } else {
                    evictUntilFits(submission.key, entryBytes, entriesToClose);
                    if (cacheBytes + entryBytes > options.maxCacheBytes()) {
                        markBuildFailed(state, System.nanoTime());
                        buildsRejected++;
                        entriesToClose.add(builtEntry);
                    } else {
                        state.readyEntry = builtEntry;
                        state.entryBytes = entryBytes;
                        state.status = Status.READY;
                        state.lastAccessNanos = System.nanoTime();
                        cacheBytes += entryBytes;
                        buildsSucceeded++;
                        LOG.info(
                                "Paimon local cache build succeeded: partition={}, bucket={}, file={}, entryBytes={}, cacheBytes={}",
                                submission.context.partition(),
                                submission.context.bucket(),
                                submission.file.fileName(),
                                entryBytes,
                                cacheBytes);
                    }
                }
            }
        }
        entriesToClose.forEach(ThresholdFileLookupRouter::closeQuietly);
    }

    private void evictUntilFits(
            FileCacheKey protectedKey, long incomingBytes, List<LocalCacheEntry> entriesToClose) {
        while (cacheBytes + incomingBytes > options.maxCacheBytes()) {
            Map.Entry<FileCacheKey, FileCacheState> victim =
                    states.entrySet().stream()
                            .filter(entry -> !entry.getKey().equals(protectedKey))
                            .filter(entry -> entry.getValue().readyEntry != null)
                            .min(
                                    java.util.Comparator.comparingLong(
                                            entry -> entry.getValue().lastAccessNanos))
                            .orElse(null);
            if (victim == null) {
                return;
            }
            states.remove(victim.getKey());
            StateRemoval removal = removeState(victim.getValue());
            if (removal.entry() != null) {
                entriesToClose.add(removal.entry());
                entriesEvicted++;
                LOG.info(
                        "Evicted Paimon local cache entry: partition={}, bucket={}, file={}, cacheBytes={}",
                        victim.getKey().context.partition(),
                        victim.getKey().context.bucket(),
                        victim.getKey().fileName(),
                        cacheBytes);
            }
        }
    }

    private void discardReadyEntry(FileCacheKey key, LocalCacheEntry expectedEntry, long now) {
        LocalCacheEntry entryToClose = null;
        synchronized (stateLock) {
            FileCacheState state = states.get(key);
            if (state != null && state.status == Status.READY && state.readyEntry == expectedEntry) {
                entryToClose = removeReadyEntry(state);
                markBuildFailed(state, now);
                buildsFailed++;
            }
        }
        closeQuietly(entryToClose);
    }

    private void markBuildFailed(FileCacheState state, long now) {
        state.status = Status.FAILED;
        state.failedAttempts++;
        state.retryAfterNanos = saturatingAdd(now, options.retryBackoff().toNanos());
    }

    private StateRemoval removeState(FileCacheState state) {
        if (state == null) {
            return StateRemoval.EMPTY;
        }
        state.generation++;
        state.status = Status.REMOVED;
        BuildSubmission build = state.build;
        state.build = null;
        releaseBuildSlot(build);
        cancelTimeout(build);
        return new StateRemoval(removeReadyEntry(state), build == null ? null : build.task());
    }

    private LocalCacheEntry removeReadyEntry(FileCacheState state) {
        LocalCacheEntry entry = state.readyEntry;
        if (entry != null) {
            cacheBytes -= state.entryBytes;
            state.readyEntry = null;
            state.entryBytes = 0L;
        }
        return entry;
    }

    private void releaseBuildSlot(BuildSubmission submission) {
        if (submission != null && submission.slotHeld) {
            submission.slotHeld = false;
            inFlightBuilds--;
        }
    }

    private void timeoutBuild(BuildSubmission submission) {
        FutureTask<Void> buildTask = null;
        synchronized (stateLock) {
            FileCacheState state = states.get(submission.key);
            if (state != submission.state
                    || state.status != Status.BUILDING
                    || state.generation != submission.generation
                    || state.build != submission) {
                return;
            }
            state.build = null;
            releaseBuildSlot(submission);
            state.generation++;
            markBuildFailed(state, System.nanoTime());
            buildsTimedOut++;
            buildTask = submission.task();
            LOG.warn(
                    "Paimon local cache build timed out: partition={}, bucket={}, file={}, timeout={}",
                    submission.context.partition(),
                    submission.context.bucket(),
                    submission.file.fileName(),
                    options.buildTimeout());
        }
        cancelQuietly(buildTask);
    }

    private static void cancelTimeout(BuildSubmission submission) {
        if (submission != null && submission.timeoutFuture != null) {
            submission.timeoutFuture.cancel(false);
        }
    }

    private static ScheduledExecutorService newTimeoutExecutor() {
        ThreadFactory factory =
                runnable -> {
                    Thread thread = new Thread(runnable, "paimon-lookup-timeout");
                    thread.setDaemon(true);
                    return thread;
                };
        return Executors.newSingleThreadScheduledExecutor(factory);
    }

    private void incrementDirectLookups() {
        synchronized (stateLock) {
            directLookups++;
        }
    }

    private void incrementLocalLookups() {
        synchronized (stateLock) {
            localLookups++;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ThresholdFileLookupRouter is closed");
        }
    }

    private static long saturatingAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void closeQuietly(LocalCacheEntry entry) {
        if (entry == null) {
            return;
        }
        try {
            entry.close();
        } catch (IOException ignored) {
            // File removal must not prevent the live file view from progressing.
        }
    }

    private static void cancelQuietly(FutureTask<Void> task) {
        if (task != null) {
            task.cancel(true);
        }
    }

    private enum Status {
        ABSENT,
        BUILDING,
        READY,
        FAILED,
        REMOVED
    }

    private static final class FileCacheState {

        private int candidateAccessCount;
        private int failedAttempts;
        private long generation;
        private long buildStartedNanos;
        private long retryAfterNanos;
        private long lastAccessNanos;
        private long entryBytes;
        private Status status = Status.ABSENT;
        private LocalCacheEntry readyEntry;
        private BuildSubmission build;
    }

    private final class BuildSubmission {

        private final FileCacheKey key;
        private final FileCacheState state;
        private final FileLookupContext context;
        private final DataFileMeta file;
        private final long generation;
        private final FutureTask<Void> task;
        private boolean slotHeld;
        private ScheduledFuture<?> timeoutFuture;

        private BuildSubmission(
                FileCacheKey key,
                FileCacheState state,
                FileLookupContext context,
                DataFileMeta file,
                long generation) {
            this.key = key;
            this.state = state;
            this.context = context;
            this.file = file;
            this.generation = generation;
            this.task = new FutureTask<>(() -> buildAndPublish(this), null);
        }

        private FutureTask<Void> task() {
            return task;
        }
    }

    private record StateRemoval(LocalCacheEntry entry, FutureTask<Void> buildTask) {

        private static final StateRemoval EMPTY = new StateRemoval(null, null);
    }

    private record FileCacheKey(
            FileLookupContext context, String fileName, long fileSize, long schemaId) {

        private static FileCacheKey of(FileLookupContext context, DataFileMeta file) {
            return new FileCacheKey(context, file.fileName(), file.fileSize(), file.schemaId());
        }
    }
}
