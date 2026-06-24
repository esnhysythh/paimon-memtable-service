package org.qwh.pms.lookup.router;

import java.time.Duration;

/** Resource and retry bounds for {@link ThresholdFileLookupRouter}. */
public record ThresholdFileLookupRouterOptions(
        int buildThreshold,
        int maxInFlightBuilds,
        long maxCacheBytes,
        Duration buildTimeout,
        Duration retryBackoff) {

    public ThresholdFileLookupRouterOptions {
        if (buildThreshold <= 0) {
            throw new IllegalArgumentException("buildThreshold must be positive");
        }
        if (maxInFlightBuilds <= 0) {
            throw new IllegalArgumentException("maxInFlightBuilds must be positive");
        }
        if (maxCacheBytes < 0) {
            throw new IllegalArgumentException("maxCacheBytes must not be negative");
        }
        if (buildTimeout.isNegative() || buildTimeout.isZero()) {
            throw new IllegalArgumentException("buildTimeout must be positive");
        }
        if (retryBackoff.isNegative()) {
            throw new IllegalArgumentException("retryBackoff must not be negative");
        }
    }

    public static ThresholdFileLookupRouterOptions defaults() {
        return new ThresholdFileLookupRouterOptions(
                ThresholdFileLookupRouter.DEFAULT_BUILD_THRESHOLD,
                4,
                10L * 1024 * 1024 * 1024,
                Duration.ofMinutes(5),
                Duration.ofSeconds(30));
    }

    public ThresholdFileLookupRouterOptions withBuildThreshold(int threshold) {
        return new ThresholdFileLookupRouterOptions(
                threshold, maxInFlightBuilds, maxCacheBytes, buildTimeout, retryBackoff);
    }
}
