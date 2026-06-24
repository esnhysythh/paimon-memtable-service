package org.qwh.pms.lookup.router;

/** Point-in-time functional counters for the threshold local-cache router. */
public record ThresholdFileLookupRouterStats(
        long directLookups,
        long localLookups,
        long buildsScheduled,
        long buildsSucceeded,
        long buildsFailed,
        long buildsTimedOut,
        long buildsRejected,
        long entriesEvicted,
        long readyEntries,
        long cacheBytes,
        long inFlightBuilds) {}
