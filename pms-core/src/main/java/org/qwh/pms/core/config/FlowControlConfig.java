package org.qwh.pms.core.config;

public record FlowControlConfig(
    int overloadedImmutableCount,
    int overloadedPendingSstCount
) {
    public static final int DEFAULT_OVERLOADED_IMMUTABLE_COUNT = 4;
    public static final int DEFAULT_OVERLOADED_PENDING_SST_COUNT = 20;

    public FlowControlConfig {
        if (overloadedImmutableCount <= 0) overloadedImmutableCount = DEFAULT_OVERLOADED_IMMUTABLE_COUNT;
        if (overloadedPendingSstCount <= 0) overloadedPendingSstCount = DEFAULT_OVERLOADED_PENDING_SST_COUNT;
    }
}
