package org.qwh.pms.core.storage;

public record BlockHandle(long offset, long size) {
    public static final int MAX_ENCODED_LENGTH = 20;

    public BlockHandle {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative");
        }
    }
}
