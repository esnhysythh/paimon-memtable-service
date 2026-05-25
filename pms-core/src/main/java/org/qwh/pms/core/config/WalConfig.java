package org.qwh.pms.core.config;

public record WalConfig(
    String dir,
    int fileSizeMb,
    boolean useMmap
) {
    public static final int DEFAULT_FILE_SIZE_MB = 256;
    public static final boolean DEFAULT_USE_MMAP = false;

    public WalConfig {
        if (dir == null || dir.isBlank()) {
            throw new IllegalArgumentException("WAL directory (walDir) must not be empty");
        }
        if (fileSizeMb <= 0) fileSizeMb = DEFAULT_FILE_SIZE_MB;
    }

    public long fileSizeBytes() {
        return (long) fileSizeMb * 1024 * 1024;
    }
}
