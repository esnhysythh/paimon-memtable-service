package org.qwh.pms.core.storage;

final class SSTFormat {
    static final byte[] MAGIC = new byte[] {'P', 'M', 'S', '_', 'S', 'S', 'T', '1'};
    static final int VERSION = 2;
    static final int FOOTER_SIZE = 8 + 4 + BlockHandle.MAX_ENCODED_LENGTH * 3 + 4;
    static final int DEFAULT_BLOCK_SIZE = 32 * 1024;
    static final int DEFAULT_RESTART_INTERVAL = 16;
    static final double DEFAULT_BLOOM_FPP = 0.01d;
    static final int VALUE_LEN_DELETE = -1;

    private SSTFormat() {
    }
}
