package org.qwh.pms.codec;

import java.util.Arrays;

public record ByteArraySlice(byte[] bytes, int offset, int length) {

    public byte[] copy() {
        return Arrays.copyOfRange(bytes, offset, offset + length);
    }
}
