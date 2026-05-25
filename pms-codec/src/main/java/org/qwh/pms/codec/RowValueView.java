package org.qwh.pms.codec;

import org.apache.paimon.types.RowKind;

import java.util.Arrays;

import static org.qwh.pms.codec.ByteUtils.FLAG_HAS_CHECKSUM;
import static org.qwh.pms.codec.ByteUtils.FLAG_LARGE_ROW;
import static org.qwh.pms.codec.ByteUtils.HEADER_SIZE;
import static org.qwh.pms.codec.ByteUtils.VERSION;
import static org.qwh.pms.codec.ByteUtils.i32le;
import static org.qwh.pms.codec.ByteUtils.u16le;
import static org.qwh.pms.codec.ByteUtils.u8;

/**
 * Parsed view of a PMS row value.
 *
 * <p>The view validates and materializes only row metadata: version, flags, row kind, writer schema
 * id, field-id arrays, end offsets and payload boundaries. It keeps the original row value bytes
 * and returns payload slices on demand, so creating a view does not deserialize any field value.
 *
 * <p>This is the object that makes top-level column projection cheap: callers binary-search a
 * field id through {@link #lookup(int)} and decode only the requested {@link #payloadSlice(int)}.
 */
public final class RowValueView {

    private final byte[] bytes;
    private final int flags;
    private final RowKind rowKind;
    private final int writerSchemaId;
    private final int[] notNullFieldIds;
    private final int[] nullFieldIds;
    private final int[] endOffsets;
    private final int payloadOffset;
    private final int payloadLength;

    private RowValueView(
            byte[] bytes,
            int flags,
            RowKind rowKind,
            int writerSchemaId,
            int[] notNullFieldIds,
            int[] nullFieldIds,
            int[] endOffsets,
            int payloadOffset,
            int payloadLength) {
        this.bytes = bytes;
        this.flags = flags;
        this.rowKind = rowKind;
        this.writerSchemaId = writerSchemaId;
        this.notNullFieldIds = notNullFieldIds;
        this.nullFieldIds = nullFieldIds;
        this.endOffsets = endOffsets;
        this.payloadOffset = payloadOffset;
        this.payloadLength = payloadLength;
    }

    public static RowValueView parse(byte[] bytes) {
        return parse(bytes, ValidationMode.TRUSTED);
    }

    public static RowValueView parse(byte[] bytes, ValidationMode validationMode) {
        if (bytes.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Row value is shorter than header: " + bytes.length);
        }
        int version = u8(bytes, 0);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported row codec version: " + version);
        }

        int flags = u8(bytes, 1);
        if ((flags & FLAG_HAS_CHECKSUM) != 0) {
            throw new IllegalArgumentException("Checksum flag is reserved but not implemented");
        }
        RowKind rowKind = RowKind.fromByteValue(bytes[2]);
        if (rowKind != RowKind.INSERT && rowKind != RowKind.DELETE) {
            throw new IllegalArgumentException("Persistent row kind must be INSERT or DELETE: " + rowKind);
        }

        int writerSchemaId = i32le(bytes, 4);
        int notNullCount = u16le(bytes, 8);
        int nullCount = u16le(bytes, 10);

        if (rowKind == RowKind.DELETE) {
            if (notNullCount != 0 || nullCount != 0 || bytes.length != HEADER_SIZE) {
                throw new IllegalArgumentException("DELETE tombstone must contain only an empty header");
            }
            return new RowValueView(
                    bytes, flags, rowKind, writerSchemaId, new int[0], new int[0], new int[0], HEADER_SIZE, 0);
        }

        boolean largeRow = (flags & FLAG_LARGE_ROW) != 0;
        int idWidth = largeRow ? 4 : 1;
        int offsetWidth = largeRow ? 4 : 2;
        int cursor = HEADER_SIZE;

        requireAvailable(bytes, cursor, (notNullCount + nullCount) * idWidth, "field ids");
        int[] notNullFieldIds = new int[notNullCount];
        int[] nullFieldIds = new int[nullCount];
        for (int i = 0; i < notNullCount; i++) {
            notNullFieldIds[i] = readNonNegativeInt(bytes, cursor, idWidth);
            cursor += idWidth;
        }
        for (int i = 0; i < nullCount; i++) {
            nullFieldIds[i] = readNonNegativeInt(bytes, cursor, idWidth);
            cursor += idWidth;
        }
        if (validationMode == ValidationMode.STRICT) {
            validateSorted("not-null field ids", notNullFieldIds);
            validateSorted("null field ids", nullFieldIds);
            validateDisjoint(notNullFieldIds, nullFieldIds);
        }

        requireAvailable(bytes, cursor, notNullCount * offsetWidth, "offsets");
        int[] endOffsets = new int[notNullCount];
        int previous = 0;
        for (int i = 0; i < notNullCount; i++) {
            int endOffset = readNonNegativeInt(bytes, cursor, offsetWidth);
            cursor += offsetWidth;
            if (endOffset < previous) {
                throw new IllegalArgumentException("Offsets must be non-decreasing");
            }
            endOffsets[i] = endOffset;
            previous = endOffset;
        }

        int payloadLength = notNullCount == 0 ? 0 : endOffsets[notNullCount - 1];
        requireAvailable(bytes, cursor, payloadLength, "payload");
        if (cursor + payloadLength != bytes.length) {
            throw new IllegalArgumentException("Unexpected trailing bytes after payload");
        }
        return new RowValueView(
                bytes, flags, rowKind, writerSchemaId, notNullFieldIds, nullFieldIds, endOffsets, cursor, payloadLength);
    }

    public int version() {
        return VERSION;
    }

    public int flags() {
        return flags;
    }

    public RowKind rowKind() {
        return rowKind;
    }

    public int writerSchemaId() {
        return writerSchemaId;
    }

    public boolean isTombstone() {
        return rowKind == RowKind.DELETE;
    }

    public FieldLookup lookup(int fieldId) {
        int notNullIndex = Arrays.binarySearch(notNullFieldIds, fieldId);
        if (notNullIndex >= 0) {
            return FieldLookup.notNull(notNullIndex);
        }
        if (Arrays.binarySearch(nullFieldIds, fieldId) >= 0) {
            return FieldLookup.nullValue();
        }
        return FieldLookup.missing();
    }

    public ByteArraySlice payloadSlice(int notNullIndex) {
        int start = notNullIndex == 0 ? 0 : endOffsets[notNullIndex - 1];
        int end = endOffsets[notNullIndex];
        return new ByteArraySlice(bytes, payloadOffset + start, end - start);
    }

    public int payloadLength() {
        return payloadLength;
    }

    private static int readNonNegativeInt(byte[] bytes, int offset, int width) {
        if (width == 1) {
            return u8(bytes, offset);
        }
        if (width == 2) {
            return u16le(bytes, offset);
        }
        if (width == 4) {
            int value = i32le(bytes, offset);
            if (value < 0) {
                throw new IllegalArgumentException("4-byte value exceeds non-negative int range");
            }
            return value;
        }
        throw new IllegalArgumentException("Unsupported unsigned width: " + width);
    }

    private static void requireAvailable(byte[] bytes, int offset, int length, String section) {
        if (length < 0 || offset + length > bytes.length) {
            throw new IllegalArgumentException("Invalid " + section + " length");
        }
    }

    private static void validateSorted(String name, int[] values) {
        int previous = -1;
        for (int value : values) {
            if (value < 0 || value <= previous) {
                throw new IllegalArgumentException(name + " must be strictly ascending and non-negative");
            }
            previous = value;
        }
    }

    private static void validateDisjoint(int[] left, int[] right) {
        int i = 0;
        int j = 0;
        while (i < left.length && j < right.length) {
            if (left[i] == right[j]) {
                throw new IllegalArgumentException("Field id appears in both not-null and null arrays: " + left[i]);
            }
            if (left[i] < right[j]) {
                i++;
            } else {
                j++;
            }
        }
    }
}
