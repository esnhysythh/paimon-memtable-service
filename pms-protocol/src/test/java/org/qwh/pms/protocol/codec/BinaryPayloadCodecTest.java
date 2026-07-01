package org.qwh.pms.protocol.codec;

import org.junit.jupiter.api.Test;
import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawKvEntry;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;
import org.qwh.pms.protocol.api.WriteResult;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryPayloadCodecTest {

    @Test
    void varIntGoldenBytesAreStableAndCanonical() {
        BinaryPayloadWriter writer = new BinaryPayloadWriter();
        VarInt.writeUnsignedInt(writer, 0);
        VarInt.writeUnsignedInt(writer, 127);
        VarInt.writeUnsignedInt(writer, 128);
        VarInt.writeUnsignedInt(writer, 16_384);

        assertEquals("007f8001808001", hex(writer.toByteArray()));

        BinaryPayloadReader reader = new BinaryPayloadReader(writer.toByteArray());
        assertEquals(0, VarInt.readUnsignedInt(reader));
        assertEquals(127, VarInt.readUnsignedInt(reader));
        assertEquals(128, VarInt.readUnsignedInt(reader));
        assertEquals(16_384, VarInt.readUnsignedInt(reader));
        reader.requireFullyRead();
    }

    @Test
    void recordBatchRequestSupportsPutAndDeleteTombstone() {
        List<RawKvEntry> entries = List.of(
                RawKvEntry.put(new byte[] {0x01}, new byte[] {0x02, 0x03}),
                RawKvEntry.delete(new byte[] {0x04, 0x05}));

        byte[] encoded = RecordBatchCodec.encodeRequest(entries);

        assertEquals("020000000500010102030000000401020405", hex(encoded));

        List<RawKvEntry> decoded = RecordBatchCodec.decodeRequest(encoded);
        assertEquals(2, decoded.size());
        assertArrayEquals(new byte[] {0x01}, decoded.get(0).key());
        assertArrayEquals(new byte[] {0x02, 0x03}, decoded.get(0).row());
        assertFalse(decoded.get(0).isDelete());
        assertArrayEquals(new byte[] {0x04, 0x05}, decoded.get(1).key());
        assertTrue(decoded.get(1).isDelete());
    }

    @Test
    void singlePutRecordBatchGoldenBytesAreStable() {
        byte[] encoded = RecordBatchCodec.encodeSinglePut(
                new byte[] {0x01, 0x02},
                new byte[] {0x0A, 0x0B, 0x0C});

        assertEquals("0100000007000201020a0b0c", hex(encoded));
    }

    @Test
    void keyBatchRequestGoldenBytesAreStableAndAllowsEmptyPrefix() {
        byte[] encoded = KeyBatchCodec.encodeSingle(new byte[] {0x0A, (byte) 0xFF});

        assertEquals("01020aff", hex(encoded));
        List<byte[]> decoded = KeyBatchCodec.decodeRequest(encoded);
        assertEquals(1, decoded.size());
        assertArrayEquals(new byte[] {0x0A, (byte) 0xFF}, decoded.get(0));

        List<byte[]> emptyPrefix = KeyBatchCodec.decodeRequest(KeyBatchCodec.encodeSingle(new byte[0]));
        assertEquals(1, emptyPrefix.size());
        assertArrayEquals(new byte[0], emptyPrefix.get(0));
    }

    @Test
    void writeResultGoldenBytesAreStable() {
        assertEquals("0000000002", hex(WriteResultCodec.encodeResponse(PmsStatus.OK, 2)));
        assertEquals("0000000200", hex(WriteResultCodec.encodeResponse(PmsStatus.OVERLOADED, 0)));

        WriteResult decoded = WriteResultCodec.decodeResponse(bytes("0000000002"));
        assertEquals(PmsStatus.OK, decoded.status());
        assertEquals(2, decoded.acceptedCount());
    }

    @Test
    void lookupBatchHitResponseGoldenBytesAreStable() {
        byte[] encoded = LookupBatchCodec.encodeResponse(
                RawLookupBatchResult.single(RawLookupResult.hit(new byte[] {0x7F})));

        assertEquals("000000000100017f", hex(encoded));

        RawLookupBatchResult decoded = LookupBatchCodec.decodeResponse(encoded);
        assertEquals(PmsStatus.OK, decoded.status());
        assertEquals(1, decoded.results().size());
        assertEquals(LookupResultType.HIT, decoded.results().get(0).type());
        assertArrayEquals(new byte[] {0x7F}, decoded.results().get(0).row());
    }

    @Test
    void lookupBatchMissAndDeletedResponsesCarryOnlyType() {
        byte[] encoded = LookupBatchCodec.encodeResponse(
                RawLookupBatchResult.ok(List.of(RawLookupResult.miss(), RawLookupResult.deleted())));

        assertEquals("00000000020102", hex(encoded));

        RawLookupBatchResult decoded = LookupBatchCodec.decodeResponse(encoded);
        assertEquals(PmsStatus.OK, decoded.status());
        assertEquals(2, decoded.results().size());
        assertEquals(LookupResultType.MISS, decoded.results().get(0).type());
        assertEquals(LookupResultType.DELETED, decoded.results().get(1).type());
    }

    @Test
    void lookupUnavailableStatusIsStableAndCarriesNoPerKeyResults() {
        byte[] encoded = LookupBatchCodec.encodeResponse(
                RawLookupBatchResult.failed(PmsStatus.LOOKUP_UNAVAILABLE));

        assertEquals("0000000600", hex(encoded));

        RawLookupBatchResult decoded = LookupBatchCodec.decodeResponse(encoded);
        assertEquals(PmsStatus.LOOKUP_UNAVAILABLE, decoded.status());
        assertTrue(decoded.results().isEmpty());
    }

    @Test
    void rejectsTrailingBytes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatchCodec.decodeRequest(bytes("0100000005000101020300")));
    }

    @Test
    void rejectsNonCanonicalVarInt() {
        assertThrows(IllegalArgumentException.class, () -> KeyBatchCodec.decodeRequest(bytes("8100")));
    }

    @Test
    void rejectsEmptyWriteBatchAndEmptyWriteKey() {
        assertThrows(IllegalArgumentException.class, () -> RecordBatchCodec.encodeRequest(List.of()));
        assertThrows(IllegalArgumentException.class, () -> RecordBatchCodec.decodeRequest(bytes("00")));
        assertThrows(
                IllegalArgumentException.class,
                () -> RawKvEntry.put(new byte[0], new byte[] {0x01}));
    }

    @Test
    void rejectsDeleteRecordWithRowBytes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatchCodec.decodeRequest(bytes("0100000004010101ff")));
    }

    @Test
    void limitedRecordBatchDecodeRejectsCountsAndFieldSizes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatchCodec.decodeRequest(bytes("8108"), 1024, 16, 16));

        byte[] tooLargeKey = RecordBatchCodec.encodeSinglePut(new byte[] {0x01, 0x02}, new byte[] {0x03});
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatchCodec.decodeRequest(tooLargeKey, 1, 1, 16));

        byte[] tooLargeRow = RecordBatchCodec.encodeSinglePut(new byte[] {0x01}, new byte[] {0x02, 0x03});
        assertThrows(
                IllegalArgumentException.class,
                () -> RecordBatchCodec.decodeRequest(tooLargeRow, 1, 16, 1));
    }

    @Test
    void limitedKeyBatchDecodeRejectsCountsAndKeySizes() {
        assertThrows(
                IllegalArgumentException.class,
                () -> KeyBatchCodec.decodeRequest(bytes("8108"), 1024, 16));

        byte[] tooLargeKey = KeyBatchCodec.encodeSingle(new byte[] {0x01, 0x02});
        assertThrows(
                IllegalArgumentException.class,
                () -> KeyBatchCodec.decodeRequest(tooLargeKey, 1, 1));
    }

    @Test
    void limitedLookupBatchDecodeRejectsCountsAndRowSizes() {
        byte[] twoResults = LookupBatchCodec.encodeResponse(
                RawLookupBatchResult.ok(List.of(RawLookupResult.miss(), RawLookupResult.deleted())));
        assertThrows(
                IllegalArgumentException.class,
                () -> LookupBatchCodec.decodeResponse(twoResults, 1, 16));

        byte[] tooLargeRow = LookupBatchCodec.encodeResponse(
                RawLookupBatchResult.single(RawLookupResult.hit(new byte[] {0x01, 0x02})));
        assertThrows(
                IllegalArgumentException.class,
                () -> LookupBatchCodec.decodeResponse(tooLargeRow, 1, 1));
    }

    @Test
    void rejectsUnknownStatusCode() {
        assertThrows(IllegalArgumentException.class, () -> WriteResultCodec.decodeResponse(bytes("0000006300")));
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(String.format("%02x", b & 0xFF));
        }
        return builder.toString();
    }

    private static byte[] bytes(String hex) {
        if ((hex.length() & 1) != 0) {
            throw new IllegalArgumentException("hex length must be even");
        }
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int index = i * 2;
            result[i] = (byte) Integer.parseInt(hex.substring(index, index + 2), 16);
        }
        return result;
    }
}
