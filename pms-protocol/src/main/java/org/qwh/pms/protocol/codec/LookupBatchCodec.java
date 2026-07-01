package org.qwh.pms.protocol.codec;

import org.qwh.pms.protocol.api.LookupResultType;
import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.RawLookupBatchResult;
import org.qwh.pms.protocol.api.RawLookupResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class LookupBatchCodec {

    private LookupBatchCodec() {}

    public static byte[] encodeResponse(RawLookupBatchResult result) {
        Objects.requireNonNull(result, "result must not be null");
        BinaryPayloadWriter writer = new BinaryPayloadWriter();
        writer.writeInt(result.status().code());
        if (result.status() != PmsStatus.OK) {
            VarInt.writeUnsignedInt(writer, 0);
            return writer.toByteArray();
        }
        VarInt.writeUnsignedInt(writer, result.results().size());
        for (RawLookupResult lookup : result.results()) {
            encodeLookupResult(writer, lookup);
        }
        return writer.toByteArray();
    }

    public static RawLookupBatchResult decodeResponse(byte[] body) {
        return decodeResponse(body, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public static RawLookupBatchResult decodeResponse(byte[] body, int maxResults, int maxRowBytes) {
        requirePositive(maxResults, "maxResults");
        requirePositive(maxRowBytes, "maxRowBytes");
        BinaryPayloadReader reader = new BinaryPayloadReader(body);
        PmsStatus status = PmsStatus.fromCode(reader.readInt());
        int resultCount = VarInt.readUnsignedInt(reader);
        RawLookupBatchResult result;
        if (status != PmsStatus.OK) {
            if (resultCount != 0) {
                throw new IllegalArgumentException("non-OK lookup batch must use resultCount 0");
            }
            result = RawLookupBatchResult.failed(status);
        } else {
            if (resultCount > maxResults) {
                throw new IllegalArgumentException("result count exceeds limit: " + resultCount + " > " + maxResults);
            }
            List<RawLookupResult> results = new ArrayList<>(resultCount);
            for (int i = 0; i < resultCount; i++) {
                results.add(decodeLookupResult(reader, maxRowBytes));
            }
            result = RawLookupBatchResult.ok(results);
        }
        reader.requireFullyRead();
        return result;
    }

    private static void encodeLookupResult(BinaryPayloadWriter writer, RawLookupResult result) {
        if (result.status() != PmsStatus.OK) {
            throw new IllegalArgumentException("lookup batch cannot encode failed per-key result");
        }
        VarInt.writeUnsignedInt(writer, result.type().code());
        if (result.type() == LookupResultType.HIT) {
            VarInt.writeUnsignedInt(writer, result.row().length);
            writer.writeBytes(result.row());
        }
    }

    private static RawLookupResult decodeLookupResult(BinaryPayloadReader reader, int maxRowBytes) {
        LookupResultType type = LookupResultType.fromCode(VarInt.readUnsignedInt(reader));
        return switch (type) {
            case HIT -> {
                int rowLength = VarInt.readUnsignedInt(reader);
                if (rowLength > maxRowBytes) {
                    throw new IllegalArgumentException("row too large: " + rowLength + " > " + maxRowBytes);
                }
                yield RawLookupResult.hit(reader.readBytes(rowLength));
            }
            case MISS -> RawLookupResult.miss();
            case DELETED -> RawLookupResult.deleted();
        };
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }
}
