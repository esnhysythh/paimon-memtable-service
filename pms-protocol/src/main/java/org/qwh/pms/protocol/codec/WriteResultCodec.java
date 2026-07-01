package org.qwh.pms.protocol.codec;

import org.qwh.pms.protocol.api.PmsStatus;
import org.qwh.pms.protocol.api.WriteResult;

import java.util.Objects;

public final class WriteResultCodec {

    private WriteResultCodec() {}

    public static byte[] encodeResponse(PmsStatus status, int acceptedCount) {
        return encodeResponse(new WriteResult(status, acceptedCount));
    }

    public static byte[] encodeResponse(WriteResult result) {
        Objects.requireNonNull(result, "result must not be null");
        BinaryPayloadWriter writer = new BinaryPayloadWriter(Integer.BYTES + VarInt.encodedSize(result.acceptedCount()));
        writer.writeInt(result.status().code());
        VarInt.writeUnsignedInt(writer, result.acceptedCount());
        return writer.toByteArray();
    }

    public static WriteResult decodeResponse(byte[] body) {
        BinaryPayloadReader reader = new BinaryPayloadReader(body);
        PmsStatus status = PmsStatus.fromCode(reader.readInt());
        int acceptedCount = VarInt.readUnsignedInt(reader);
        reader.requireFullyRead();
        return new WriteResult(status, acceptedCount);
    }
}
