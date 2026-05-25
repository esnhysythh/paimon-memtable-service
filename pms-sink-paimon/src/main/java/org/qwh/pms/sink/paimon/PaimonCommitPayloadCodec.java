package org.qwh.pms.sink.paimon;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class PaimonCommitPayloadCodec {
    private static final int MAGIC = 0x5043504C; // PCPL
    private static final int VERSION = 1;

    private final CommitMessageSerializer serializer = new CommitMessageSerializer();

    public byte[] encode(List<CommitMessage> messages) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(serializer.getVersion());
            out.writeInt(messages.size());
            for (CommitMessage message : messages) {
                writeBytes(out, serializer.serialize(message));
            }
            out.flush();
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("encode Paimon commit payload failed", e);
        }
    }

    public List<CommitMessage> decode(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != MAGIC || version != VERSION) {
                throw new IllegalArgumentException("unsupported Paimon commit payload");
            }
            int serializerVersion = in.readInt();
            int count = readNonNegativeInt(in, "messageCount");
            List<CommitMessage> messages = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                messages.add(serializer.deserialize(serializerVersion, readBytes(in)));
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("payload has trailing bytes: " + in.available());
            }
            return messages;
        } catch (IOException e) {
            throw new IllegalArgumentException("decode Paimon commit payload failed", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        int length = readNonNegativeInt(in, "message length");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static int readNonNegativeInt(DataInputStream in, String name) throws IOException {
        int value = in.readInt();
        if (value < 0) {
            throw new IllegalArgumentException("invalid " + name + ": " + value);
        }
        return value;
    }
}
