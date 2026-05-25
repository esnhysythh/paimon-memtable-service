package org.qwh.pms.sink.paimon;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonCommitPayloadCodecTest {

    @Test
    void roundTripsEmptyMessageList() {
        PaimonCommitPayloadCodec codec = new PaimonCommitPayloadCodec();

        assertEquals(List.of(), codec.decode(codec.encode(List.of())));
    }

    @Test
    void rejectsUnknownPayload() {
        PaimonCommitPayloadCodec codec = new PaimonCommitPayloadCodec();

        assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[] {1, 2, 3, 4}));
    }
}
