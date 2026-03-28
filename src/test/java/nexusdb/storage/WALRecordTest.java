package nexusdb.storage;

import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for WALRecord binary serialization/deserialization.
 * Format: [lsn:8][txnId:8][type:1][keyLen:4][key:keyLen][prevLsn:8]
 *         [beforeLen:4][beforeImage:beforeLen][afterLen:4][afterImage:afterLen]
 */
class WALRecordTest {

    @Test
    @DisplayName("UPDATE record round-trips through serialize/deserialize")
    void updateRecordRoundTrip() {
        WALRecord record = new WALRecord(
                1L, 100L, WALRecord.RecordType.UPDATE, "user:alice",
                "old-value".getBytes(), "new-value".getBytes(), 0L);

        byte[] bytes = record.serialize();
        WALRecord deserialized = WALRecord.deserialize(ByteBuffer.wrap(bytes));

        assertThat(deserialized.lsn()).isEqualTo(1L);
        assertThat(deserialized.txnId()).isEqualTo(100L);
        assertThat(deserialized.type()).isEqualTo(WALRecord.RecordType.UPDATE);
        assertThat(deserialized.key()).isEqualTo("user:alice");
        assertThat(deserialized.beforeImage()).isEqualTo("old-value".getBytes());
        assertThat(deserialized.afterImage()).isEqualTo("new-value".getBytes());
        assertThat(deserialized.prevLsn()).isEqualTo(0L);
    }

    @Test
    @DisplayName("COMMIT record round-trips (no before/after images)")
    void commitRecordRoundTrip() {
        WALRecord record = WALRecord.commit(2L, 100L, 1L);

        byte[] bytes = record.serialize();
        WALRecord deserialized = WALRecord.deserialize(ByteBuffer.wrap(bytes));

        assertThat(deserialized.lsn()).isEqualTo(2L);
        assertThat(deserialized.txnId()).isEqualTo(100L);
        assertThat(deserialized.type()).isEqualTo(WALRecord.RecordType.COMMIT);
        assertThat(deserialized.key()).isEmpty();
        assertThat(deserialized.beforeImage()).isNull();
        assertThat(deserialized.afterImage()).isNull();
        assertThat(deserialized.prevLsn()).isEqualTo(1L);
    }

    @Test
    @DisplayName("ABORT record round-trips")
    void abortRecordRoundTrip() {
        WALRecord record = WALRecord.abort(3L, 100L, 2L);

        byte[] bytes = record.serialize();
        WALRecord deserialized = WALRecord.deserialize(ByteBuffer.wrap(bytes));

        assertThat(deserialized.type()).isEqualTo(WALRecord.RecordType.ABORT);
        assertThat(deserialized.txnId()).isEqualTo(100L);
        assertThat(deserialized.prevLsn()).isEqualTo(2L);
    }

    @Test
    @DisplayName("CLR record created from original UPDATE for undo")
    void clrRecordFromUpdate() {
        WALRecord original = new WALRecord(
                10L, 200L, WALRecord.RecordType.UPDATE, "counter",
                "before".getBytes(), "after".getBytes(), 5L);

        WALRecord clr = WALRecord.clrFor(original, 20L, 5L);

        assertThat(clr.lsn()).isEqualTo(20L);
        assertThat(clr.txnId()).isEqualTo(200L);
        assertThat(clr.type()).isEqualTo(WALRecord.RecordType.CLR);
        assertThat(clr.key()).isEqualTo("counter");
        // CLR's afterImage = original's beforeImage (restoring state)
        assertThat(clr.afterImage()).isEqualTo("before".getBytes());
        // CLR's beforeImage = original's afterImage (state we're undoing from)
        assertThat(clr.beforeImage()).isEqualTo("after".getBytes());
        // prevLsn = undoNextLsn (points to next record to undo)
        assertThat(clr.prevLsn()).isEqualTo(5L);
    }

    @Test
    @DisplayName("Null before/after images serialize as length -1")
    void nullImagesSerialize() {
        WALRecord record = WALRecord.commit(1L, 100L, 0L);

        byte[] bytes = record.serialize();
        WALRecord deserialized = WALRecord.deserialize(ByteBuffer.wrap(bytes));

        assertThat(deserialized.beforeImage()).isNull();
        assertThat(deserialized.afterImage()).isNull();
    }

    @Test
    @DisplayName("Empty key serializes correctly")
    void emptyKeySerializes() {
        WALRecord record = WALRecord.commit(1L, 100L, 0L);
        byte[] bytes = record.serialize();

        assertThat(bytes.length).isGreaterThan(0);
        WALRecord deserialized = WALRecord.deserialize(ByteBuffer.wrap(bytes));
        assertThat(deserialized.key()).isEmpty();
    }

    @Test
    @DisplayName("isPhysical returns true for UPDATE and CLR, false for others")
    void isPhysical() {
        WALRecord update = new WALRecord(1L, 1L, WALRecord.RecordType.UPDATE, "k",
                "b".getBytes(), "a".getBytes(), 0L);
        WALRecord clr = WALRecord.clrFor(update, 2L, 0L);
        WALRecord commit = WALRecord.commit(3L, 1L, 2L);
        WALRecord abort = WALRecord.abort(4L, 1L, 3L);

        assertThat(update.isPhysical()).isTrue();
        assertThat(clr.isPhysical()).isTrue();
        assertThat(commit.isPhysical()).isFalse();
        assertThat(abort.isPhysical()).isFalse();
    }

    @Test
    @DisplayName("Multiple records deserialize sequentially from a buffer")
    void multipleRecordsFromBuffer() {
        WALRecord r1 = new WALRecord(1L, 10L, WALRecord.RecordType.UPDATE, "key1",
                null, "v1".getBytes(), 0L);
        WALRecord r2 = WALRecord.commit(2L, 10L, 1L);

        byte[] b1 = r1.serialize();
        byte[] b2 = r2.serialize();
        byte[] combined = new byte[b1.length + b2.length];
        System.arraycopy(b1, 0, combined, 0, b1.length);
        System.arraycopy(b2, 0, combined, b1.length, b2.length);

        ByteBuffer buf = ByteBuffer.wrap(combined);
        WALRecord d1 = WALRecord.deserialize(buf);
        WALRecord d2 = WALRecord.deserialize(buf);

        assertThat(d1.lsn()).isEqualTo(1L);
        assertThat(d1.type()).isEqualTo(WALRecord.RecordType.UPDATE);
        assertThat(d2.lsn()).isEqualTo(2L);
        assertThat(d2.type()).isEqualTo(WALRecord.RecordType.COMMIT);
    }

    @Test
    @DisplayName("Serialize size matches expected binary layout")
    void serializeSizeMatchesLayout() {
        WALRecord record = new WALRecord(
                1L, 100L, WALRecord.RecordType.UPDATE, "key",
                "before".getBytes(), "after".getBytes(), 0L);

        byte[] bytes = record.serialize();
        // 8(lsn) + 8(txnId) + 1(type) + 4(keyLen) + 3(key) + 8(prevLsn)
        // + 4(beforeLen) + 6(before) + 4(afterLen) + 5(after) = 51
        assertThat(bytes.length).isEqualTo(51);
    }
}
