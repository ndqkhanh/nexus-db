package nexusdb.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Immutable WAL record for ARIES-style write-ahead logging.
 *
 * <p>Binary format on disk:
 * [lsn:8][txnId:8][type:1][keyLen:4][key:keyLen][prevLsn:8]
 * [beforeLen:4][beforeImage:beforeLen][afterLen:4][afterImage:afterLen]
 *
 * <p>Null images are encoded as length -1 (no payload bytes follow).
 */
public record WALRecord(
        long lsn,
        long txnId,
        RecordType type,
        String key,
        byte[] beforeImage,
        byte[] afterImage,
        long prevLsn
) {
    public enum RecordType {
        UPDATE, COMMIT, ABORT, CLR, CHECKPOINT;

        private static final RecordType[] VALUES = values();

        public static RecordType fromOrdinal(int ordinal) {
            return VALUES[ordinal];
        }
    }

    /** True for records that carry page/key modifications. */
    public boolean isPhysical() {
        return type == RecordType.UPDATE || type == RecordType.CLR;
    }

    /** Create a COMMIT record. */
    public static WALRecord commit(long lsn, long txnId, long prevLsn) {
        return new WALRecord(lsn, txnId, RecordType.COMMIT, "", null, null, prevLsn);
    }

    /** Create an ABORT record. */
    public static WALRecord abort(long lsn, long txnId, long prevLsn) {
        return new WALRecord(lsn, txnId, RecordType.ABORT, "", null, null, prevLsn);
    }

    /**
     * Compensation Log Record for undoing {@code original}.
     * The CLR's afterImage is the original's beforeImage (restoring state).
     * The CLR's beforeImage is the original's afterImage (state we're undoing from).
     * prevLsn = undoNextLsn (points to next record to undo, skipping this CLR).
     */
    public static WALRecord clrFor(WALRecord original, long newLsn, long undoNextLsn) {
        return new WALRecord(
                newLsn,
                original.txnId(),
                RecordType.CLR,
                original.key(),
                original.afterImage(),   // before = state we're undoing from
                original.beforeImage(),  // after  = state we're restoring to
                undoNextLsn
        );
    }

    /** Serialize this record to a byte array. */
    public byte[] serialize() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int beforeLen = beforeImage == null ? -1 : beforeImage.length;
        int afterLen = afterImage == null ? -1 : afterImage.length;

        int size = 8 + 8 + 1 + 4 + keyBytes.length + 8
                + 4 + Math.max(0, beforeLen)
                + 4 + Math.max(0, afterLen);

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putLong(lsn);
        buf.putLong(txnId);
        buf.put((byte) type.ordinal());
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putLong(prevLsn);
        buf.putInt(beforeLen);
        if (beforeLen > 0) buf.put(beforeImage);
        buf.putInt(afterLen);
        if (afterLen > 0) buf.put(afterImage);

        return buf.array();
    }

    /** Deserialize a record from the buffer's current position. */
    public static WALRecord deserialize(ByteBuffer buf) {
        long lsn = buf.getLong();
        long txnId = buf.getLong();
        RecordType type = RecordType.fromOrdinal(buf.get() & 0xFF);
        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        long prevLsn = buf.getLong();

        int beforeLen = buf.getInt();
        byte[] beforeImage = null;
        if (beforeLen >= 0) {
            beforeImage = new byte[beforeLen];
            buf.get(beforeImage);
        }

        int afterLen = buf.getInt();
        byte[] afterImage = null;
        if (afterLen >= 0) {
            afterImage = new byte[afterLen];
            buf.get(afterImage);
        }

        return new WALRecord(lsn, txnId, type,
                new String(keyBytes, StandardCharsets.UTF_8),
                beforeImage, afterImage, prevLsn);
    }
}
