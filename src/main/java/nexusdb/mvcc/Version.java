package nexusdb.mvcc;

/**
 * A single version of a key's value in the MVCC version chain.
 *
 * Immutable after installation: all fields are written once at creation and
 * read many times thereafter. The {@code previous} pointer forms the chain.
 * {@code commitTimestamp} and {@code status} are volatile for cross-thread visibility.
 */
public final class Version {

    public enum Status { ACTIVE, COMMITTED, ABORTED }

    private final long txnId;
    private volatile long commitTimestamp;
    private final byte[] value;
    private final Version previous;
    private volatile Status status;

    public Version(long txnId, byte[] value, Version previous) {
        this.txnId = txnId;
        this.commitTimestamp = Long.MAX_VALUE; // sentinel: not yet committed
        this.value = value;
        this.previous = previous;
        this.status = Status.ACTIVE;
    }

    public long txnId() { return txnId; }
    public long commitTimestamp() { return commitTimestamp; }
    public byte[] value() { return value; }
    public Version previous() { return previous; }
    public Status status() { return status; }

    /** Returns true if this version represents a deletion (tombstone). */
    public boolean isDeletion() {
        return value == null;
    }

    /**
     * Returns true if this version is visible to a reader with the given snapshot timestamp.
     * Visibility rule: status == COMMITTED && commitTimestamp <= snapshotTimestamp
     */
    public boolean isVisibleTo(long snapshotTimestamp) {
        return status == Status.COMMITTED && commitTimestamp <= snapshotTimestamp;
    }

    /** Transition to COMMITTED state with the given timestamp. */
    public void commit(long timestamp) {
        this.commitTimestamp = timestamp;
        this.status = Status.COMMITTED;
    }

    /** Transition to ABORTED state. */
    public void abort() {
        this.status = Status.ABORTED;
    }
}
