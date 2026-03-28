package nexusdb.transaction;

/**
 * Represents an active database transaction.
 *
 * Lifecycle: begin → {get, put, delete}* → commit | abort
 */
public interface Transaction {

    /** Returns the transaction's unique ID. */
    long id();

    /** Returns the snapshot timestamp assigned at begin(). */
    long snapshotTimestamp();

    /**
     * Reads the value associated with the given key as visible at this transaction's snapshot.
     *
     * @return the value bytes, or null if the key does not exist
     */
    byte[] get(String key);

    /**
     * Writes a value for the given key. The write is buffered until commit.
     */
    void put(String key, byte[] value);

    /**
     * Deletes the given key. The deletion is buffered until commit.
     */
    void delete(String key);

    /**
     * Range scan: returns all keys in [fromInclusive, toExclusive) in sorted order.
     * Values are read at this transaction's snapshot timestamp.
     *
     * @return list of keys in the range, sorted lexicographically
     */
    java.util.List<String> scan(String fromInclusive, String toExclusive);

    /**
     * Commits this transaction, making all writes visible to future transactions.
     *
     * @throws nexusdb.transaction.TransactionAbortException if validation fails (e.g., write conflict, SSI abort)
     */
    void commit();

    /**
     * Aborts this transaction, discarding all writes.
     */
    void abort();
}
