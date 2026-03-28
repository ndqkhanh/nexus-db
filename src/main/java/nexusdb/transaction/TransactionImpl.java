package nexusdb.transaction;

import nexusdb.mvcc.Version;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concrete transaction implementation. Buffers writes locally until commit,
 * then delegates to TransactionManager for conflict detection and installation.
 *
 * For SERIALIZABLE isolation, tracks read sets and rw-antidependency edges
 * to support SSI (Serializable Snapshot Isolation) validation.
 */
final class TransactionImpl implements Transaction {

    private final long id;
    private final long snapshotTimestamp;
    private final IsolationLevel isolationLevel;
    private final TransactionManager manager;

    /** Local write buffer: key → value (null value = deletion tombstone). */
    private final LinkedHashMap<String, byte[]> writeSet = new LinkedHashMap<>();

    /** Versions installed in the version chains during commit. Used for abort cleanup. */
    private final List<Version> installedVersions = new ArrayList<>();

    // ========== SSI tracking fields ==========

    /** Keys read by this transaction (SERIALIZABLE only). */
    private final Set<String> readSet = ConcurrentHashMap.newKeySet();

    /** Transaction IDs that have inbound rw-antidependency edges TO this transaction. */
    private final Set<Long> inboundRwAntideps = ConcurrentHashMap.newKeySet();

    /** Transaction IDs that have outbound rw-antidependency edges FROM this transaction. */
    private final Set<Long> outboundRwAntideps = ConcurrentHashMap.newKeySet();

    /** If non-null, this transaction has been marked for abort with the given reason. */
    private volatile String abortReason;

    private boolean committed = false;
    private boolean aborted = false;

    TransactionImpl(long id, long snapshotTimestamp, IsolationLevel isolationLevel, TransactionManager manager) {
        this.id = id;
        this.snapshotTimestamp = snapshotTimestamp;
        this.isolationLevel = isolationLevel;
        this.manager = manager;
    }

    @Override
    public long id() { return id; }

    @Override
    public long snapshotTimestamp() { return snapshotTimestamp; }

    @Override
    public byte[] get(String key) {
        checkActive();
        return manager.read(this, key);
    }

    @Override
    public void put(String key, byte[] value) {
        checkActive();
        writeSet.put(key, value);
    }

    @Override
    public void delete(String key) {
        checkActive();
        writeSet.put(key, null); // null = deletion tombstone
    }

    @Override
    public java.util.List<String> scan(String fromInclusive, String toExclusive) {
        checkActive();
        return manager.scan(this, fromInclusive, toExclusive);
    }

    @Override
    public void commit() {
        checkActive();
        manager.commit(this);
        committed = true;
    }

    @Override
    public void abort() {
        if (committed || aborted) return; // idempotent
        manager.abort(this);
        aborted = true;
    }

    LinkedHashMap<String, byte[]> writeSet() { return writeSet; }
    List<Version> installedVersions() { return installedVersions; }
    void addInstalledVersion(Version v) { installedVersions.add(v); }
    IsolationLevel isolationLevel() { return isolationLevel; }
    boolean isCommitted() { return committed; }

    // ========== SSI methods ==========

    /** Record that this transaction read the given key. */
    void recordRead(String key) { readSet.add(key); }

    /** Keys read by this transaction. */
    Set<String> readSet() { return readSet; }

    /** Record an inbound rw-antidependency edge (some other txn's read was staled by us). */
    void addInboundRwAntidep(long txnId) { inboundRwAntideps.add(txnId); }

    /** Record an outbound rw-antidependency edge (our read was staled by another txn). */
    void addOutboundRwAntidep(long txnId) { outboundRwAntideps.add(txnId); }

    /**
     * A transaction is a "dangerous pivot" if it has BOTH inbound AND outbound
     * rw-antidependency edges — this forms a potential non-serializable cycle.
     */
    boolean isDangerousPivot() {
        return !inboundRwAntideps.isEmpty() && !outboundRwAntideps.isEmpty();
    }

    /** Mark this transaction for abort (will be checked at commit time). */
    void markForAbort(String reason) { this.abortReason = reason; }

    /** Returns the abort reason if marked, null otherwise. */
    String abortReason() { return abortReason; }

    private void checkActive() {
        if (committed) throw new IllegalStateException("Transaction " + id + " already committed");
        if (aborted) throw new IllegalStateException("Transaction " + id + " already aborted");
    }
}
