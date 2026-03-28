package nexusdb.transaction;

import nexusdb.concurrency.AdaptiveLockManager;
import nexusdb.mvcc.*;
import nexusdb.storage.BTree;
import nexusdb.storage.RecoveryManager;
import nexusdb.storage.WALRecord;
import nexusdb.storage.WALWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinates transaction lifecycle: begin, commit, abort.
 *
 * Uses MVCC version chains for storage (Phase 1: ConcurrentHashMap-backed),
 * AdaptiveLockManager for write concurrency, SnapshotReader for reads,
 * and EpochGarbageCollector for version reclamation.
 *
 * Phase 2 adds: SSI validation (rw-antidependency detection) and WAL durability.
 */
public final class TransactionManager {

    private final AtomicLong nextTxnId = new AtomicLong(1);
    private final AtomicLong globalTimestamp = new AtomicLong(0);

    private static final int BTREE_ORDER = 128;

    /** Key → VersionChain mapping. Phase 3: concurrent B-Tree with hand-over-hand crabbing. */
    private final BTree store = new BTree(BTREE_ORDER);

    private final AdaptiveLockManager lockManager;
    private final SnapshotReader snapshotReader;
    private final EpochGarbageCollector epochGC;
    private final Path dataDir;
    private final WALWriter walWriter; // null when in-memory only

    /** In-memory only (no WAL). */
    public TransactionManager() {
        this(null);
    }

    /** With optional WAL directory for durability. */
    public TransactionManager(Path dataDir) {
        this.dataDir = dataDir;
        this.lockManager = new AdaptiveLockManager();
        this.snapshotReader = new SnapshotReader();
        this.epochGC = new EpochGarbageCollector();

        if (dataDir != null) {
            try {
                // Recovery: rebuild in-memory store from WAL
                RecoveryManager recovery = new RecoveryManager();
                Map<String, byte[]> recovered = recovery.recover(dataDir);

                // Rebuild version chains from recovered state
                for (var entry : recovered.entrySet()) {
                    VersionChain chain = new VersionChain();
                    Version v = new Version(0, entry.getValue(), null);
                    v.commit(0);
                    chain.installVersion(v);
                    store.put(entry.getKey(), chain);
                }

                // Resume IDs after recovery
                if (recovery.highestTxnId() > 0) {
                    nextTxnId.set(recovery.highestTxnId() + 1);
                    globalTimestamp.set(recovery.highestTxnId() + 1);
                }

                this.walWriter = new WALWriter(dataDir);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to initialize WAL", e);
            }
        } else {
            this.walWriter = null;
        }
    }

    /** Begin a new transaction at default SNAPSHOT isolation. */
    public Transaction begin() {
        return begin(IsolationLevel.SNAPSHOT);
    }

    /** Begin a new transaction at the specified isolation level. */
    public Transaction begin(IsolationLevel isolation) {
        long txnId = nextTxnId.getAndIncrement();
        long snapshotTs = globalTimestamp.get();
        TransactionImpl txn = new TransactionImpl(txnId, snapshotTs, isolation, this);
        registerSsi(txn);
        return txn;
    }

    /**
     * Commit a transaction: install all buffered writes, detect conflicts,
     * assign commit timestamp, and make writes visible.
     */
    void commit(TransactionImpl txn) {
        // Check if already marked for abort by a concurrent SSI validation
        if (txn.abortReason() != null) {
            abortWriteSet(txn);
            ssiRegistry.remove(txn.id());
            throw new TransactionAbortException(txn.id(), txn.abortReason());
        }

        long commitTs = globalTimestamp.incrementAndGet();

        // SSI validation (only for SERIALIZABLE transactions)
        if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
            ssiValidate(txn);
        }

        // Write-write conflict detection (first-committer-wins, applies to all levels)
        for (var entry : txn.writeSet().entrySet()) {
            String key = entry.getKey();
            VersionChain chain = store.get(key);
            if (chain != null) {
                Version v = chain.getHead();
                while (v != null) {
                    if (v.status() == Version.Status.COMMITTED
                            && v.commitTimestamp() > txn.snapshotTimestamp()
                            && v.txnId() != txn.id()) {
                        abortWriteSet(txn);
                        throw new TransactionAbortException(txn.id(), "write-write conflict on key: " + key);
                    }
                    v = v.previous();
                }
            }
        }

        // WAL: write UPDATE records for each key, then COMMIT, then flush
        if (walWriter != null) {
            try {
                long prevLsn = 0;
                for (var entry : txn.writeSet().entrySet()) {
                    String key = entry.getKey();
                    byte[] value = entry.getValue();
                    // beforeImage: read current value from version chain
                    VersionChain existingChain = store.get(key);
                    byte[] beforeImage = existingChain != null
                            ? snapshotReader.read(existingChain, txn.snapshotTimestamp(),
                                    lockManager.getLockForKey(key))
                            : null;
                    prevLsn = walWriter.append(txn.id(), WALRecord.RecordType.UPDATE,
                            key, beforeImage, value, prevLsn);
                }
                walWriter.append(txn.id(), WALRecord.RecordType.COMMIT, "", null, null, prevLsn);
                walWriter.flush(); // fsync — durability guarantee
            } catch (IOException e) {
                throw new UncheckedIOException("WAL write failed during commit", e);
            }
        }

        // No conflicts — install all writes and commit them
        for (var entry : txn.writeSet().entrySet()) {
            String key = entry.getKey();
            byte[] value = entry.getValue();

            VersionChain chain = store.computeIfAbsent(key);
            Version installed = lockManager.installVersion(chain, key, txn.id(), value);
            installed.commit(commitTs);
            txn.addInstalledVersion(installed);
        }

        // SSI: notify read markers that these keys have been written
        if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
            ssiNotifyWrites(txn, commitTs);
        }

        // Lazy cleanup: remove committed SSI txns that are no longer needed
        // (keep recently committed ones so later-committing txns can detect edges)
        ssiCleanup(txn.snapshotTimestamp());
    }

    /** Abort a transaction: mark all installed versions as aborted. */
    void abort(TransactionImpl txn) {
        abortWriteSet(txn);
        ssiRegistry.remove(txn.id());
    }

    private void abortWriteSet(TransactionImpl txn) {
        for (Version v : txn.installedVersions()) {
            v.abort();
        }
    }

    /**
     * Read a key's value at the given transaction's snapshot.
     * First checks the transaction's local write buffer, then the version chain.
     * For SERIALIZABLE, records the read in the SSI read set.
     */
    byte[] read(TransactionImpl txn, String key) {
        // Read-your-own-writes: check local write buffer first
        if (txn.writeSet().containsKey(key)) {
            return txn.writeSet().get(key);
        }

        // Read from version chain via SnapshotReader
        VersionChain chain = store.get(key);
        if (chain == null) {
            // SSI: track that we read this key (even though it was absent)
            if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
                txn.recordRead(key);
            }
            return null;
        }

        epochGC.enterEpoch();
        try {
            byte[] result = snapshotReader.read(chain, txn.snapshotTimestamp(), lockManager.getLockForKey(key));
            // SSI: track reads for antidependency detection
            if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
                txn.recordRead(key);
            }
            return result;
        } finally {
            epochGC.exitEpoch();
        }
    }

    /**
     * Range scan: returns keys in [fromInclusive, toExclusive) that are visible
     * at the transaction's snapshot timestamp. Filters out keys with no visible version.
     * For SERIALIZABLE, records each visible key as a read for SSI tracking.
     */
    java.util.List<String> scan(TransactionImpl txn, String fromInclusive, String toExclusive) {
        java.util.List<String> allKeys = store.rangeScanKeys(fromInclusive, toExclusive);
        java.util.List<String> visibleKeys = new java.util.ArrayList<>();

        epochGC.enterEpoch();
        try {
            for (String key : allKeys) {
                // Check local write buffer first
                if (txn.writeSet().containsKey(key)) {
                    visibleKeys.add(key);
                    continue;
                }
                // Check version chain visibility at snapshot
                VersionChain chain = store.get(key);
                if (chain != null) {
                    byte[] val = snapshotReader.read(chain, txn.snapshotTimestamp(),
                            lockManager.getLockForKey(key));
                    if (val != null) {
                        visibleKeys.add(key);
                    }
                }
            }
        } finally {
            epochGC.exitEpoch();
        }

        if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
            for (String key : visibleKeys) {
                txn.recordRead(key);
            }
        }
        return visibleKeys;
    }

    // ========== SSI Support ==========

    /**
     * Registry of active SERIALIZABLE transactions for antidependency scanning.
     * Maps txnId → TransactionImpl.
     */
    private final ConcurrentHashMap<Long, TransactionImpl> ssiRegistry = new ConcurrentHashMap<>();

    void registerSsi(TransactionImpl txn) {
        if (txn.isolationLevel() == IsolationLevel.SERIALIZABLE) {
            ssiRegistry.put(txn.id(), txn);
        }
    }

    /**
     * SSI commit-time validation: detect dangerous structure (pivot with both
     * inbound and outbound rw-antidependency edges).
     */
    private void ssiValidate(TransactionImpl txn) {
        // Direction 1: For each key we're writing, find concurrent txns that read it
        // Edge: concurrent --rw--> txn (concurrent's read is staled by our write)
        for (String writtenKey : txn.writeSet().keySet()) {
            for (TransactionImpl concurrent : ssiRegistry.values()) {
                if (concurrent.id() == txn.id()) continue;
                if (concurrent.readSet().contains(writtenKey)) {
                    concurrent.addOutboundRwAntidep(txn.id());
                    txn.addInboundRwAntidep(concurrent.id());

                    if (concurrent.isDangerousPivot()) {
                        concurrent.markForAbort("SSI dangerous structure detected by txn " + txn.id());
                    }
                }
            }
        }

        // Direction 2: For each key we read, check if another transaction committed
        // a write to it after our snapshot — this is an rw-antidependency even if
        // the writing transaction has already left the ssiRegistry.
        // Edge: txn --rw--> writer (our read is staled by their committed write)
        for (String readKey : txn.readSet()) {
            VersionChain chain = store.get(readKey);
            if (chain == null) continue;
            Version v = chain.getHead();
            while (v != null) {
                if (v.status() == Version.Status.COMMITTED
                        && v.commitTimestamp() > txn.snapshotTimestamp()
                        && v.txnId() != txn.id()) {
                    txn.addOutboundRwAntidep(v.txnId());
                }
                v = v.previous();
            }

            // Also check concurrent (not yet committed) txns writing this key
            for (TransactionImpl concurrent : ssiRegistry.values()) {
                if (concurrent.id() == txn.id()) continue;
                if (concurrent.writeSet().containsKey(readKey)) {
                    txn.addOutboundRwAntidep(concurrent.id());
                    concurrent.addInboundRwAntidep(txn.id());

                    if (concurrent.isDangerousPivot()) {
                        concurrent.markForAbort("SSI dangerous structure detected by txn " + txn.id());
                    }
                }
            }
        }

        // Check if committing transaction itself is a dangerous pivot
        if (txn.isDangerousPivot()) {
            abortWriteSet(txn);
            throw new TransactionAbortException(txn.id(),
                    "SSI abort: dangerous structure (pivot with inbound + outbound rw-antideps)");
        }
    }

    /**
     * After committing writes, notify other SERIALIZABLE transactions that
     * their reads may now be stale.
     */
    private void ssiNotifyWrites(TransactionImpl txn, long commitTs) {
        for (String writtenKey : txn.writeSet().keySet()) {
            for (TransactionImpl concurrent : ssiRegistry.values()) {
                if (concurrent.id() == txn.id()) continue;
                if (concurrent.readSet().contains(writtenKey)) {
                    // concurrent --rw--> txn
                    concurrent.addOutboundRwAntidep(txn.id());
                    txn.addInboundRwAntidep(concurrent.id());
                }
            }
        }
    }

    /**
     * Lazily remove committed SSI transactions that are no longer needed.
     * A committed txn can be removed when no active SERIALIZABLE transaction
     * has a snapshot older than or equal to the committed txn's snapshot.
     */
    private void ssiCleanup(long currentSnapshotTs) {
        ssiRegistry.values().removeIf(t ->
                t.isCommitted() && t.snapshotTimestamp() < currentSnapshotTs);
    }

    public void close() {
        if (walWriter != null) {
            try {
                walWriter.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close WAL", e);
            }
        }
    }

    AdaptiveLockManager getLockManager() { return lockManager; }
    EpochGarbageCollector getEpochGC() { return epochGC; }
}
