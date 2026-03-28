package nexusdb.concurrency;

import nexusdb.mvcc.Version;
import nexusdb.mvcc.VersionChain;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

/**
 * Adaptive Lock Elision — the core of NexusDB's crown jewel.
 *
 * Callers see a single installVersion() interface. Internally, the manager
 * delegates to either:
 *   - CAS optimistic path (lock-free, retry on failure)
 *   - 2PL pessimistic path (ReentrantLock, guaranteed single-attempt success)
 *
 * The ContentionMonitor decides which path to use based on observed CAS
 * success/failure ratios per key range.
 *
 * @see "JCIP Ch11 — Performance and Scalability"
 * @see "DDIA Ch7 — Transactions"
 */
public final class AdaptiveLockManager {

    private static final int NUM_RANGES = 256;
    private static final int MAX_CAS_RETRIES = 8;

    private final ContentionMonitor contentionMonitor;
    private final StampedLock[] stampedLocks;
    private final ReentrantLock[] pessimisticLocks;

    public AdaptiveLockManager() {
        this.contentionMonitor = new ContentionMonitor();
        this.stampedLocks = new StampedLock[NUM_RANGES];
        this.pessimisticLocks = new ReentrantLock[NUM_RANGES];
        for (int i = 0; i < NUM_RANGES; i++) {
            stampedLocks[i] = new StampedLock();
            pessimisticLocks[i] = new ReentrantLock();
        }
    }

    /**
     * Install a new version on the chain, using either CAS or 2PL path
     * based on current contention mode.
     *
     * @param chain the version chain for the key
     * @param key   the key (used for contention monitoring and lock selection)
     * @param txnId the writing transaction's ID
     * @param value the value bytes (null for deletion)
     * @return the newly installed Version
     */
    public Version installVersion(VersionChain chain, String key, long txnId, byte[] value) {
        ConcurrencyMode mode = contentionMonitor.modeFor(key);

        if (mode == ConcurrencyMode.CAS) {
            return installOptimistic(chain, key, txnId, value);
        } else {
            return installPessimistic(chain, key, txnId, value);
        }
    }

    /**
     * CAS optimistic path: retry loop with bounded retries.
     * On each CAS failure, records the outcome to the ContentionMonitor.
     * If retries exhausted, falls back to pessimistic path.
     */
    private Version installOptimistic(VersionChain chain, String key, long txnId, byte[] value) {
        for (int attempt = 0; attempt < MAX_CAS_RETRIES; attempt++) {
            Version currentHead = chain.getHead();
            Version newVersion = new Version(txnId, value, currentHead);
            if (chain.installVersion(newVersion)) {
                contentionMonitor.recordOutcome(key, true);
                return newVersion;
            }
            contentionMonitor.recordOutcome(key, false);
        }
        // CAS retries exhausted — fall back to pessimistic
        return installPessimistic(chain, key, txnId, value);
    }

    /**
     * 2PL pessimistic path: acquire ReentrantLock, install version, release.
     * Guaranteed single-attempt success since the lock serializes writers.
     */
    private Version installPessimistic(VersionChain chain, String key, long txnId, byte[] value) {
        int rangeIdx = rangeIndex(key);
        ReentrantLock lock = pessimisticLocks[rangeIdx];
        lock.lock();
        try {
            Version currentHead = chain.getHead();
            Version newVersion = new Version(txnId, value, currentHead);
            // Under lock, CAS is guaranteed to succeed (no concurrent writers)
            boolean installed = chain.installVersion(newVersion);
            assert installed : "CAS under lock must succeed";
            return newVersion;
        } finally {
            lock.unlock();
        }
    }

    /** Expose contention recording for testing and external contention injection. */
    public void recordCasOutcome(String key, boolean success) {
        contentionMonitor.recordOutcome(key, success);
    }

    /** Returns the current concurrency mode for the given key's range. */
    public ConcurrencyMode currentMode(String key) {
        return contentionMonitor.modeFor(key);
    }

    /** Returns the StampedLock for the given key's range (used by SnapshotReader). */
    public StampedLock getLockForKey(String key) {
        return stampedLocks[rangeIndex(key)];
    }

    /** Returns the contention ratio for the given key's range. */
    public double getContentionRatio(String key) {
        return contentionMonitor.getContentionRatio(key);
    }

    ContentionMonitor getContentionMonitor() {
        return contentionMonitor;
    }

    private int rangeIndex(String key) {
        return (key.hashCode() & 0x7FFF_FFFF) % NUM_RANGES;
    }
}
