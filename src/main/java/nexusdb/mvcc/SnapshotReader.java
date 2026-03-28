package nexusdb.mvcc;

import java.util.concurrent.locks.StampedLock;

/**
 * Reads version chains using StampedLock optimistic reads.
 *
 * Happy path (>99% of calls in read-heavy workloads):
 *   1. Take a stamp — single volatile read, no CAS, no lock acquisition.
 *   2. Traverse the chain via findVisible().
 *   3. Validate the stamp — another volatile read.
 *   4. Return.
 *
 * Fallback path (<1% of calls):
 *   1. Acquire shared read lock.
 *   2. Traverse the chain under lock.
 *   3. Release.
 */
public final class SnapshotReader {

    /**
     * Read the value visible at the given snapshot timestamp from a version chain.
     *
     * @param chain      the version chain for the target key
     * @param snapshotTs the reader's snapshot timestamp
     * @param lock       the StampedLock associated with this key/chain
     * @return the value bytes, or null if no visible version exists or the visible version is a deletion
     */
    public byte[] read(VersionChain chain, long snapshotTs, StampedLock lock) {
        // --- OPTIMISTIC PATH ---
        long stamp = lock.tryOptimisticRead();
        if (stamp != 0L) {
            Version visible = chain.findVisible(snapshotTs);
            if (lock.validate(stamp)) {
                return extractValue(visible);
            }
        }

        // --- FALLBACK PATH ---
        stamp = lock.readLock();
        try {
            Version visible = chain.findVisible(snapshotTs);
            return extractValue(visible);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private byte[] extractValue(Version version) {
        if (version == null || version.isDeletion()) {
            return null;
        }
        return version.value();
    }
}
