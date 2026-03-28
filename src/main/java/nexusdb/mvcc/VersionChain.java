package nexusdb.mvcc;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-key version chain container.
 *
 * The head AtomicReference always points to the newest (most recently written)
 * version. New versions are prepended via CAS. Readers traverse
 * head → previous → ... → oldest.
 */
public final class VersionChain {

    private final AtomicReference<Version> head = new AtomicReference<>(null);

    /**
     * Install a new version as the chain head via CAS.
     *
     * The caller must pre-link newVersion.previous to the current head before calling.
     * On CAS failure (concurrent writer raced ahead), returns false — caller must
     * re-read head, re-link, and retry.
     *
     * @return true if installation succeeded
     */
    public boolean installVersion(Version newVersion) {
        Version expectedHead = newVersion.previous();
        return head.compareAndSet(expectedHead, newVersion);
    }

    /** Returns the newest version, or null if the chain is empty. */
    public Version getHead() {
        return head.get();
    }

    /**
     * Find the newest version visible at the given snapshot timestamp.
     * Traverses the chain from head (newest) to tail (oldest), returning the first
     * version where {@code isVisibleTo(snapshotTimestamp)} is true.
     *
     * @return the visible version, or null if no version is visible
     */
    public Version findVisible(long snapshotTimestamp) {
        Version current = head.get();
        while (current != null) {
            if (current.isVisibleTo(snapshotTimestamp)) {
                return current;
            }
            current = current.previous();
        }
        return null;
    }
}
