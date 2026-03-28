package nexusdb.mvcc;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Epoch-based garbage collector for MVCC version chains.
 *
 * Inspired by Linux kernel RCU (Read-Copy-Update). Readers register entry/exit
 * from epochs; the GC advances the global epoch only when all readers have
 * exited the previous one. Versions retired at epoch E are safe to reclaim
 * when the global epoch reaches E+2.
 *
 * Three-epoch invariant:
 *   - E   (current):    active readers running, new retirements tagged E
 *   - E-1 (grace):      some readers may still be here
 *   - E-2 (reclaimable): all readers guaranteed exited, safe to free
 */
public final class EpochGarbageCollector {

    private final AtomicLong globalEpoch = new AtomicLong(0L);

    /**
     * Per-thread epoch participation. Value == Long.MAX_VALUE means thread is not
     * in a critical section. Value == E means thread entered at epoch E.
     */
    private final ConcurrentHashMap<Thread, AtomicLong> threadEpochs = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<RetiredVersion> retireList = new ConcurrentLinkedQueue<>();

    /** Returns the current global epoch. */
    public long currentEpoch() {
        return globalEpoch.get();
    }

    /**
     * Called by a reader thread before accessing any version chain.
     * Records the current global epoch as this thread's participation epoch.
     *
     * @return the epoch stamp (for diagnostics; caller must call exitEpoch() when done)
     */
    public long enterEpoch() {
        long epoch = globalEpoch.get();
        threadEpochs
                .computeIfAbsent(Thread.currentThread(), t -> new AtomicLong(Long.MAX_VALUE))
                .set(epoch);
        return epoch;
    }

    /**
     * Called by a reader thread after completing its version chain traversal.
     * Clears this thread's epoch participation.
     */
    public void exitEpoch() {
        AtomicLong registration = threadEpochs.get(Thread.currentThread());
        if (registration != null) {
            registration.set(Long.MAX_VALUE);
        }
    }

    /**
     * Retire a version for eventual reclamation. Called when a version becomes
     * logically unreachable (superseded by newer committed versions).
     */
    public void retire(Version version) {
        retireList.add(new RetiredVersion(version, globalEpoch.get()));
    }

    /** Returns the number of versions waiting for reclamation. */
    public int retiredCount() {
        return retireList.size();
    }

    /**
     * Attempt to advance the global epoch and reclaim old versions.
     * Called by the background GC thread on each sweep.
     *
     * 1. Compute the minimum epoch among all active readers.
     * 2. If all readers have exited the current epoch, advance.
     * 3. Reclaim versions retired at epoch <= (currentEpoch - 2).
     */
    public void gcSweep() {
        long currentEpoch = globalEpoch.get();

        long minActiveEpoch = threadEpochs.values().stream()
                .mapToLong(AtomicLong::get)
                .filter(e -> e != Long.MAX_VALUE)
                .min()
                .orElse(currentEpoch); // no active readers: safe to advance

        // Advance only if all readers have exited the current epoch
        if (minActiveEpoch >= currentEpoch) {
            globalEpoch.compareAndSet(currentEpoch, currentEpoch + 1);
        }

        // Reclaim versions retired at epoch E-2 or earlier
        long reclaimBefore = globalEpoch.get() - 2;
        retireList.removeIf(rv -> rv.retiredAtEpoch() <= reclaimBefore);
    }

    /**
     * A version awaiting reclamation, tagged with the epoch at which it was retired.
     */
    record RetiredVersion(Version version, long retiredAtEpoch) {}
}
