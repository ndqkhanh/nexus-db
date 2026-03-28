package nexusdb.concurrency;

/**
 * The two concurrency modes that the Adaptive Lock Elision system can switch between.
 */
public enum ConcurrencyMode {
    /** Optimistic CAS-based path — lock-free, best for low contention. */
    CAS,
    /** Pessimistic 2PL path — ReentrantLock-based, best for high contention. */
    LOCK
}
