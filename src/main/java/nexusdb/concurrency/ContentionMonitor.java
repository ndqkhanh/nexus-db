package nexusdb.concurrency;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks CAS success/failure rates per key range and triggers mode transitions
 * when contention crosses hysteresis thresholds.
 *
 * The key space is partitioned into NUM_RANGES ranges. Each range maintains
 * a sliding window of recent operation outcomes and a current concurrency mode.
 *
 * Thread safety: all mutable state uses atomic operations. No synchronized blocks —
 * the monitor itself must not become a contention point.
 *
 * @see "JCIP Ch11.4 — Reducing Lock Contention"
 */
public final class ContentionMonitor {

    private static final int NUM_RANGES = 256;
    private static final int WINDOW_SIZE = 1_000;
    private static final int MIN_SAMPLES = WINDOW_SIZE / 4; // 250
    private static final double ESCALATION_THRESHOLD = 0.30;
    private static final double DE_ESCALATION_THRESHOLD = 0.15;

    private final RangeState[] ranges;

    public ContentionMonitor() {
        this.ranges = new RangeState[NUM_RANGES];
        for (int i = 0; i < NUM_RANGES; i++) {
            ranges[i] = new RangeState();
        }
    }

    /**
     * Returns the current concurrency mode for the given key.
     * Hot path: one hash, one array lookup, one volatile read.
     */
    public ConcurrencyMode modeFor(String key) {
        return ranges[rangeIndex(key)].mode;
    }

    /**
     * Records the outcome of a CAS attempt and evaluates whether
     * a mode transition is warranted.
     *
     * @param key     the key that was operated on
     * @param success true if the CAS succeeded, false if it failed
     */
    public void recordOutcome(String key, boolean success) {
        RangeState state = ranges[rangeIndex(key)];

        long total = state.totalCount.incrementAndGet();
        if (!success) {
            state.failureCount.incrementAndGet();
        }

        // Sliding window: when we exceed WINDOW_SIZE, decrement a proportional
        // share of failures. This approximates a true circular buffer without
        // the memory overhead of storing each individual outcome.
        if (total > WINDOW_SIZE && total % WINDOW_SIZE == 0) {
            // Reset: scale failures to the window proportion
            long failures = state.failureCount.get();
            double failureRatio = (double) failures / total;
            long estimatedFailuresInWindow = (long) (failureRatio * WINDOW_SIZE);
            state.failureCount.set(estimatedFailuresInWindow);
            state.totalCount.set(WINDOW_SIZE);
        }

        // Not enough samples yet
        if (total < MIN_SAMPLES) return;

        // Compute contention ratio
        long failures = state.failureCount.get();
        long windowTotal = Math.min(total, WINDOW_SIZE);
        if (windowTotal == 0) return;
        double ratio = (double) failures / windowTotal;

        // Evaluate thresholds with hysteresis
        if (state.mode == ConcurrencyMode.CAS && ratio > ESCALATION_THRESHOLD) {
            state.mode = ConcurrencyMode.LOCK;
        } else if (state.mode == ConcurrencyMode.LOCK && ratio < DE_ESCALATION_THRESHOLD) {
            state.mode = ConcurrencyMode.CAS;
        }
    }

    /**
     * Returns the current contention ratio for the range containing the given key.
     */
    public double getContentionRatio(String key) {
        RangeState state = ranges[rangeIndex(key)];
        long total = state.totalCount.get();
        if (total == 0) return 0.0;
        long failures = state.failureCount.get();
        return (double) failures / Math.min(total, WINDOW_SIZE);
    }

    private int rangeIndex(String key) {
        return (key.hashCode() & 0x7FFF_FFFF) % NUM_RANGES;
    }

    /**
     * Per-range contention state. Accessed concurrently by all threads
     * operating on keys in this range.
     */
    private static final class RangeState {
        volatile ConcurrencyMode mode = ConcurrencyMode.CAS;
        final AtomicLong totalCount = new AtomicLong(0);
        final AtomicLong failureCount = new AtomicLong(0);
    }
}
