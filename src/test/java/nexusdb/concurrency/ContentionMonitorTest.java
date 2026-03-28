package nexusdb.concurrency;

import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class ContentionMonitorTest {

    private ContentionMonitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new ContentionMonitor();
    }

    @Test
    @DisplayName("Initial mode for any key is CAS (optimistic)")
    void initialModeIsCAS() {
        assertThat(monitor.modeFor("any-key")).isEqualTo(ConcurrencyMode.CAS);
        assertThat(monitor.modeFor("another-key")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("Mode stays CAS when all operations succeed")
    void staysCasOnSuccess() {
        for (int i = 0; i < 500; i++) {
            monitor.recordOutcome("hot-key", true);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("Escalates to LOCK when failure ratio exceeds 30%")
    void escalatesToLockOnHighContention() {
        // Record enough failures to exceed 30% threshold
        // Need at least MIN_SAMPLES (250) before switching is allowed
        for (int i = 0; i < 300; i++) {
            // 40% failure rate: 60% success, 40% failure
            boolean success = (i % 5) < 3; // 3 success, 2 failure per 5 = 40% failure
            monitor.recordOutcome("hot-key", success);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.LOCK);
    }

    @Test
    @DisplayName("Does NOT escalate before minimum samples reached")
    void noEscalationBeforeMinSamples() {
        // Record high failure rate but below minimum sample count
        for (int i = 0; i < 100; i++) {
            monitor.recordOutcome("hot-key", false); // 100% failure
        }
        // Still CAS — not enough samples
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("De-escalates back to CAS when failure ratio drops below 15%")
    void deEscalatesOnLowContention() {
        // First escalate
        for (int i = 0; i < 300; i++) {
            boolean success = (i % 5) < 3; // 40% failure → escalate
            monitor.recordOutcome("hot-key", success);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.LOCK);

        // Now flood with successes to bring ratio below 15%
        for (int i = 0; i < 1000; i++) {
            monitor.recordOutcome("hot-key", true);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("Hysteresis: mode stays LOCK when ratio is between 15% and 30%")
    void hysteresisPreventsBouncing() {
        // Escalate to LOCK
        for (int i = 0; i < 300; i++) {
            boolean success = (i % 5) < 3; // 40% failure → escalate
            monitor.recordOutcome("hot-key", success);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.LOCK);

        // Feed ~20% failure rate (between 15% and 30%) — should STAY in LOCK
        for (int i = 0; i < 500; i++) {
            boolean success = (i % 5) < 4; // 1 failure per 5 = 20%
            monitor.recordOutcome("hot-key", success);
        }
        assertThat(monitor.modeFor("hot-key")).isEqualTo(ConcurrencyMode.LOCK);
    }

    @Test
    @DisplayName("Different keys in different ranges can have different modes")
    void independentKeyRanges() {
        // Escalate key A
        for (int i = 0; i < 300; i++) {
            monitor.recordOutcome("key-A", (i % 5) < 3);
        }

        // Key B stays all success
        for (int i = 0; i < 300; i++) {
            monitor.recordOutcome("key-B", true);
        }

        // Key A might be LOCK, key B should be CAS
        // (Keys may hash to same range, so we only assert key-B is CAS)
        assertThat(monitor.modeFor("key-B")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("Concurrent recordOutcome calls are thread-safe")
    void concurrentRecordOutcome() throws Exception {
        int threadCount = 16;
        int opsPerThread = 500;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try { startGate.await(); } catch (InterruptedException e) { }
                for (int i = 0; i < opsPerThread; i++) {
                    monitor.recordOutcome("shared-key", i % 2 == 0);
                }
                done.countDown();
            });
        }

        startGate.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.close();

        assertThat(completed).isTrue();
        // Just verify no exceptions and mode is a valid value
        ConcurrencyMode mode = monitor.modeFor("shared-key");
        assertThat(mode).isIn(ConcurrencyMode.CAS, ConcurrencyMode.LOCK);
    }

    @Test
    @DisplayName("getContentionRatio returns ratio for a key's range")
    void contentionRatioReported() {
        for (int i = 0; i < 300; i++) {
            monitor.recordOutcome("key", true);
        }
        double ratio = monitor.getContentionRatio("key");
        assertThat(ratio).isBetween(0.0, 0.01); // all successes → ~0%
    }
}
