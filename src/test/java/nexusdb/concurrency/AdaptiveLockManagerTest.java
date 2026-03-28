package nexusdb.concurrency;

import nexusdb.mvcc.*;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the AdaptiveLockManager — the CAS ↔ 2PL switching state machine.
 * Callers see a single acquireAndExecute() interface; the manager delegates
 * to OptimisticExecutor (CAS) or PessimisticExecutor (2PL) based on ContentionMonitor.
 */
class AdaptiveLockManagerTest {

    private AdaptiveLockManager lockManager;

    @BeforeEach
    void setUp() {
        lockManager = new AdaptiveLockManager();
    }

    @Test
    @DisplayName("Write installs version on version chain via CAS path (low contention)")
    void writeInstallsVersionViaCas() {
        VersionChain chain = new VersionChain();

        Version result = lockManager.installVersion(chain, "key1", 1L, "hello".getBytes());

        assertThat(result).isNotNull();
        assertThat(result.txnId()).isEqualTo(1L);
        assertThat(result.value()).isEqualTo("hello".getBytes());
        assertThat(chain.getHead()).isSameAs(result);
    }

    @Test
    @DisplayName("Multiple sequential writes build a version chain")
    void sequentialWritesBuildChain() {
        VersionChain chain = new VersionChain();

        Version v1 = lockManager.installVersion(chain, "key1", 1L, "v1".getBytes());
        v1.commit(10L);

        Version v2 = lockManager.installVersion(chain, "key1", 2L, "v2".getBytes());

        assertThat(chain.getHead()).isSameAs(v2);
        assertThat(v2.previous()).isSameAs(v1);
    }

    @Test
    @DisplayName("Under high contention, mode escalates to LOCK and writes still succeed")
    void escalatesToLockUnderContention() {
        VersionChain chain = new VersionChain();

        // Simulate high contention by recording many CAS failures
        for (int i = 0; i < 300; i++) {
            lockManager.recordCasOutcome("hot-key", (i % 5) < 3); // 40% failure
        }

        assertThat(lockManager.currentMode("hot-key")).isEqualTo(ConcurrencyMode.LOCK);

        // Writes still succeed via pessimistic path
        Version v = lockManager.installVersion(chain, "hot-key", 1L, "data".getBytes());
        assertThat(v).isNotNull();
        assertThat(chain.getHead()).isSameAs(v);
    }

    @Test
    @DisplayName("Concurrent writes to same key do not lose versions")
    void concurrentWritesNoLoss() throws Exception {
        VersionChain chain = new VersionChain();
        int writerCount = 20;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writerCount);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < writerCount; i++) {
            final long txnId = i;
            executor.submit(() -> {
                try { startGate.await(); } catch (InterruptedException e) { }
                lockManager.installVersion(chain, "shared-key", txnId, ("v" + txnId).getBytes());
                done.countDown();
            });
        }

        startGate.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.close();

        assertThat(completed).isTrue();

        // Count chain length — should have all versions
        int count = 0;
        Version current = chain.getHead();
        while (current != null) {
            count++;
            current = current.previous();
        }
        assertThat(count).isEqualTo(writerCount);
    }

    @Test
    @DisplayName("De-escalation: mode returns to CAS after contention subsides")
    void deEscalationBackToCas() {
        // Escalate
        for (int i = 0; i < 300; i++) {
            lockManager.recordCasOutcome("key", (i % 5) < 3);
        }
        assertThat(lockManager.currentMode("key")).isEqualTo(ConcurrencyMode.LOCK);

        // Flood with successes
        for (int i = 0; i < 1000; i++) {
            lockManager.recordCasOutcome("key", true);
        }
        assertThat(lockManager.currentMode("key")).isEqualTo(ConcurrencyMode.CAS);
    }

    @Test
    @DisplayName("getLockForKey returns consistent StampedLock for same key range")
    void consistentLockPerKeyRange() {
        var lock1 = lockManager.getLockForKey("key-a");
        var lock2 = lockManager.getLockForKey("key-a");

        assertThat(lock1).isSameAs(lock2);
    }
}
