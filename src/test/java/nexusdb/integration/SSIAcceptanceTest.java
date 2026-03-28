package nexusdb.integration;

import nexusdb.NexusDB;
import nexusdb.transaction.IsolationLevel;
import nexusdb.transaction.Transaction;
import nexusdb.transaction.TransactionAbortException;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for Serializable Snapshot Isolation (SSI).
 * These verify that write skew — the one anomaly SI allows — is detected and aborted under SSI.
 */
class SSIAcceptanceTest {

    private NexusDB db;

    @BeforeEach
    void setUp() {
        db = NexusDB.open();
    }

    @AfterEach
    void tearDown() {
        if (db != null) db.close();
    }

    @Test
    @DisplayName("SSI prevents write skew: doctor-on-call scenario")
    void ssiPreventsWriteSkew() throws Exception {
        // Setup: both doctors on-call
        Transaction setup = db.begin();
        setup.put("oncall_alice", "true".getBytes());
        setup.put("oncall_bob", "true".getBytes());
        setup.commit();

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch bothDone = new CountDownLatch(2);
        boolean[] t1Aborted = {false};
        boolean[] t2Aborted = {false};

        // T1: read both, set alice=false (SERIALIZABLE)
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin(IsolationLevel.SERIALIZABLE);
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier);
            txn.put("oncall_alice", "false".getBytes());
            try {
                txn.commit();
            } catch (TransactionAbortException e) {
                t1Aborted[0] = true;
            }
            bothDone.countDown();
        });

        // T2: read both, set bob=false (SERIALIZABLE)
        Thread t2 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin(IsolationLevel.SERIALIZABLE);
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier);
            txn.put("oncall_bob", "false".getBytes());
            try {
                txn.commit();
            } catch (TransactionAbortException e) {
                t2Aborted[0] = true;
            }
            bothDone.countDown();
        });

        bothDone.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        // At least one must be aborted to prevent write skew
        assertThat(t1Aborted[0] || t2Aborted[0])
                .as("SSI must abort at least one transaction to prevent write skew")
                .isTrue();

        // Verify invariant: at least one doctor is still on-call
        Transaction verify = db.begin();
        String alice = verify.get("oncall_alice") != null ? new String(verify.get("oncall_alice")) : "true";
        String bob = verify.get("oncall_bob") != null ? new String(verify.get("oncall_bob")) : "true";
        verify.commit();

        boolean atLeastOneOnCall = "true".equals(alice) || "true".equals(bob);
        assertThat(atLeastOneOnCall)
                .as("At least one doctor must remain on-call, alice=%s, bob=%s", alice, bob)
                .isTrue();
    }

    @Test
    @DisplayName("SSI allows non-conflicting concurrent reads and writes")
    void ssiAllowsNonConflictingOperations() {
        Transaction setup = db.begin();
        setup.put("key-A", "1".getBytes());
        setup.put("key-B", "2".getBytes());
        setup.commit();

        // T1 reads A, writes C
        Transaction t1 = db.begin(IsolationLevel.SERIALIZABLE);
        t1.get("key-A");
        t1.put("key-C", "3".getBytes());

        // T2 reads B, writes D (no overlap with T1's read/write sets)
        Transaction t2 = db.begin(IsolationLevel.SERIALIZABLE);
        t2.get("key-B");
        t2.put("key-D", "4".getBytes());

        // Both should commit successfully — no rw-antidependency cycle
        t1.commit();
        t2.commit();

        Transaction verify = db.begin();
        assertThat(verify.get("key-C")).isEqualTo("3".getBytes());
        assertThat(verify.get("key-D")).isEqualTo("4".getBytes());
        verify.commit();
    }

    @Test
    @DisplayName("SSI detects rw-antidependency: T1 reads key that T2 writes")
    void ssiDetectsRwAntidependency() {
        Transaction setup = db.begin();
        setup.put("counter", "10".getBytes());
        setup.put("flag", "off".getBytes());
        setup.commit();

        // T1: reads counter, writes flag based on counter value
        Transaction t1 = db.begin(IsolationLevel.SERIALIZABLE);
        t1.get("counter"); // read
        t1.put("flag", "on".getBytes()); // write different key

        // T2: reads flag, writes counter
        Transaction t2 = db.begin(IsolationLevel.SERIALIZABLE);
        t2.get("flag"); // read
        t2.put("counter", "20".getBytes()); // write different key

        // This creates a cycle: T1 reads counter → T2 writes counter (T1 --rw--> T2)
        //                        T2 reads flag → T1 writes flag (T2 --rw--> T1)
        // One must be aborted
        boolean anyAborted = false;
        try {
            t1.commit();
        } catch (TransactionAbortException e) {
            anyAborted = true;
        }
        try {
            t2.commit();
        } catch (TransactionAbortException e) {
            anyAborted = true;
        }

        assertThat(anyAborted)
                .as("SSI must detect the rw-antidependency cycle and abort at least one")
                .isTrue();
    }

    @Test
    @DisplayName("SNAPSHOT isolation still allows write skew (backward compatibility)")
    void snapshotStillAllowsWriteSkew() throws Exception {
        Transaction setup = db.begin();
        setup.put("oncall_alice", "true".getBytes());
        setup.put("oncall_bob", "true".getBytes());
        setup.commit();

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch bothDone = new CountDownLatch(2);

        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin(IsolationLevel.SNAPSHOT);
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier);
            txn.put("oncall_alice", "false".getBytes());
            txn.commit(); // should succeed under SI
            bothDone.countDown();
        });

        Thread t2 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin(IsolationLevel.SNAPSHOT);
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier);
            txn.put("oncall_bob", "false".getBytes());
            txn.commit(); // should succeed under SI
            bothDone.countDown();
        });

        bothDone.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        // Both committed — write skew occurred (expected under SI)
        Transaction verify = db.begin();
        assertThat(verify.get("oncall_alice")).isEqualTo("false".getBytes());
        assertThat(verify.get("oncall_bob")).isEqualTo("false".getBytes());
        verify.commit();
    }

    private static void barrierAwait(CyclicBarrier barrier) {
        try {
            barrier.await(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
