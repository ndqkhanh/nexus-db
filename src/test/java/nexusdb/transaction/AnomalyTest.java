package nexusdb.transaction;

import nexusdb.NexusDB;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Five controlled interleaving tests verifying that NexusDB's Snapshot Isolation
 * prevents standard transaction anomalies.
 *
 * Each test uses CyclicBarrier / CountDownLatch to create a specific interleaving
 * between two concurrent transactions, then asserts the correct isolation behavior.
 *
 * @see "DDIA Ch7 — Weak Isolation Levels"
 */
class AnomalyTest {

    private NexusDB db;

    @BeforeEach
    void setUp() {
        db = NexusDB.open();
    }

    @AfterEach
    void tearDown() {
        if (db != null) db.close();
    }

    /**
     * Anomaly 1: Dirty Read
     *
     * T1 writes x=1 but does NOT commit.
     * T2 reads x.
     * Expected: T2 must NOT see T1's uncommitted write (x should be null).
     */
    @Test
    @DisplayName("Anomaly 1: No dirty reads — uncommitted writes are invisible")
    void noDirtyReads() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch t2Done = new CountDownLatch(1);
        byte[][] t2Result = new byte[1][];

        // T1: write but don't commit yet
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            txn.put("x", "dirty-value".getBytes());
            barrierAwait(barrier); // sync: T1 has written, T2 can now read
            barrierAwait(barrier); // sync: wait for T2 to read before we decide to abort
            txn.abort();
        });

        // T2: read after T1 writes but before T1 commits/aborts
        Thread t2 = Thread.ofVirtual().start(() -> {
            barrierAwait(barrier); // sync: T1 has written
            Transaction txn = db.begin();
            t2Result[0] = txn.get("x");
            txn.commit();
            barrierAwait(barrier); // sync: signal T1 we're done reading
            t2Done.countDown();
        });

        t2Done.await(5, TimeUnit.SECONDS);
        t1.join(5000);

        // T2 must NOT see T1's uncommitted write
        assertThat(t2Result[0]).isNull();
    }

    /**
     * Anomaly 2: Non-Repeatable Read
     *
     * T1 reads x (sees "original").
     * T2 writes x="updated" and commits.
     * T1 reads x again.
     * Expected: T1 must still see "original" (snapshot isolation guarantees repeatable reads).
     */
    @Test
    @DisplayName("Anomaly 2: No non-repeatable reads — snapshot is frozen at begin()")
    void noNonRepeatableReads() throws Exception {
        // Setup
        Transaction setup = db.begin();
        setup.put("x", "original".getBytes());
        setup.commit();

        CyclicBarrier barrier = new CyclicBarrier(2);
        byte[][] t1FirstRead = new byte[1][];
        byte[][] t1SecondRead = new byte[1][];
        CountDownLatch t1Done = new CountDownLatch(1);

        // T1: read, wait for T2 to commit, read again
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            t1FirstRead[0] = txn.get("x");
            barrierAwait(barrier); // sync: T1 has read, T2 can now write
            barrierAwait(barrier); // sync: T2 has committed
            t1SecondRead[0] = txn.get("x");
            txn.commit();
            t1Done.countDown();
        });

        // T2: write and commit between T1's two reads
        Thread t2 = Thread.ofVirtual().start(() -> {
            barrierAwait(barrier); // sync: T1 has done first read
            Transaction txn = db.begin();
            txn.put("x", "updated".getBytes());
            txn.commit();
            barrierAwait(barrier); // sync: T2 committed, T1 can do second read
        });

        t1Done.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        assertThat(t1FirstRead[0]).isEqualTo("original".getBytes());
        assertThat(t1SecondRead[0]).isEqualTo("original".getBytes()); // must be same!
    }

    /**
     * Anomaly 3: Phantom Read
     *
     * T1 reads keys "a", "b" (both exist).
     * T2 inserts "c" and commits.
     * T1 reads "c".
     * Expected: T1 must NOT see the phantom "c" (it was inserted after T1's snapshot).
     */
    @Test
    @DisplayName("Anomaly 3: No phantom reads — new keys inserted after snapshot are invisible")
    void noPhantomReads() throws Exception {
        // Setup
        Transaction setup = db.begin();
        setup.put("a", "1".getBytes());
        setup.put("b", "2".getBytes());
        setup.commit();

        CyclicBarrier barrier = new CyclicBarrier(2);
        byte[][] t1PhantomRead = new byte[1][];
        CountDownLatch t1Done = new CountDownLatch(1);

        // T1: begin, read existing keys, wait, then try to read phantom
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            txn.get("a"); // existing
            txn.get("b"); // existing
            barrierAwait(barrier); // sync: T2 can now insert phantom
            barrierAwait(barrier); // sync: T2 committed
            t1PhantomRead[0] = txn.get("c"); // phantom?
            txn.commit();
            t1Done.countDown();
        });

        // T2: insert phantom key "c"
        Thread t2 = Thread.ofVirtual().start(() -> {
            barrierAwait(barrier); // sync: T1 has started
            Transaction txn = db.begin();
            txn.put("c", "phantom".getBytes());
            txn.commit();
            barrierAwait(barrier); // sync: T2 committed
        });

        t1Done.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        // T1 must NOT see the phantom
        assertThat(t1PhantomRead[0]).isNull();
    }

    /**
     * Anomaly 4: Write Skew (under SI, this CAN occur — SSI would prevent it)
     *
     * Setup: doctor-on-call. Both Alice and Bob are on-call (oncall_alice=true, oncall_bob=true).
     * Rule: at least one doctor must remain on-call.
     *
     * T1: reads both → sees both on-call → sets alice=false.
     * T2: reads both → sees both on-call → sets bob=false.
     * Both commit (no write-write conflict — different keys).
     * Result: both off-call → invariant violated.
     *
     * Under SI, this is a KNOWN limitation. This test documents the behavior.
     * SSI (Phase 2) will detect and abort one transaction.
     */
    @Test
    @DisplayName("Anomaly 4: Write skew — SI allows it (SSI will prevent in Phase 2)")
    void writeSkewAllowedUnderSI() throws Exception {
        // Setup
        Transaction setup = db.begin();
        setup.put("oncall_alice", "true".getBytes());
        setup.put("oncall_bob", "true".getBytes());
        setup.commit();

        CyclicBarrier barrier = new CyclicBarrier(2);
        CountDownLatch bothDone = new CountDownLatch(2);

        // T1: read both, then set alice=false
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier); // sync: both have read
            txn.put("oncall_alice", "false".getBytes());
            txn.commit(); // succeeds — no write-write conflict
            bothDone.countDown();
        });

        // T2: read both, then set bob=false
        Thread t2 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            txn.get("oncall_alice");
            txn.get("oncall_bob");
            barrierAwait(barrier); // sync: both have read
            txn.put("oncall_bob", "false".getBytes());
            txn.commit(); // succeeds — no write-write conflict (different keys)
            bothDone.countDown();
        });

        bothDone.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        // Verify both are off-call → write skew occurred (expected under SI)
        Transaction verify = db.begin();
        assertThat(verify.get("oncall_alice")).isEqualTo("false".getBytes());
        assertThat(verify.get("oncall_bob")).isEqualTo("false".getBytes());
        verify.commit();
    }

    /**
     * Anomaly 5: Lost Update
     *
     * T1 reads x=100.
     * T2 reads x=100.
     * T1 writes x=100+50=150, commits.
     * T2 writes x=100+30=130, tries to commit.
     * Expected: T2 is ABORTED (first-committer-wins detects write-write conflict).
     */
    @Test
    @DisplayName("Anomaly 5: No lost update — first-committer-wins prevents it")
    void noLostUpdate() throws Exception {
        // Setup
        Transaction setup = db.begin();
        setup.put("balance", "100".getBytes());
        setup.commit();

        CyclicBarrier readDone = new CyclicBarrier(2);
        CyclicBarrier t1Committed = new CyclicBarrier(2);
        CountDownLatch t2Done = new CountDownLatch(1);
        boolean[] t2Aborted = {false};

        // T1: read, compute, commit first
        Thread t1 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            byte[] val = txn.get("balance");
            int current = Integer.parseInt(new String(val));
            barrierAwait(readDone); // sync: both have read
            txn.put("balance", String.valueOf(current + 50).getBytes()); // 100+50=150
            txn.commit();
            barrierAwait(t1Committed); // sync: T1 committed
        });

        // T2: read, compute, try to commit second
        Thread t2 = Thread.ofVirtual().start(() -> {
            Transaction txn = db.begin();
            byte[] val = txn.get("balance");
            int current = Integer.parseInt(new String(val));
            barrierAwait(readDone); // sync: both have read
            barrierAwait(t1Committed); // sync: wait for T1 to commit first
            txn.put("balance", String.valueOf(current + 30).getBytes()); // 100+30=130
            try {
                txn.commit(); // should FAIL — write-write conflict
            } catch (TransactionAbortException e) {
                t2Aborted[0] = true;
            }
            t2Done.countDown();
        });

        t2Done.await(5, TimeUnit.SECONDS);
        t1.join(5000);
        t2.join(5000);

        // T2 must be aborted
        assertThat(t2Aborted[0]).isTrue();

        // Final balance must be 150 (T1's write), not 130 (T2's lost update)
        Transaction verify = db.begin();
        assertThat(verify.get("balance")).isEqualTo("150".getBytes());
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
