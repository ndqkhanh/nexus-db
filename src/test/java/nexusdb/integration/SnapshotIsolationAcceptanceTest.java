package nexusdb.integration;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for Snapshot Isolation guarantees.
 * Verifies that concurrent readers see consistent snapshots.
 */
class SnapshotIsolationAcceptanceTest {

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
    @DisplayName("Snapshot isolation: reader does not see writes committed after its begin()")
    void readerDoesNotSeeFutureWrites() throws Exception {
        // Setup: write initial value
        Transaction setup = db.begin();
        setup.put("balance", "100".getBytes());
        setup.commit();

        // Reader begins BEFORE writer commits
        Transaction reader = db.begin();

        // Writer updates the value and commits
        Transaction writer = db.begin();
        writer.put("balance", "200".getBytes());
        writer.commit();

        // Reader should still see the old value (snapshot at begin time)
        byte[] result = reader.get("balance");
        reader.commit();

        assertThat(result).isEqualTo("100".getBytes());
    }

    @Test
    @DisplayName("Snapshot isolation: concurrent readers see consistent snapshot across multiple keys")
    void consistentSnapshotAcrossKeys() throws Exception {
        // Setup: write initial state
        Transaction setup = db.begin();
        setup.put("account-A", "500".getBytes());
        setup.put("account-B", "500".getBytes());
        setup.commit();

        // Reader begins and sees initial state
        Transaction reader = db.begin();

        // Writer transfers money: A -= 100, B += 100
        Transaction writer = db.begin();
        writer.put("account-A", "400".getBytes());
        writer.put("account-B", "600".getBytes());
        writer.commit();

        // Reader must see consistent snapshot: both old values
        byte[] balanceA = reader.get("account-A");
        byte[] balanceB = reader.get("account-B");
        reader.commit();

        assertThat(balanceA).isEqualTo("500".getBytes());
        assertThat(balanceB).isEqualTo("500".getBytes());
    }

    @Test
    @DisplayName("Snapshot isolation: write-write conflict results in first-committer-wins")
    void writeWriteConflictFirstCommitterWins() {
        // Setup
        Transaction setup = db.begin();
        setup.put("key", "original".getBytes());
        setup.commit();

        // Two concurrent writers both read and modify the same key
        Transaction writer1 = db.begin();
        Transaction writer2 = db.begin();

        writer1.put("key", "from-writer1".getBytes());
        writer2.put("key", "from-writer2".getBytes());

        // First committer wins
        writer1.commit();

        // Second committer should be aborted due to write-write conflict
        assertThatThrownBy(writer2::commit)
                .isInstanceOf(nexusdb.transaction.TransactionAbortException.class);

        // Verify writer1's value persisted
        Transaction verifier = db.begin();
        assertThat(verifier.get("key")).isEqualTo("from-writer1".getBytes());
        verifier.commit();
    }

    @Test
    @DisplayName("Snapshot isolation: concurrent readers do not block each other")
    void concurrentReadersDoNotBlock() throws Exception {
        // Setup
        Transaction setup = db.begin();
        setup.put("shared", "data".getBytes());
        setup.commit();

        int readerCount = 10;
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch allStarted = new CountDownLatch(readerCount);
        CountDownLatch allDone = new CountDownLatch(readerCount);
        ConcurrentLinkedQueue<byte[]> results = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < readerCount; i++) {
            executor.submit(() -> {
                allStarted.countDown();
                try {
                    allStarted.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                Transaction txn = db.begin();
                results.add(txn.get("shared"));
                txn.commit();
                allDone.countDown();
            });
        }

        boolean completed = allDone.await(10, TimeUnit.SECONDS);
        executor.close();

        assertThat(completed).isTrue();
        assertThat(results)
                .hasSize(readerCount)
                .allSatisfy(v -> assertThat(v).isEqualTo("data".getBytes()));
    }

    @Test
    @DisplayName("Snapshot isolation: new transaction sees committed writes from completed transactions")
    void newTransactionSeesCommittedWrites() {
        // Chain of transactions, each building on the previous
        Transaction txn1 = db.begin();
        txn1.put("counter", "1".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        assertThat(txn2.get("counter")).isEqualTo("1".getBytes());
        txn2.put("counter", "2".getBytes());
        txn2.commit();

        Transaction txn3 = db.begin();
        assertThat(txn3.get("counter")).isEqualTo("2".getBytes());
        txn3.put("counter", "3".getBytes());
        txn3.commit();

        Transaction verify = db.begin();
        assertThat(verify.get("counter")).isEqualTo("3".getBytes());
        verify.commit();
    }
}
