package nexusdb.transaction;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for TransactionManager — begin/commit/abort lifecycle,
 * snapshot isolation, and write-write conflict detection.
 */
class TransactionManagerTest {

    private TransactionManager txnManager;

    @BeforeEach
    void setUp() {
        txnManager = new TransactionManager();
    }

    @Test
    @DisplayName("begin() returns a transaction with unique monotonic ID")
    void beginReturnsUniqueTransaction() {
        Transaction txn1 = txnManager.begin();
        Transaction txn2 = txnManager.begin();

        assertThat(txn1.id()).isGreaterThan(0);
        assertThat(txn2.id()).isGreaterThan(txn1.id());

        txn1.abort();
        txn2.abort();
    }

    @Test
    @DisplayName("begin() assigns monotonically increasing snapshot timestamps")
    void beginAssignsMonotonicSnapshots() {
        Transaction txn1 = txnManager.begin();
        Transaction txn2 = txnManager.begin();

        assertThat(txn2.snapshotTimestamp()).isGreaterThanOrEqualTo(txn1.snapshotTimestamp());

        txn1.abort();
        txn2.abort();
    }

    @Test
    @DisplayName("put then get within same transaction returns the written value")
    void readYourOwnWrites() {
        Transaction txn = txnManager.begin();
        txn.put("key", "value".getBytes());

        byte[] result = txn.get("key");

        assertThat(result).isEqualTo("value".getBytes());
        txn.commit();
    }

    @Test
    @DisplayName("Committed value is visible to new transaction")
    void committedValueVisible() {
        Transaction txn1 = txnManager.begin();
        txn1.put("key", "hello".getBytes());
        txn1.commit();

        Transaction txn2 = txnManager.begin();
        byte[] result = txn2.get("key");
        txn2.commit();

        assertThat(result).isEqualTo("hello".getBytes());
    }

    @Test
    @DisplayName("Aborted value is NOT visible to new transaction")
    void abortedValueNotVisible() {
        Transaction txn1 = txnManager.begin();
        txn1.put("key", "gone".getBytes());
        txn1.abort();

        Transaction txn2 = txnManager.begin();
        byte[] result = txn2.get("key");
        txn2.commit();

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Snapshot isolation: reader does not see concurrent write committed after its begin")
    void snapshotIsolation() {
        Transaction setup = txnManager.begin();
        setup.put("balance", "100".getBytes());
        setup.commit();

        Transaction reader = txnManager.begin();

        Transaction writer = txnManager.begin();
        writer.put("balance", "200".getBytes());
        writer.commit();

        byte[] result = reader.get("balance");
        reader.commit();

        assertThat(result).isEqualTo("100".getBytes());
    }

    @Test
    @DisplayName("Write-write conflict: first committer wins, second is aborted")
    void writeWriteConflict() {
        Transaction setup = txnManager.begin();
        setup.put("key", "original".getBytes());
        setup.commit();

        Transaction w1 = txnManager.begin();
        Transaction w2 = txnManager.begin();

        w1.put("key", "from-w1".getBytes());
        w2.put("key", "from-w2".getBytes());

        w1.commit(); // first committer wins

        assertThatThrownBy(w2::commit)
                .isInstanceOf(TransactionAbortException.class);

        // Verify w1's value persisted
        Transaction verifier = txnManager.begin();
        assertThat(verifier.get("key")).isEqualTo("from-w1".getBytes());
        verifier.commit();
    }

    @Test
    @DisplayName("Delete makes key invisible to subsequent transactions")
    void deleteKey() {
        Transaction txn1 = txnManager.begin();
        txn1.put("key", "data".getBytes());
        txn1.commit();

        Transaction txn2 = txnManager.begin();
        txn2.delete("key");
        txn2.commit();

        Transaction txn3 = txnManager.begin();
        assertThat(txn3.get("key")).isNull();
        txn3.commit();
    }

    @Test
    @DisplayName("Multiple puts to same key within transaction: last write wins")
    void multiplePutsSameKeyLastWins() {
        Transaction txn = txnManager.begin();
        txn.put("key", "first".getBytes());
        txn.put("key", "second".getBytes());
        txn.put("key", "third".getBytes());

        assertThat(txn.get("key")).isEqualTo("third".getBytes());
        txn.commit();

        Transaction reader = txnManager.begin();
        assertThat(reader.get("key")).isEqualTo("third".getBytes());
        reader.commit();
    }

    @Test
    @DisplayName("Non-conflicting writes on different keys both commit successfully")
    void nonConflictingWritesBothCommit() {
        Transaction w1 = txnManager.begin();
        Transaction w2 = txnManager.begin();

        w1.put("key-A", "value-A".getBytes());
        w2.put("key-B", "value-B".getBytes());

        w1.commit();
        w2.commit(); // should NOT throw

        Transaction verifier = txnManager.begin();
        assertThat(verifier.get("key-A")).isEqualTo("value-A".getBytes());
        assertThat(verifier.get("key-B")).isEqualTo("value-B".getBytes());
        verifier.commit();
    }
}
