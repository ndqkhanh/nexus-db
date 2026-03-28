package nexusdb.integration;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for NexusDB transaction lifecycle.
 * These define "done" for Phase 1 — all must pass before moving on.
 */
class TransactionAcceptanceTest {

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
    @DisplayName("Basic put-commit-get: written value is visible to subsequent transaction")
    void putCommitGet() {
        Transaction txn1 = db.begin();
        txn1.put("name", "alice".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        byte[] result = txn2.get("name");
        txn2.commit();

        assertThat(result).isEqualTo("alice".getBytes());
    }

    @Test
    @DisplayName("Read-your-own-writes: uncommitted write is visible within the same transaction")
    void readYourOwnWrites() {
        Transaction txn = db.begin();
        txn.put("key", "value".getBytes());

        byte[] result = txn.get("key");
        txn.commit();

        assertThat(result).isEqualTo("value".getBytes());
    }

    @Test
    @DisplayName("Aborted transaction: writes are not visible to subsequent transactions")
    void abortedWritesNotVisible() {
        Transaction txn1 = db.begin();
        txn1.put("key", "aborted-value".getBytes());
        txn1.abort();

        Transaction txn2 = db.begin();
        byte[] result = txn2.get("key");
        txn2.commit();

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Overwrite: later committed write replaces earlier committed write")
    void overwrite() {
        Transaction txn1 = db.begin();
        txn1.put("key", "v1".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        txn2.put("key", "v2".getBytes());
        txn2.commit();

        Transaction txn3 = db.begin();
        byte[] result = txn3.get("key");
        txn3.commit();

        assertThat(result).isEqualTo("v2".getBytes());
    }

    @Test
    @DisplayName("Delete: committed delete makes key invisible")
    void deleteKey() {
        Transaction txn1 = db.begin();
        txn1.put("key", "value".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        txn2.delete("key");
        txn2.commit();

        Transaction txn3 = db.begin();
        byte[] result = txn3.get("key");
        txn3.commit();

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Multiple keys: independent key-value pairs in single transaction")
    void multipleKeys() {
        Transaction txn1 = db.begin();
        txn1.put("a", "1".getBytes());
        txn1.put("b", "2".getBytes());
        txn1.put("c", "3".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        assertThat(txn2.get("a")).isEqualTo("1".getBytes());
        assertThat(txn2.get("b")).isEqualTo("2".getBytes());
        assertThat(txn2.get("c")).isEqualTo("3".getBytes());
        txn2.commit();
    }

    @Test
    @DisplayName("Non-existent key returns null")
    void nonExistentKey() {
        Transaction txn = db.begin();
        byte[] result = txn.get("does-not-exist");
        txn.commit();

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Transaction IDs are monotonically increasing")
    void monotonicTransactionIds() {
        Transaction txn1 = db.begin();
        Transaction txn2 = db.begin();
        Transaction txn3 = db.begin();

        assertThat(txn2.id()).isGreaterThan(txn1.id());
        assertThat(txn3.id()).isGreaterThan(txn2.id());

        txn1.abort();
        txn2.abort();
        txn3.abort();
    }
}
