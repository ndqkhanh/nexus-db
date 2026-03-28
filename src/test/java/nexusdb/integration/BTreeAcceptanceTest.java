package nexusdb.integration;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for Phase 3: B-Tree integration with range scans.
 *
 * Validates that the B-Tree storage engine supports point operations,
 * range scans, and concurrent access through the NexusDB public API.
 */
class BTreeAcceptanceTest {

    private NexusDB db;

    @BeforeEach
    void setUp() {
        db = NexusDB.open();
    }

    @AfterEach
    void tearDown() {
        db.close();
    }

    @Test
    void rangeScanReturnsCommittedKeysInOrder() {
        Transaction txn = db.begin();
        txn.put("user:charlie", "Charlie".getBytes());
        txn.put("user:alice", "Alice".getBytes());
        txn.put("user:bob", "Bob".getBytes());
        txn.put("user:delta", "Delta".getBytes());
        txn.put("order:001", "Order1".getBytes());
        txn.commit();

        Transaction reader = db.begin();
        List<String> users = reader.scan("user:", "user:~");

        assertThat(users).containsExactly(
                "user:alice", "user:bob", "user:charlie", "user:delta"
        );

        // order: prefix should not appear
        List<String> orders = reader.scan("order:", "order:~");
        assertThat(orders).containsExactly("order:001");

        reader.abort();
    }

    @Test
    void rangeScanSeesSnapshotConsistentView() {
        // Write initial data
        Transaction t1 = db.begin();
        t1.put("k1", "v1".getBytes());
        t1.put("k2", "v2".getBytes());
        t1.put("k3", "v3".getBytes());
        t1.commit();

        // Start reader BEFORE new writes
        Transaction reader = db.begin();

        // Write more data AFTER reader's snapshot
        Transaction t2 = db.begin();
        t2.put("k2a", "v2a".getBytes());
        t2.commit();

        // Reader should NOT see k2a (written after snapshot)
        List<String> keys = reader.scan("k", "l");
        assertThat(keys).containsExactly("k1", "k2", "k3");

        reader.abort();
    }

    @Test
    void rangeScanWithManyKeysAcrossMultipleSplits() {
        // Insert enough keys to trigger many B-Tree splits
        Transaction writer = db.begin();
        for (int i = 0; i < 500; i++) {
            writer.put(String.format("item:%04d", i), ("val" + i).getBytes());
        }
        writer.commit();

        Transaction reader = db.begin();
        List<String> range = reader.scan("item:0100", "item:0200");

        // Should get items 0100..0199 (100 keys)
        assertThat(range).hasSize(100);
        assertThat(range.get(0)).isEqualTo("item:0100");
        assertThat(range.get(99)).isEqualTo("item:0199");

        // Full scan
        List<String> all = reader.scan("item:", "item:~");
        assertThat(all).hasSize(500);

        reader.abort();
    }

    @Test
    void rangeScanEmptyResult() {
        Transaction txn = db.begin();
        txn.put("aaa", "val".getBytes());
        txn.put("zzz", "val".getBytes());
        txn.commit();

        Transaction reader = db.begin();
        List<String> empty = reader.scan("mmm", "nnn");
        assertThat(empty).isEmpty();
        reader.abort();
    }
}
