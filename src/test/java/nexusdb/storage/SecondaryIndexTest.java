package nexusdb.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class SecondaryIndexTest {

    private SecondaryIndex index;

    @BeforeEach
    void setUp() {
        index = new SecondaryIndex("idx_email", "email");
    }

    // ── Insert / Lookup ───────────────────────────────────────────────────────

    @Test
    void insert_and_lookup_roundTrip() {
        index.insert("alice@example.com", "pk-1");
        assertThat(index.lookup("alice@example.com")).containsExactly("pk-1");
    }

    @Test
    void lookup_nonexistent_returnsEmptySet() {
        assertThat(index.lookup("nobody@example.com")).isEmpty();
    }

    @Test
    void insert_multiplePrimaryKeysForSameIndexKey() {
        index.insert("shared@example.com", "pk-1");
        index.insert("shared@example.com", "pk-2");
        assertThat(index.lookup("shared@example.com")).containsExactlyInAnyOrder("pk-1", "pk-2");
    }

    @Test
    void insert_duplicate_isIdempotent() {
        index.insert("a@b.com", "pk-1");
        index.insert("a@b.com", "pk-1"); // second insert same pair
        assertThat(index.lookup("a@b.com")).hasSize(1);
        assertThat(index.size()).isEqualTo(1);
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    @Test
    void delete_removesSpecificMapping() {
        index.insert("key", "pk-1");
        index.insert("key", "pk-2");
        index.delete("key", "pk-1");
        assertThat(index.lookup("key")).containsExactly("pk-2");
    }

    @Test
    void delete_removesEntryWhenSetBecomesEmpty() {
        index.insert("key", "pk-1");
        index.delete("key", "pk-1");
        assertThat(index.lookup("key")).isEmpty();
        assertThat(index.size()).isZero();
    }

    @Test
    void delete_nonexistentIsNoOp() {
        assertThatCode(() -> index.delete("missing", "pk-1")).doesNotThrowAnyException();
    }

    @Test
    void delete_nonexistentPrimaryKeyIsNoOp() {
        index.insert("key", "pk-1");
        assertThatCode(() -> index.delete("key", "pk-999")).doesNotThrowAnyException();
        assertThat(index.lookup("key")).containsExactly("pk-1");
    }

    // ── Range scan ────────────────────────────────────────────────────────────

    @Test
    void rangeScan_returnsKeysInRange() {
        index.insert("b", "pk-b");
        index.insert("a", "pk-a");
        index.insert("c", "pk-c");
        index.insert("d", "pk-d");

        List<String> result = index.rangeScan("b", "c");
        assertThat(result).containsExactlyInAnyOrder("pk-b", "pk-c");
    }

    @Test
    void rangeScan_noMatchesReturnsEmpty() {
        index.insert("a", "pk-a");
        index.insert("z", "pk-z");
        assertThat(index.rangeScan("m", "n")).isEmpty();
    }

    @Test
    void rangeScan_inclusiveBoundaries() {
        index.insert("apple", "pk-1");
        index.insert("banana", "pk-2");
        index.insert("cherry", "pk-3");

        List<String> result = index.rangeScan("apple", "cherry");
        assertThat(result).containsExactlyInAnyOrder("pk-1", "pk-2", "pk-3");
    }

    // ── ScanAll ───────────────────────────────────────────────────────────────

    @Test
    void scanAll_returnsEntriesSortedByIndexKey() {
        index.insert("c", "pk-c");
        index.insert("a", "pk-a");
        index.insert("b", "pk-b");

        List<Map.Entry<String, Set<String>>> entries = index.scanAll();
        assertThat(entries).extracting(Map.Entry::getKey)
                .containsExactly("a", "b", "c");
    }

    @Test
    void scanAll_emptyWhenNoEntries() {
        assertThat(index.scanAll()).isEmpty();
    }

    // ── Size ──────────────────────────────────────────────────────────────────

    @Test
    void size_tracksEntryCount() {
        assertThat(index.size()).isZero();
        index.insert("k1", "pk-1");
        assertThat(index.size()).isEqualTo(1);
        index.insert("k2", "pk-2");
        assertThat(index.size()).isEqualTo(2);
        index.delete("k1", "pk-1");
        assertThat(index.size()).isEqualTo(1);
    }

    @Test
    void size_multipleKeysUnderSameIndexKeyCountsOnce() {
        index.insert("email", "pk-1");
        index.insert("email", "pk-2");
        assertThat(index.size()).isEqualTo(1); // one index-key entry
    }

    // ── Unique index ──────────────────────────────────────────────────────────

    @Test
    void uniqueIndex_secondInsertWithDifferentPkThrows() {
        SecondaryIndex unique = new SecondaryIndex("idx_unique", "email", true);
        unique.insert("same@key.com", "pk-1");
        assertThatThrownBy(() -> unique.insert("same@key.com", "pk-2"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void uniqueIndex_duplicatePairIsIdempotent() {
        SecondaryIndex unique = new SecondaryIndex("idx_unique", "email", true);
        unique.insert("k", "pk-1");
        assertThatCode(() -> unique.insert("k", "pk-1")).doesNotThrowAnyException();
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    @Test
    void concurrent_insertAndLookup_isThreadSafe() throws InterruptedException {
        int threads = 8;
        int perThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < perThread; i++) {
                    String key = "key-" + (i % 10);
                    String pk  = "pk-" + tid + "-" + i;
                    index.insert(key, pk);
                    index.lookup(key); // must not throw
                }
            });
        }

        latch.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // All inserts completed; size must be <= 10 (distinct keys)
        assertThat(index.size()).isLessThanOrEqualTo(10);
    }
}
