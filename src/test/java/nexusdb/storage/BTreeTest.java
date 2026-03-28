package nexusdb.storage;

import nexusdb.mvcc.VersionChain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for BTree — concurrent B-Tree with hand-over-hand crabbing.
 *
 * Tests cover: point lookup, insert, range scan, concurrent access,
 * split propagation, and computeIfAbsent semantics.
 */
class BTreeTest {

    private BTree tree;

    @BeforeEach
    void setUp() {
        tree = new BTree(4); // order=4 for easy split testing
    }

    // ========== Point Operations ==========

    @Test
    void getReturnsNullForEmptyTree() {
        assertThat(tree.get("missing")).isNull();
    }

    @Test
    void putAndGetSingleKey() {
        VersionChain chain = tree.computeIfAbsent("hello");
        assertThat(chain).isNotNull();
        assertThat(tree.get("hello")).isSameAs(chain);
    }

    @Test
    void putMultipleKeysAndRetrieve() {
        VersionChain a = tree.computeIfAbsent("alpha");
        VersionChain b = tree.computeIfAbsent("bravo");
        VersionChain c = tree.computeIfAbsent("charlie");

        assertThat(tree.get("alpha")).isSameAs(a);
        assertThat(tree.get("bravo")).isSameAs(b);
        assertThat(tree.get("charlie")).isSameAs(c);
        assertThat(tree.get("missing")).isNull();
    }

    @Test
    void computeIfAbsentReturnsSameChainOnSecondCall() {
        VersionChain first = tree.computeIfAbsent("key");
        VersionChain second = tree.computeIfAbsent("key");
        assertThat(second).isSameAs(first);
    }

    // ========== Split Propagation ==========

    @Test
    void insertsBeyondOrderTriggerSplit() {
        // Order=4: inserting 5+ keys forces at least one split
        for (int i = 0; i < 10; i++) {
            tree.computeIfAbsent("key" + String.format("%02d", i));
        }

        // All 10 keys must be retrievable after splits
        for (int i = 0; i < 10; i++) {
            String key = "key" + String.format("%02d", i);
            assertThat(tree.get(key)).as("key %s should exist", key).isNotNull();
        }
    }

    @Test
    void manyInsertionsAllRetrievable() {
        // Stress test: 1000 keys with splits at every level
        for (int i = 0; i < 1000; i++) {
            tree.computeIfAbsent("k" + String.format("%04d", i));
        }

        for (int i = 0; i < 1000; i++) {
            String key = "k" + String.format("%04d", i);
            assertThat(tree.get(key)).as("key %s should exist", key).isNotNull();
        }
    }

    // ========== Range Scan ==========

    @Test
    void rangeScanReturnsKeysInOrder() {
        tree.computeIfAbsent("delta");
        tree.computeIfAbsent("alpha");
        tree.computeIfAbsent("charlie");
        tree.computeIfAbsent("bravo");
        tree.computeIfAbsent("echo");

        List<String> range = tree.rangeScanKeys("bravo", "echo");

        // Should include bravo, charlie, delta (>= bravo, < echo)
        assertThat(range).containsExactly("bravo", "charlie", "delta");
    }

    @Test
    void rangeScanEmptyRange() {
        tree.computeIfAbsent("alpha");
        tree.computeIfAbsent("zulu");

        List<String> range = tree.rangeScanKeys("bravo", "delta");
        assertThat(range).isEmpty();
    }

    @Test
    void rangeScanAllKeys() {
        for (int i = 0; i < 20; i++) {
            tree.computeIfAbsent("k" + String.format("%02d", i));
        }

        // Range covering all keys
        List<String> range = tree.rangeScanKeys("k00", "k99");
        assertThat(range).hasSize(20);
        // Verify sorted
        for (int i = 1; i < range.size(); i++) {
            assertThat(range.get(i)).isGreaterThan(range.get(i - 1));
        }
    }

    // ========== Concurrent Access ==========

    @Test
    void concurrentInsertsAreThreadSafe() throws Exception {
        int threadCount = 20;
        int keysPerThread = 50;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger failures = new AtomicInteger(0);

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            threads[t] = Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    for (int k = 0; k < keysPerThread; k++) {
                        String key = String.format("t%02d_k%03d", threadId, k);
                        tree.computeIfAbsent(key);
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            });
        }

        for (Thread t : threads) {
            t.join(10_000);
        }

        assertThat(failures.get()).isEqualTo(0);

        // Verify all keys are present
        for (int t = 0; t < threadCount; t++) {
            for (int k = 0; k < keysPerThread; k++) {
                String key = String.format("t%02d_k%03d", t, k);
                assertThat(tree.get(key)).as("key %s should exist", key).isNotNull();
            }
        }
    }

    @Test
    void concurrentReadsAndWritesAreThreadSafe() throws Exception {
        // Pre-populate
        for (int i = 0; i < 100; i++) {
            tree.computeIfAbsent("pre" + String.format("%03d", i));
        }

        int writerCount = 5;
        int readerCount = 10;
        CyclicBarrier barrier = new CyclicBarrier(writerCount + readerCount);
        AtomicInteger failures = new AtomicInteger(0);

        Thread[] threads = new Thread[writerCount + readerCount];

        // Writers
        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            threads[w] = Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    for (int k = 0; k < 100; k++) {
                        tree.computeIfAbsent(String.format("w%d_k%03d", writerId, k));
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            });
        }

        // Readers
        for (int r = 0; r < readerCount; r++) {
            threads[writerCount + r] = Thread.ofVirtual().start(() -> {
                try {
                    barrier.await();
                    for (int k = 0; k < 100; k++) {
                        // Read pre-populated keys — should always find them
                        tree.get("pre" + String.format("%03d", k));
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            });
        }

        for (Thread t : threads) {
            t.join(10_000);
        }

        assertThat(failures.get()).isEqualTo(0);

        // All pre-populated keys still present
        for (int i = 0; i < 100; i++) {
            assertThat(tree.get("pre" + String.format("%03d", i))).isNotNull();
        }
    }
}
