package nexusdb.shard;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

class ShardedStoreTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Build a ShardedStore with N hash shards backed by InMemoryStores. */
    private static ShardedStore hashStore(int shardCount) {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(shardCount));
        Map<Integer, Store> shards = new HashMap<>();
        for (int i = 0; i < shardCount; i++) shards.put(i, new InMemoryStore());
        return new ShardedStore(router, shards);
    }

    /** Build a ShardedStore with range boundaries backed by InMemoryStores. */
    private static ShardedStore rangeStore(List<String> boundaries) {
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(boundaries));
        int count = boundaries.size() + 1;
        Map<Integer, Store> shards = new HashMap<>();
        for (int i = 0; i < count; i++) shards.put(i, new InMemoryStore());
        return new ShardedStore(router, shards);
    }

    private ShardedStore store;

    @BeforeEach
    void setUp() {
        store = hashStore(4);
    }

    // ── Basic get / put ───────────────────────────────────────────────────────

    @Test
    void put_and_get_roundTrip() {
        store.put("hello", "world");
        assertThat(store.get("hello")).hasValue("world");
    }

    @Test
    void get_nonexistent_returnsEmpty() {
        assertThat(store.get("missing")).isEmpty();
    }

    @Test
    void put_overwritesExistingValue() {
        store.put("key", "v1");
        store.put("key", "v2");
        assertThat(store.get("key")).hasValue("v2");
    }

    @Test
    void keysOnDifferentShards_storedIndependently() {
        store.put("alpha", "A");
        store.put("beta",  "B");
        store.put("gamma", "C");

        assertThat(store.get("alpha")).hasValue("A");
        assertThat(store.get("beta")).hasValue("B");
        assertThat(store.get("gamma")).hasValue("C");
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    @Test
    void delete_removesKey() {
        store.put("k", "v");
        store.delete("k");
        assertThat(store.get("k")).isEmpty();
    }

    @Test
    void delete_nonexistent_isNoOp() {
        assertThatCode(() -> store.delete("missing")).doesNotThrowAnyException();
    }

    // ── Scan ──────────────────────────────────────────────────────────────────

    @Test
    void scan_acrossShards_mergesResultsSorted() {
        // Use a range-based store so scan results are deterministic
        ShardedStore rs = rangeStore(List.of("f", "m", "t"));
        rs.put("apple",  "1");
        rs.put("banana", "2");
        rs.put("grape",  "3");
        rs.put("orange", "4");
        rs.put("plum",   "5");

        List<Map.Entry<String, String>> result = rs.scan("apple", "plum");
        List<String> keys = result.stream().map(Map.Entry::getKey).collect(Collectors.toList());
        assertThat(keys).containsExactly("apple", "banana", "grape", "orange", "plum");
    }

    @Test
    void scan_withinSingleShard() {
        ShardedStore rs = rangeStore(List.of("m"));
        rs.put("ant",   "1");
        rs.put("bee",   "2");
        rs.put("cat",   "3");
        rs.put("zebra", "4");

        // "ant".."cat" all < "m" → shard 0 only
        List<Map.Entry<String, String>> result = rs.scan("ant", "cat");
        assertThat(result).hasSize(3);
        assertThat(result).extracting(Map.Entry::getKey)
                .containsExactly("ant", "bee", "cat");
    }

    @Test
    void scan_fanOut_returnsSortedResults() {
        ShardedStore rs = rangeStore(List.of("g", "n"));
        rs.put("cherry", "c");
        rs.put("apple",  "a");
        rs.put("mango",  "m");
        rs.put("orange", "o");

        List<Map.Entry<String, String>> result = rs.scan("apple", "orange");
        assertThat(result).extracting(Map.Entry::getKey)
                .containsExactly("apple", "cherry", "mango", "orange");
    }

    @Test
    void multipleShards_withDifferentKeys() {
        ShardedStore hs = hashStore(8);
        for (int i = 0; i < 20; i++) {
            hs.put("key-" + i, "val-" + i);
        }
        for (int i = 0; i < 20; i++) {
            assertThat(hs.get("key-" + i)).hasValue("val-" + i);
        }
    }

    @Test
    void shardedStore_withRangeStrategy() {
        ShardedStore rs = rangeStore(List.of("k"));
        rs.put("apple", "a");  // < "k" → shard 0
        rs.put("lemon", "l");  // >= "k" → shard 1

        assertThat(rs.get("apple")).hasValue("a");
        assertThat(rs.get("lemon")).hasValue("l");
    }

    // ── Concurrency ───────────────────────────────────────────────────────────

    @Test
    void concurrent_putAndGet_isThreadSafe() throws InterruptedException {
        ShardedStore hs = hashStore(4);
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                for (int i = 0; i < perThread; i++) {
                    String key = "t" + tid + "-k" + i;
                    hs.put(key, "v" + i);
                    hs.get(key); // must not throw
                }
            });
        }

        latch.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
}
