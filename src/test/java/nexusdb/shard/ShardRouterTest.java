package nexusdb.shard;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

class ShardRouterTest {

    // ── Hash sharding ─────────────────────────────────────────────────────────

    @Test
    void hashShard_distributesKeysAcrossShards() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(4));
        // With 4 shards, every shard ID must be in [0, 3]
        String[] keys = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"};
        for (String key : keys) {
            int shard = router.getShard(key);
            assertThat(shard).isBetween(0, 3);
        }
    }

    @Test
    void hashShard_sameKeyAlwaysSameShard() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(8));
        int first = router.getShard("stable-key");
        for (int i = 0; i < 20; i++) {
            assertThat(router.getShard("stable-key")).isEqualTo(first);
        }
    }

    @Test
    void hashShard_singleShardAllKeysRouteToZero() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(1));
        assertThat(router.getShard("anything")).isEqualTo(0);
        assertThat(router.getShard("other")).isEqualTo(0);
    }

    @Test
    void hashShard_shardCountReturnsCorrectCount() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(5));
        assertThat(router.shardCount()).isEqualTo(5);
    }

    @Test
    void hashShard_getShardForRange_returnsAllShards() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(3));
        Set<Integer> shards = router.getShardForRange("a", "z");
        assertThat(shards).containsExactlyInAnyOrder(0, 1, 2);
    }

    @Test
    void hashShard_rebalanceProducesMigrationPlan() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(3));
        Map<Integer, List<Integer>> plan = router.rebalance(6);
        // Old shards 0,1,2 should each have a list of new shard targets
        assertThat(plan).containsKeys(0, 1, 2);
        plan.values().forEach(targets -> assertThat(targets).isNotEmpty());
    }

    @Test
    void hashShard_rebalancePlanHasNewShardCount() {
        ShardRouter router = new ShardRouter(new ShardStrategy.HashShard(2));
        Map<Integer, List<Integer>> plan = router.rebalance(4);
        plan.values().forEach(targets ->
                targets.forEach(t -> assertThat(t).isBetween(0, 3))
        );
    }

    // ── Range sharding ────────────────────────────────────────────────────────

    @Test
    void rangeShard_routesCorrectlyBasedOnBoundaries() {
        // Boundaries: ["m", "t"]  → shards 0, 1, 2
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of("m", "t")));
        assertThat(router.getShard("apple")).isEqualTo(0);   // < "m"
        assertThat(router.getShard("n")).isEqualTo(1);       // >= "m", < "t"
        assertThat(router.getShard("zebra")).isEqualTo(2);   // >= "t"
    }

    @Test
    void rangeShard_boundaryKeyGoesToNextShard() {
        // key == boundary goes to the shard ABOVE (lo = mid+1 in binary search)
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of("m")));
        assertThat(router.getShard("m")).isEqualTo(1);  // exactly "m" → shard 1
        assertThat(router.getShard("l")).isEqualTo(0);  // < "m" → shard 0
    }

    @Test
    void rangeShard_shardCountIsBoundariesPlusOne() {
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of("d", "m", "t")));
        assertThat(router.shardCount()).isEqualTo(4);
    }

    @Test
    void rangeShard_emptyBoundaries_allKeysToShard0() {
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of()));
        assertThat(router.getShard("anything")).isEqualTo(0);
        assertThat(router.shardCount()).isEqualTo(1);
    }

    @Test
    void rangeShard_getShardForRange_returnsCorrectSubset() {
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of("f", "m", "t")));
        // Keys "a"→"h" spans shard 0 ("a"<"f") and shard 1 ("f"<="h"<"m")
        Set<Integer> shards = router.getShardForRange("a", "h");
        assertThat(shards).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void rangeShard_getShardForRange_singleShardRange() {
        ShardRouter router = new ShardRouter(new ShardStrategy.RangeShard(List.of("f", "m", "t")));
        // Keys "g"→"h" both fall in shard 1
        Set<Integer> shards = router.getShardForRange("g", "h");
        assertThat(shards).containsExactly(1);
    }
}
