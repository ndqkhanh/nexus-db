package nexusdb.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Routes keys to shard IDs based on a {@link ShardStrategy}.
 *
 * <p>Supports both hash-based and range-based strategies, range queries,
 * and a simple rebalance migration plan.
 */
public final class ShardRouter {

    private static final Logger log = LoggerFactory.getLogger(ShardRouter.class);

    private final ShardStrategy strategy;

    public ShardRouter(ShardStrategy strategy) {
        this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
    }

    // ── Routing ───────────────────────────────────────────────────────────────

    /**
     * Return the shard ID for a single key.
     */
    public int getShard(String key) {
        return switch (strategy) {
            case ShardStrategy.HashShard h -> Math.abs(key.hashCode() % h.shardCount());
            case ShardStrategy.RangeShard r -> rangeShard(key, r.boundaries());
        };
    }

    /**
     * Return all shard IDs that may contain keys in [fromKey, toKey] (inclusive).
     */
    public Set<Integer> getShardForRange(String fromKey, String toKey) {
        Set<Integer> shards = new HashSet<>();
        switch (strategy) {
            case ShardStrategy.HashShard h -> {
                // With hashing, any shard could hold a key in the range
                for (int i = 0; i < h.shardCount(); i++) shards.add(i);
            }
            case ShardStrategy.RangeShard r -> {
                int from = rangeShard(fromKey, r.boundaries());
                int to   = rangeShard(toKey,   r.boundaries());
                for (int i = from; i <= to; i++) shards.add(i);
            }
        }
        return shards;
    }

    // ── Metadata ──────────────────────────────────────────────────────────────

    /** Total number of shards managed by this router. */
    public int shardCount() {
        return switch (strategy) {
            case ShardStrategy.HashShard h -> h.shardCount();
            case ShardStrategy.RangeShard r -> r.boundaries().size() + 1;
        };
    }

    /**
     * Produce a migration plan for re-sharding to {@code newShardCount}.
     *
     * <p>Only meaningful for hash-based strategies. Returns a map of
     * {@code oldShardId → list of new shard IDs} that the old shard's keys
     * might migrate to. For range strategies, returns an empty map.
     *
     * <p>Implementation: for each old shard, every key {@code k} with
     * {@code hash(k) % oldCount == oldShard} may end up at any new shard —
     * we conservatively list all new shards as potential targets.
     * For a simpler deterministic plan we map old → new by modulo reduction.
     */
    public Map<Integer, List<Integer>> rebalance(int newShardCount) {
        if (newShardCount < 1) throw new IllegalArgumentException("newShardCount must be >= 1");
        Map<Integer, List<Integer>> plan = new LinkedHashMap<>();

        switch (strategy) {
            case ShardStrategy.HashShard h -> {
                int oldCount = h.shardCount();
                for (int oldShard = 0; oldShard < oldCount; oldShard++) {
                    // A key in oldShard satisfies hash(key) % oldCount == oldShard.
                    // After rebalance: hash(key) % newCount can be any value from 0..newCount-1
                    // We list all newCount shards as potential targets (conservative plan).
                    List<Integer> targets = new ArrayList<>();
                    for (int ns = 0; ns < newShardCount; ns++) targets.add(ns);
                    plan.put(oldShard, targets);
                }
            }
            case ShardStrategy.RangeShard ignored -> {
                // Range shards are re-configured via new boundary definitions, not migrated here
            }
        }
        return plan;
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    /**
     * Binary-search-based range shard assignment.
     *
     * <ul>
     *   <li>key &lt; boundaries[0]  → 0</li>
     *   <li>boundaries[i-1] &lt;= key &lt; boundaries[i]  → i</li>
     *   <li>key &gt;= boundaries[last]  → boundaries.size()</li>
     * </ul>
     */
    private static int rangeShard(String key, List<String> boundaries) {
        int lo = 0, hi = boundaries.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (key.compareTo(boundaries.get(mid)) < 0) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        return lo;
    }
}
