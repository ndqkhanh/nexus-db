package nexusdb.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Wrapper that routes key-value operations to per-shard {@link Store} instances
 * using a {@link ShardRouter}.
 *
 * <p>Thread safety is delegated to the underlying stores.
 */
public final class ShardedStore {

    private static final Logger log = LoggerFactory.getLogger(ShardedStore.class);

    private final ShardRouter router;
    private final Map<Integer, Store> shards;

    /**
     * @param router the routing strategy
     * @param shards map of shardId → Store; must cover every shard the router can return
     */
    public ShardedStore(ShardRouter router, Map<Integer, Store> shards) {
        this.router = Objects.requireNonNull(router, "router must not be null");
        this.shards = Map.copyOf(Objects.requireNonNull(shards, "shards must not be null"));
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    public void put(String key, String value) {
        int shardId = router.getShard(key);
        log.trace("put key={} → shard {}", key, shardId);
        shardFor(shardId).put(key, value);
    }

    public void delete(String key) {
        int shardId = router.getShard(key);
        log.trace("delete key={} → shard {}", key, shardId);
        shardFor(shardId).delete(key);
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    public Optional<String> get(String key) {
        int shardId = router.getShard(key);
        return shardFor(shardId).get(key);
    }

    /**
     * Fan-out range scan across all relevant shards; results are merged and
     * returned in ascending key order.
     */
    public List<Map.Entry<String, String>> scan(String fromKey, String toKey) {
        Set<Integer> targetShards = router.getShardForRange(fromKey, toKey);
        return targetShards.stream()
                .flatMap(id -> shardFor(id).scan(fromKey, toKey).stream())
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toList());
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private Store shardFor(int shardId) {
        Store s = shards.get(shardId);
        if (s == null) throw new IllegalStateException("No store for shard " + shardId);
        return s;
    }
}
