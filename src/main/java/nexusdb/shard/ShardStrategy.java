package nexusdb.shard;

import java.util.List;

/**
 * Sealed interface representing a sharding strategy.
 *
 * <ul>
 *   <li>{@link HashShard} — distributes keys by {@code hash(key) % shardCount}.</li>
 *   <li>{@link RangeShard} — routes keys to a shard by binary-searching sorted boundaries.</li>
 * </ul>
 */
public sealed interface ShardStrategy permits ShardStrategy.HashShard, ShardStrategy.RangeShard {

    /**
     * Hash-based sharding: {@code Math.abs(key.hashCode()) % shardCount}.
     *
     * @param shardCount total number of shards (must be &gt;= 1)
     */
    record HashShard(int shardCount) implements ShardStrategy {
        public HashShard {
            if (shardCount < 1) throw new IllegalArgumentException("shardCount must be >= 1");
        }
    }

    /**
     * Range-based sharding using sorted boundary strings.
     *
     * <p>Shard assignment:
     * <ul>
     *   <li>key &lt; boundaries[0]  → shard 0</li>
     *   <li>boundaries[i-1] &lt;= key &lt; boundaries[i]  → shard i</li>
     *   <li>key &gt;= boundaries[last]  → shard boundaries.size()</li>
     * </ul>
     *
     * @param boundaries sorted list of split-point strings (must be non-null, may be empty)
     */
    record RangeShard(List<String> boundaries) implements ShardStrategy {
        public RangeShard {
            boundaries = List.copyOf(boundaries); // defensive copy + null check
        }
    }
}
