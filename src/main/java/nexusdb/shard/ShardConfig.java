package nexusdb.shard;

/**
 * Metadata for a single shard.
 *
 * @param shardId     zero-based shard identifier
 * @param description human-readable label
 */
public record ShardConfig(int shardId, String description) {}
