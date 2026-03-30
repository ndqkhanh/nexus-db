package nexusdb.shard;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal key-value store interface used by {@link ShardedStore}.
 */
public interface Store {

    void put(String key, String value);

    Optional<String> get(String key);

    void delete(String key);

    /**
     * Return all entries with {@code fromKey <= key <= toKey} in ascending key order.
     */
    List<Map.Entry<String, String>> scan(String fromKey, String toKey);
}
