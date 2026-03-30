package nexusdb.shard;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Thread-safe in-memory {@link Store} backed by a ConcurrentSkipListMap
 * (provides sorted iteration for range scans without external locking).
 */
public final class InMemoryStore implements Store {

    private final ConcurrentSkipListMap<String, String> data = new ConcurrentSkipListMap<>();

    @Override
    public void put(String key, String value) {
        data.put(key, value);
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(data.get(key));
    }

    @Override
    public void delete(String key) {
        data.remove(key);
    }

    @Override
    public List<Map.Entry<String, String>> scan(String fromKey, String toKey) {
        return new ArrayList<>(data.subMap(fromKey, true, toKey, true).entrySet());
    }
}
