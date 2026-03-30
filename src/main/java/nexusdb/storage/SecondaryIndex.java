package nexusdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Non-clustering secondary index: maps an index key to one or more primary keys.
 *
 * Internally backed by a TreeMap for sorted iteration and range scans.
 * Thread-safe via a single ReentrantReadWriteLock (matches BTree pattern).
 *
 * <p>When {@code unique} is {@code true}, attempting to insert a second primary
 * key for the same index key throws {@link IllegalStateException}.
 */
public final class SecondaryIndex {

    private static final Logger log = LoggerFactory.getLogger(SecondaryIndex.class);

    private final String indexName;
    private final String columnName;
    private final boolean unique;

    /** index key → set of primary keys */
    private final TreeMap<String, Set<String>> tree = new TreeMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public SecondaryIndex(String indexName, String columnName) {
        this(indexName, columnName, false);
    }

    public SecondaryIndex(String indexName, String columnName, boolean unique) {
        this.indexName = indexName;
        this.columnName = columnName;
        this.unique = unique;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String getIndexName() { return indexName; }
    public String getColumnName() { return columnName; }

    // ── Mutators ──────────────────────────────────────────────────────────────

    /**
     * Add a mapping from {@code indexKey} to {@code primaryKey}.
     *
     * Idempotent: inserting the same pair twice has no effect.
     *
     * @throws IllegalStateException if this is a unique index and {@code indexKey}
     *                               already maps to a different primary key
     */
    public void insert(String indexKey, String primaryKey) {
        lock.writeLock().lock();
        try {
            Set<String> pks = tree.computeIfAbsent(indexKey, k -> new HashSet<>());
            if (unique && !pks.isEmpty() && !pks.contains(primaryKey)) {
                throw new IllegalStateException(
                        "Unique index violation on " + indexName + " for key " + indexKey);
            }
            pks.add(primaryKey);
            log.trace("insert idx={} key={} pk={}", indexName, indexKey, primaryKey);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove the mapping from {@code indexKey} to {@code primaryKey}.
     * No-op if the mapping does not exist.
     */
    public void delete(String indexKey, String primaryKey) {
        lock.writeLock().lock();
        try {
            Set<String> pks = tree.get(indexKey);
            if (pks == null) return;
            pks.remove(primaryKey);
            if (pks.isEmpty()) {
                tree.remove(indexKey);
            }
            log.trace("delete idx={} key={} pk={}", indexName, indexKey, primaryKey);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ── Queries ───────────────────────────────────────────────────────────────

    /**
     * Exact-match lookup. Returns an unmodifiable copy of the primary-key set,
     * or an empty set if the key is not indexed.
     */
    public Set<String> lookup(String indexKey) {
        lock.readLock().lock();
        try {
            Set<String> pks = tree.get(indexKey);
            return pks == null ? Set.of() : Set.copyOf(pks);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Inclusive range scan: returns all primary keys whose index key satisfies
     * {@code fromKey <= key <= toKey}, in ascending index-key order.
     */
    public List<String> rangeScan(String fromKey, String toKey) {
        lock.readLock().lock();
        try {
            return tree.subMap(fromKey, true, toKey, true)
                    .values()
                    .stream()
                    .flatMap(Set::stream)
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return all entries sorted by index key.
     * Each entry is (indexKey → unmodifiable set of primary keys).
     */
    public List<Map.Entry<String, Set<String>>> scanAll() {
        lock.readLock().lock();
        try {
            return tree.entrySet().stream()
                    .map(e -> Map.entry(e.getKey(), Set.copyOf(e.getValue())))
                    .collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Total number of distinct index-key entries (not primary keys).
     */
    public int size() {
        lock.readLock().lock();
        try {
            return tree.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
