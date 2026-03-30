package nexusdb.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory schema registry for NexusDB.
 *
 * Stores table and index schemas; thread-safe via ConcurrentHashMap.
 * Throws IllegalArgumentException on duplicate names and
 * IllegalStateException when referencing unknown tables.
 */
public final class Catalog {

    private static final Logger log = LoggerFactory.getLogger(Catalog.class);

    private final ConcurrentHashMap<String, TableSchema> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IndexSchema> indexes = new ConcurrentHashMap<>();

    // ── Table operations ──────────────────────────────────────────────────────

    /**
     * Register a new table schema.
     *
     * @throws IllegalArgumentException if a table with the same name already exists
     */
    public void createTable(TableSchema schema) {
        if (tables.putIfAbsent(schema.tableName(), schema) != null) {
            throw new IllegalArgumentException("Table already exists: " + schema.tableName());
        }
        log.debug("Created table {}", schema.tableName());
    }

    /**
     * Remove a table and all of its indexes.
     *
     * @throws IllegalArgumentException if the table does not exist
     */
    public void dropTable(String tableName) {
        if (tables.remove(tableName) == null) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        // Remove every index belonging to this table
        indexes.values().removeIf(idx -> idx.tableName().equals(tableName));
        log.debug("Dropped table {} and its indexes", tableName);
    }

    /** Return the schema for the given table, or empty if not found. */
    public Optional<TableSchema> getTable(String tableName) {
        return Optional.ofNullable(tables.get(tableName));
    }

    /** Return true if the table has been registered. */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /** Return all registered table names (unordered). */
    public List<String> listTables() {
        return new ArrayList<>(tables.keySet());
    }

    // ── Index operations ──────────────────────────────────────────────────────

    /**
     * Register a new index schema.
     *
     * @throws IllegalArgumentException if an index with the same name already exists
     * @throws IllegalStateException    if the referenced table does not exist
     */
    public void createIndex(IndexSchema schema) {
        if (!tables.containsKey(schema.tableName())) {
            throw new IllegalStateException("Table does not exist: " + schema.tableName());
        }
        if (indexes.putIfAbsent(schema.indexName(), schema) != null) {
            throw new IllegalArgumentException("Index already exists: " + schema.indexName());
        }
        log.debug("Created index {} on table {}", schema.indexName(), schema.tableName());
    }

    /**
     * Remove an index by name.
     *
     * @throws IllegalArgumentException if the index does not exist
     */
    public void dropIndex(String indexName) {
        if (indexes.remove(indexName) == null) {
            throw new IllegalArgumentException("Index does not exist: " + indexName);
        }
        log.debug("Dropped index {}", indexName);
    }

    /** Return all indexes defined on a given table (may be empty). */
    public List<IndexSchema> getIndexesForTable(String tableName) {
        return indexes.values().stream()
                .filter(idx -> idx.tableName().equals(tableName))
                .collect(Collectors.toList());
    }

    /**
     * Return the first index on {@code tableName} that covers {@code columnName},
     * or empty if none exists.
     */
    public Optional<IndexSchema> getIndexForColumn(String tableName, String columnName) {
        return indexes.values().stream()
                .filter(idx -> idx.tableName().equals(tableName)
                        && idx.columns().contains(columnName))
                .findFirst();
    }

    /** Return all registered index names (unordered). */
    public List<String> listIndexes() {
        return new ArrayList<>(indexes.keySet());
    }
}
