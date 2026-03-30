package nexusdb.sql;

import java.util.*;

/**
 * In-memory schema registry: tracks tables, their column definitions, and indexes.
 *
 * Used by {@link ExecutionPlan.QueryPlanner} to decide whether an indexed column
 * access is possible, and by {@link QueryExecutor} to validate DDL and DML statements.
 */
public final class Catalog {

    /** Table name → ordered list of column definitions. */
    private final Map<String, List<SQLParser.ColumnDef>> tables = new LinkedHashMap<>();

    /**
     * Maps "table.column" → index name.
     * A column can have at most one index registered here.
     */
    private final Map<String, String> columnIndexes = new LinkedHashMap<>();

    /** All index registrations: index name → CreateIndexStatement info. */
    private final Map<String, SQLParser.SQLStatement.CreateIndexStatement> indexes = new LinkedHashMap<>();

    // =========================================================
    // Registration
    // =========================================================

    /** Register a table schema. Throws if the table already exists. */
    public void createTable(String tableName, List<SQLParser.ColumnDef> columns) {
        if (tables.containsKey(tableName)) {
            throw new SQLExecutionException("Table already exists: " + tableName);
        }
        tables.put(tableName, List.copyOf(columns));
    }

    /** Register an index. Throws if index name already exists. */
    public void createIndex(SQLParser.SQLStatement.CreateIndexStatement stmt) {
        if (indexes.containsKey(stmt.indexName())) {
            throw new SQLExecutionException("Index already exists: " + stmt.indexName());
        }
        indexes.put(stmt.indexName(), stmt);
        for (String col : stmt.columns()) {
            columnIndexes.put(stmt.table() + "." + col, stmt.indexName());
        }
        // Also auto-register primary key columns as indexed
    }

    /** Register a primary-key index automatically during CREATE TABLE. */
    public void registerPrimaryKey(String table, String pkColumn) {
        String pkIndexName = "pk_" + table + "_" + pkColumn;
        columnIndexes.putIfAbsent(table + "." + pkColumn, pkIndexName);
    }

    // =========================================================
    // Queries
    // =========================================================

    /** Returns true if a table with the given name is registered. */
    public boolean tableExists(String tableName) {
        return tables.containsKey(tableName);
    }

    /** Returns the column definitions for a table, or empty list if unknown. */
    public List<SQLParser.ColumnDef> getColumns(String tableName) {
        return tables.getOrDefault(tableName, Collections.emptyList());
    }

    /**
     * Returns the index name for a column, or {@code null} if no index exists.
     *
     * @param table  table name
     * @param column bare column name (no table prefix)
     */
    public String findIndex(String table, String column) {
        return columnIndexes.get(table + "." + column);
    }

    /** Returns true if the given table has a registered index on {@code column}. */
    public boolean hasIndex(String table, String column) {
        return columnIndexes.containsKey(table + "." + column);
    }

    /** Returns all registered table names. */
    public Set<String> tableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }
}
