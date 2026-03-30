package nexusdb.sql;

import nexusdb.transaction.Transaction;
import nexusdb.transaction.TransactionManager;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Executes SQL statements against the NexusDB storage engine.
 *
 * <p>Workflow: SQL string → {@link SQLParser} → {@link ExecutionPlan.QueryPlanner} → execute.
 *
 * <p>For SELECT, traverses the plan tree, reading rows from the
 * {@link TransactionManager} via MVCC transactions.
 * For INSERT/UPDATE/DELETE, opens a transaction, performs the write, and commits.
 * For CREATE TABLE/INDEX, registers the schema in the {@link Catalog}.
 * For EXPLAIN, returns the plan string instead of executing the query.
 */
public final class QueryExecutor {

    /** Column and row result set. */
    public record ResultSet(List<String> columns, List<Map<String, String>> rows) {
        public static ResultSet empty() {
            return new ResultSet(Collections.emptyList(), Collections.emptyList());
        }
        public static ResultSet message(String msg) {
            return new ResultSet(List.of("result"), List.of(Map.of("result", msg)));
        }
    }

    // =========================================================
    // Row storage key scheme
    //
    // Each "row" in the BTree-backed store is serialised as a single string value.
    // Key format  : "<table>:<pk_value>"
    // Value format: "col1=val1\0col2=val2\0..."  (NUL-delimited key=value pairs)
    //
    // For tables without a declared PRIMARY KEY we fall back to using
    // the first column as the implicit PK.
    // =========================================================

    private static final String KV_SEP   = "\0"; // separates col=val entries
    private static final String COL_SEP  = "=";  // separates col name from value

    private final TransactionManager txnManager;
    private final Catalog            catalog;
    private final SQLParser          sqlParser;
    private final ExecutionPlan.QueryPlanner planner;

    public QueryExecutor(TransactionManager txnManager) {
        this.txnManager = txnManager;
        this.catalog    = new Catalog();
        this.sqlParser  = new SQLParser();
        this.planner    = new ExecutionPlan.QueryPlanner(catalog);
    }

    /** Package-visible: allows tests to share the catalog for pre-population. */
    Catalog catalog() { return catalog; }

    // =========================================================
    // Public entry point
    // =========================================================

    /**
     * Parse and execute a SQL string.
     *
     * @throws SQLParseException     on syntax errors
     * @throws SQLExecutionException on semantic errors (unknown table, type mismatch, …)
     */
    public ResultSet execute(String sql) {
        SQLParser.SQLStatement stmt = sqlParser.parse(sql);
        return executeStatement(stmt);
    }

    // =========================================================
    // Statement dispatch
    // =========================================================

    private ResultSet executeStatement(SQLParser.SQLStatement stmt) {
        return switch (stmt) {
            case SQLParser.SQLStatement.SelectStatement s  -> executeSelect(s);
            case SQLParser.SQLStatement.InsertStatement s  -> executeInsert(s);
            case SQLParser.SQLStatement.UpdateStatement s  -> executeUpdate(s);
            case SQLParser.SQLStatement.DeleteStatement s  -> executeDelete(s);
            case SQLParser.SQLStatement.CreateTableStatement s -> executeCreateTable(s);
            case SQLParser.SQLStatement.CreateIndexStatement s -> executeCreateIndex(s);
            case SQLParser.SQLStatement.ExplainStatement s -> executeExplain(s);
        };
    }

    // =========================================================
    // SELECT
    // =========================================================

    private ResultSet executeSelect(SQLParser.SQLStatement.SelectStatement select) {
        String table = select.table();
        requireTableExists(table);

        Transaction txn = txnManager.begin();
        try {
            List<Map<String, String>> rows = scanTable(txn, table);

            // Apply WHERE filter
            if (select.where() != null) {
                rows = applyFilter(rows, select.where());
            }

            // Apply ORDER BY
            if (select.orderBy() != null) {
                rows = applySort(rows, select.orderBy());
            }

            // Determine output columns
            List<String> outputCols = resolveOutputColumns(table, select.columns());
            rows = applyProject(rows, outputCols);

            txn.commit();
            return new ResultSet(outputCols, rows);
        } catch (Exception e) {
            txn.abort();
            throw e;
        }
    }

    // =========================================================
    // INSERT
    // =========================================================

    private ResultSet executeInsert(SQLParser.SQLStatement.InsertStatement insert) {
        String table = insert.table();
        requireTableExists(table);

        List<SQLParser.ColumnDef> schemaCols = catalog.getColumns(table);
        validateInsertColumns(insert.columns(), schemaCols);

        Transaction txn = txnManager.begin();
        try {
            for (List<String> valueRow : insert.rows()) {
                if (valueRow.size() != insert.columns().size()) {
                    throw new SQLExecutionException(
                            "Column count " + insert.columns().size() +
                            " does not match value count " + valueRow.size());
                }

                // Build column → value map for this row
                Map<String, String> rowData = new LinkedHashMap<>();
                for (int i = 0; i < insert.columns().size(); i++) {
                    String colName = insert.columns().get(i);
                    String value   = valueRow.get(i);
                    validateColumnValue(colName, value, schemaCols);
                    rowData.put(colName, value);
                }

                // Determine primary key value
                String pkValue = getPkValue(table, rowData);
                String storageKey = tableKey(table, pkValue);
                String storageVal = serializeRow(rowData);

                txn.put(storageKey, storageVal.getBytes(StandardCharsets.UTF_8));
            }
            txn.commit();
        } catch (Exception e) {
            txn.abort();
            throw e;
        }

        return ResultSet.message("OK");
    }

    // =========================================================
    // UPDATE
    // =========================================================

    private ResultSet executeUpdate(SQLParser.SQLStatement.UpdateStatement update) {
        String table = update.table();
        requireTableExists(table);

        Transaction txn = txnManager.begin();
        try {
            List<Map<String, String>> rows = scanTable(txn, table);

            // Filter rows to update
            List<Map<String, String>> toUpdate = update.where() != null
                    ? applyFilter(rows, update.where())
                    : rows;

            int count = 0;
            for (Map<String, String> row : toUpdate) {
                Map<String, String> newRow = new LinkedHashMap<>(row);
                for (var entry : update.setClause().entrySet()) {
                    String colName = entry.getKey();
                    String newVal  = entry.getValue();
                    validateColumnValue(colName, newVal, catalog.getColumns(table));
                    newRow.put(colName, newVal);
                }
                String pkValue    = getPkValue(table, row);
                String storageKey = tableKey(table, pkValue);
                txn.put(storageKey, serializeRow(newRow).getBytes(StandardCharsets.UTF_8));
                count++;
            }
            txn.commit();
            return ResultSet.message("Updated " + count + " row(s)");
        } catch (Exception e) {
            txn.abort();
            throw e;
        }
    }

    // =========================================================
    // DELETE
    // =========================================================

    private ResultSet executeDelete(SQLParser.SQLStatement.DeleteStatement delete) {
        String table = delete.table();
        requireTableExists(table);

        Transaction txn = txnManager.begin();
        try {
            List<Map<String, String>> rows = scanTable(txn, table);

            List<Map<String, String>> toDelete = delete.where() != null
                    ? applyFilter(rows, delete.where())
                    : rows;

            int count = 0;
            for (Map<String, String> row : toDelete) {
                String pkValue    = getPkValue(table, row);
                String storageKey = tableKey(table, pkValue);
                txn.delete(storageKey);
                count++;
            }
            txn.commit();
            return ResultSet.message("Deleted " + count + " row(s)");
        } catch (Exception e) {
            txn.abort();
            throw e;
        }
    }

    // =========================================================
    // CREATE TABLE / INDEX
    // =========================================================

    private ResultSet executeCreateTable(SQLParser.SQLStatement.CreateTableStatement stmt) {
        catalog.createTable(stmt.table(), stmt.columns());
        // Auto-register PK index
        for (SQLParser.ColumnDef col : stmt.columns()) {
            if (col.isPrimaryKey()) {
                catalog.registerPrimaryKey(stmt.table(), col.name());
            }
        }
        return ResultSet.message("Table created: " + stmt.table());
    }

    private ResultSet executeCreateIndex(SQLParser.SQLStatement.CreateIndexStatement stmt) {
        requireTableExists(stmt.table());
        catalog.createIndex(stmt);
        return ResultSet.message("Index created: " + stmt.indexName());
    }

    // =========================================================
    // EXPLAIN
    // =========================================================

    private ResultSet executeExplain(SQLParser.SQLStatement.ExplainStatement explain) {
        ExecutionPlan.PlanNode plan = planner.plan(explain.inner());
        String planText = plan != null
                ? ExecutionPlan.explain(plan)
                : "(no read plan — direct write statement)";
        return new ResultSet(
                List.of("plan"),
                List.of(Map.of("plan", planText)));
    }

    // =========================================================
    // Storage helpers
    // =========================================================

    /** Scan all rows for a table visible at the current transaction snapshot. */
    private List<Map<String, String>> scanTable(Transaction txn, String table) {
        String from = tableKey(table, "");          // "" sorts before any pk value
        String to   = tableKey(table, "\uFFFF");    // high sentinel
        List<String> keys = txn.scan(from, to);

        List<Map<String, String>> rows = new ArrayList<>();
        for (String key : keys) {
            byte[] raw = txn.get(key);
            if (raw != null) {
                rows.add(deserializeRow(raw));
            }
        }
        return rows;
    }

    private static String tableKey(String table, String pkValue) {
        return table + ":" + pkValue;
    }

    private static String serializeRow(Map<String, String> row) {
        StringBuilder sb = new StringBuilder();
        for (var entry : row.entrySet()) {
            if (!sb.isEmpty()) sb.append(KV_SEP);
            sb.append(entry.getKey()).append(COL_SEP).append(entry.getValue());
        }
        return sb.toString();
    }

    private static Map<String, String> deserializeRow(byte[] raw) {
        String s = new String(raw, StandardCharsets.UTF_8);
        Map<String, String> row = new LinkedHashMap<>();
        for (String pair : s.split(KV_SEP, -1)) {
            int eq = pair.indexOf(COL_SEP);
            if (eq > 0) {
                row.put(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return row;
    }

    // =========================================================
    // Plan execution helpers (filter / sort / project)
    // =========================================================

    private List<Map<String, String>> applyFilter(
            List<Map<String, String>> rows, SQLParser.WhereClause where) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : rows) {
            if (evaluateWhere(row, where)) result.add(row);
        }
        return result;
    }

    private boolean evaluateWhere(Map<String, String> row, SQLParser.WhereClause where) {
        return switch (where) {
            case SQLParser.WhereClause.And a ->
                    evaluateWhere(row, a.left()) && evaluateWhere(row, a.right());
            case SQLParser.WhereClause.Or o ->
                    evaluateWhere(row, o.left()) || evaluateWhere(row, o.right());
            case SQLParser.WhereClause.Comparison c -> {
                String col = stripTablePrefix(c.column());
                String rowVal = row.getOrDefault(col, null);
                if (rowVal == null) yield false;
                yield compareValues(rowVal, c.operator(), c.value());
            }
        };
    }

    private boolean compareValues(String actual, SQLParser.Operator op, String expected) {
        // Try numeric comparison first
        try {
            long a = Long.parseLong(actual);
            long b = Long.parseLong(expected);
            return switch (op) {
                case EQ -> a == b;
                case NE -> a != b;
                case LT -> a <  b;
                case GT -> a >  b;
                case LE -> a <= b;
                case GE -> a >= b;
            };
        } catch (NumberFormatException ignored) {}

        // Boolean comparison
        if (isBooleanValue(actual) && isBooleanValue(expected)) {
            boolean a = Boolean.parseBoolean(actual);
            boolean b = Boolean.parseBoolean(expected);
            return switch (op) {
                case EQ -> a == b;
                case NE -> a != b;
                default -> throw new SQLExecutionException("Boolean columns only support = and !=");
            };
        }

        // String comparison
        int cmp = actual.compareTo(expected);
        return switch (op) {
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
            case LT -> cmp <  0;
            case GT -> cmp >  0;
            case LE -> cmp <= 0;
            case GE -> cmp >= 0;
        };
    }

    private static boolean isBooleanValue(String v) {
        return "true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v);
    }

    private List<Map<String, String>> applySort(
            List<Map<String, String>> rows, SQLParser.OrderByClause orderBy) {
        String col = stripTablePrefix(orderBy.column());
        List<Map<String, String>> sorted = new ArrayList<>(rows);
        sorted.sort((r1, r2) -> {
            String v1 = r1.getOrDefault(col, "");
            String v2 = r2.getOrDefault(col, "");
            int cmp;
            try {
                cmp = Long.compare(Long.parseLong(v1), Long.parseLong(v2));
            } catch (NumberFormatException e) {
                cmp = v1.compareTo(v2);
            }
            return orderBy.ascending() ? cmp : -cmp;
        });
        return sorted;
    }

    private List<Map<String, String>> applyProject(
            List<Map<String, String>> rows, List<String> columns) {
        if (columns.isEmpty()) return rows; // SELECT * — keep all columns
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : rows) {
            Map<String, String> projected = new LinkedHashMap<>();
            for (String col : columns) {
                String bare = stripTablePrefix(col);
                projected.put(bare, row.getOrDefault(bare, null));
            }
            result.add(projected);
        }
        return result;
    }

    // =========================================================
    // Schema / validation helpers
    // =========================================================

    private void requireTableExists(String table) {
        if (!catalog.tableExists(table)) {
            throw new SQLExecutionException("Table does not exist: " + table);
        }
    }

    private void validateInsertColumns(List<String> insertCols,
                                       List<SQLParser.ColumnDef> schemaCols) {
        Set<String> schemaColNames = new HashSet<>();
        for (SQLParser.ColumnDef cd : schemaCols) schemaColNames.add(cd.name());
        for (String col : insertCols) {
            if (!schemaColNames.contains(col)) {
                throw new SQLExecutionException("Unknown column in INSERT: " + col);
            }
        }
    }

    private void validateColumnValue(String colName, String value,
                                     List<SQLParser.ColumnDef> schemaCols) {
        for (SQLParser.ColumnDef cd : schemaCols) {
            if (!cd.name().equals(colName)) continue;
            if ("null".equals(value) && cd.isNotNull()) {
                throw new SQLExecutionException("Column " + colName + " cannot be NULL");
            }
            if ("null".equals(value)) return; // nullable, allowed
            if (cd.type() == SQLParser.ColumnType.INT) {
                try { Long.parseLong(value); }
                catch (NumberFormatException e) {
                    throw new SQLExecutionException(
                            "Column " + colName + " expects INT but got: " + value);
                }
            }
            if (cd.type() == SQLParser.ColumnType.BOOLEAN) {
                if (!isBooleanValue(value)) {
                    throw new SQLExecutionException(
                            "Column " + colName + " expects BOOLEAN but got: " + value);
                }
            }
            return;
        }
        // Column not found in schema — allow (unknown columns are silently ignored for UPDATE)
    }

    /** Determine the primary key value for a row map. Uses the first PK column, or first column. */
    private String getPkValue(String table, Map<String, String> row) {
        for (SQLParser.ColumnDef cd : catalog.getColumns(table)) {
            if (cd.isPrimaryKey()) {
                return row.getOrDefault(cd.name(), "");
            }
        }
        // No PK declared — use first column
        List<SQLParser.ColumnDef> cols = catalog.getColumns(table);
        if (!cols.isEmpty()) {
            return row.getOrDefault(cols.get(0).name(), "");
        }
        return UUID.randomUUID().toString();
    }

    private List<String> resolveOutputColumns(String table, List<String> requestedCols) {
        if (requestedCols.isEmpty()) {
            // SELECT * — return all schema columns
            List<String> all = new ArrayList<>();
            for (SQLParser.ColumnDef cd : catalog.getColumns(table)) {
                all.add(cd.name());
            }
            return all.isEmpty() ? List.of("*") : all;
        }
        List<String> bare = new ArrayList<>();
        for (String col : requestedCols) bare.add(stripTablePrefix(col));
        return bare;
    }

    private static String stripTablePrefix(String col) {
        int dot = col.indexOf('.');
        return dot >= 0 ? col.substring(dot + 1) : col;
    }
}
