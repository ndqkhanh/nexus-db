package nexusdb.sql;

import nexusdb.sql.SQLParser.*;

import java.util.*;

/**
 * Query plan representation as a tree of {@link PlanNode} operators,
 * plus a {@link QueryPlanner} that converts a {@link SQLStatement} into a plan.
 */
public final class ExecutionPlan {

    // =========================================================
    // PlanNode sealed hierarchy
    // =========================================================

    /** A node in the physical query plan tree. */
    public sealed interface PlanNode
            permits PlanNode.TableScan,
                    PlanNode.IndexSeek,
                    PlanNode.IndexScan,
                    PlanNode.Filter,
                    PlanNode.Project,
                    PlanNode.Sort,
                    PlanNode.Limit {

        /** Full sequential scan of all rows in a table. */
        record TableScan(String table) implements PlanNode {}

        /** Direct index lookup (equality). */
        record IndexSeek(String table, String indexName, String column,
                         Operator op, String value) implements PlanNode {}

        /** Range scan over an index. */
        record IndexScan(String table, String indexName,
                         String startKey, String endKey) implements PlanNode {}

        /** Apply a predicate to the child's output rows. */
        record Filter(PlanNode child, WhereClause predicate) implements PlanNode {}

        /** Select a subset of columns from the child's output. */
        record Project(PlanNode child, List<String> columns) implements PlanNode {}

        /** Sort the child's output by a column. */
        record Sort(PlanNode child, String column, boolean ascending) implements PlanNode {}

        /** Return at most {@code count} rows from the child. */
        record Limit(PlanNode child, int count) implements PlanNode {}
    }

    // =========================================================
    // QueryPlanner
    // =========================================================

    /**
     * Converts a {@link SQLStatement} into a {@link PlanNode} tree.
     *
     * <p>Planning rules (applied in order):
     * <ol>
     *   <li>If a WHERE equality predicate targets an indexed column → {@link PlanNode.IndexSeek}</li>
     *   <li>If a WHERE range predicate (GT/LT/GE/LE) targets an indexed column → {@link PlanNode.IndexScan}</li>
     *   <li>Otherwise → {@link PlanNode.TableScan} + {@link PlanNode.Filter}</li>
     *   <li>Wrap with {@link PlanNode.Project} when specific columns requested.</li>
     *   <li>Wrap with {@link PlanNode.Sort} when ORDER BY present.</li>
     * </ol>
     */
    public static final class QueryPlanner {

        /** Catalog used to check which columns have indexes. */
        private final Catalog catalog;

        public QueryPlanner(Catalog catalog) {
            this.catalog = catalog;
        }

        /**
         * Build a plan for the given statement.
         *
         * @return {@code null} for statements that are handled as direct writes
         *         (INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX).
         */
        public PlanNode plan(SQLStatement stmt) {
            return switch (stmt) {
                case SQLStatement.SelectStatement s  -> planSelect(s);
                case SQLStatement.ExplainStatement e -> plan(e.inner()); // delegate to inner
                default -> null; // direct-write statements have no read plan
            };
        }

        private PlanNode planSelect(SQLStatement.SelectStatement select) {
            String table = select.table();
            WhereClause where = select.where();

            // --- Build scan/seek node ---
            PlanNode base = buildScanNode(table, where);

            // If we used an index for part of the WHERE but there are residual predicates,
            // wrap with a Filter for the remainder.  For simplicity we push the full
            // predicate down to Filter when needed; the executor will short-circuit.
            if (where != null && !(base instanceof PlanNode.IndexSeek)
                    && !(base instanceof PlanNode.IndexScan)) {
                base = new PlanNode.Filter(base, where);
            } else if (where != null && needsResidualFilter(base, where)) {
                base = new PlanNode.Filter(base, where);
            }

            // --- Project ---
            if (!select.columns().isEmpty()) {
                base = new PlanNode.Project(base, select.columns());
            }

            // --- Sort ---
            if (select.orderBy() != null) {
                base = new PlanNode.Sort(base, select.orderBy().column(),
                        select.orderBy().ascending());
            }

            return base;
        }

        /**
         * Build the lowest-level scan node, choosing IndexSeek / IndexScan over TableScan
         * when the WHERE clause has a simple comparison on an indexed column.
         */
        private PlanNode buildScanNode(String table, WhereClause where) {
            if (where instanceof WhereClause.Comparison cmp) {
                String col = stripTablePrefix(cmp.column());
                String idxName = catalog.findIndex(table, col);
                if (idxName != null) {
                    return switch (cmp.operator()) {
                        case EQ -> new PlanNode.IndexSeek(table, idxName, col,
                                Operator.EQ, cmp.value());
                        case LT, GT, LE, GE ->
                                new PlanNode.IndexScan(table, idxName,
                                        cmp.operator() == Operator.GT || cmp.operator() == Operator.GE
                                                ? cmp.value() : null,
                                        cmp.operator() == Operator.LT || cmp.operator() == Operator.LE
                                                ? cmp.value() : null);
                        case NE -> new PlanNode.TableScan(table); // NE can't use index efficiently
                    };
                }
            }
            // AND: check if either side is an indexed equality
            if (where instanceof WhereClause.And and) {
                PlanNode leftPlan  = buildScanNode(table, and.left());
                if (!(leftPlan instanceof PlanNode.TableScan)) return leftPlan;
                PlanNode rightPlan = buildScanNode(table, and.right());
                if (!(rightPlan instanceof PlanNode.TableScan)) return rightPlan;
            }
            return new PlanNode.TableScan(table);
        }

        /** Returns true when an index node was chosen but the full WHERE has more conditions. */
        private boolean needsResidualFilter(PlanNode base, WhereClause where) {
            if (!(base instanceof PlanNode.IndexSeek) && !(base instanceof PlanNode.IndexScan))
                return false;
            // The WHERE clause has more than a single comparison → residual filter needed
            return (where instanceof WhereClause.And) || (where instanceof WhereClause.Or);
        }

        /** Strip "table." prefix from a column reference like "users.id" → "id". */
        private static String stripTablePrefix(String col) {
            int dot = col.indexOf('.');
            return dot >= 0 ? col.substring(dot + 1) : col;
        }
    }

    // =========================================================
    // explain() — human-readable plan tree
    // =========================================================

    /**
     * Render a plan node tree as an indented string.
     *
     * Example:
     * <pre>
     * Sort[name ASC]
     *   Project[id, name]
     *     Filter[age > 25]
     *       TableScan[users]
     * </pre>
     */
    public static String explain(PlanNode node) {
        StringBuilder sb = new StringBuilder();
        explainNode(node, 0, sb);
        return sb.toString().stripTrailing();
    }

    private static void explainNode(PlanNode node, int indent, StringBuilder sb) {
        sb.append("  ".repeat(indent));
        switch (node) {
            case PlanNode.TableScan ts ->
                sb.append("TableScan[").append(ts.table()).append("]");
            case PlanNode.IndexSeek ise ->
                sb.append("IndexSeek[").append(ise.table())
                  .append(" via ").append(ise.indexName())
                  .append(" ").append(ise.column())
                  .append(" ").append(ise.op())
                  .append(" ").append(ise.value()).append("]");
            case PlanNode.IndexScan isc ->
                sb.append("IndexScan[").append(isc.table())
                  .append(" via ").append(isc.indexName())
                  .append(" start=").append(isc.startKey())
                  .append(" end=").append(isc.endKey()).append("]");
            case PlanNode.Filter f -> {
                sb.append("Filter[").append(describeWhere(f.predicate())).append("]");
                sb.append("\n");
                explainNode(f.child(), indent + 1, sb);
                return;
            }
            case PlanNode.Project p -> {
                sb.append("Project[").append(String.join(", ", p.columns())).append("]");
                sb.append("\n");
                explainNode(p.child(), indent + 1, sb);
                return;
            }
            case PlanNode.Sort s -> {
                sb.append("Sort[").append(s.column())
                  .append(s.ascending() ? " ASC" : " DESC").append("]");
                sb.append("\n");
                explainNode(s.child(), indent + 1, sb);
                return;
            }
            case PlanNode.Limit l -> {
                sb.append("Limit[").append(l.count()).append("]");
                sb.append("\n");
                explainNode(l.child(), indent + 1, sb);
                return;
            }
        }
        sb.append("\n");
    }

    private static String describeWhere(WhereClause w) {
        return switch (w) {
            case WhereClause.Comparison c ->
                c.column() + " " + c.operator() + " " + c.value();
            case WhereClause.And a ->
                "(" + describeWhere(a.left()) + " AND " + describeWhere(a.right()) + ")";
            case WhereClause.Or o ->
                "(" + describeWhere(o.left()) + " OR " + describeWhere(o.right()) + ")";
        };
    }
}
