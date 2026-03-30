package nexusdb.sql;

import nexusdb.sql.ExecutionPlan.*;
import nexusdb.sql.SQLParser.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ExecutionPlanTest {

    private Catalog catalog;
    private QueryPlanner planner;
    private final SQLParser parser = new SQLParser();

    @BeforeEach
    void setUp() {
        catalog = new Catalog();
        // Register a 'users' table with id (PK), name, age columns
        catalog.createTable("users", java.util.List.of(
                new ColumnDef("id",   ColumnType.INT,     true,  true),
                new ColumnDef("name", ColumnType.VARCHAR, false, true),
                new ColumnDef("age",  ColumnType.INT,     false, false)
        ));
        catalog.registerPrimaryKey("users", "id");

        // Register an index on users.name
        catalog.createIndex((SQLStatement.CreateIndexStatement)
                parser.parse("CREATE INDEX idx_name ON users (name)"));

        // Register a 'products' table
        catalog.createTable("products", java.util.List.of(
                new ColumnDef("id",    ColumnType.INT,     true,  true),
                new ColumnDef("price", ColumnType.INT,     false, false),
                new ColumnDef("label", ColumnType.VARCHAR, false, false)
        ));
        catalog.registerPrimaryKey("products", "id");

        planner = new QueryPlanner(catalog);
    }

    // =========================================================
    // TableScan
    // =========================================================

    @Test
    void selectStarProducesTableScan() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users"));
        assertThat(plan).isInstanceOf(PlanNode.TableScan.class);
        assertThat(((PlanNode.TableScan) plan).table()).isEqualTo("users");
    }

    @Test
    void selectNonIndexedWhereProducesTableScanFilter() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE age > 25"));
        assertThat(plan).isInstanceOf(PlanNode.Filter.class);
        PlanNode.Filter filter = (PlanNode.Filter) plan;
        assertThat(filter.child()).isInstanceOf(PlanNode.TableScan.class);
    }

    // =========================================================
    // IndexSeek
    // =========================================================

    @Test
    void selectWherePrimaryKeyEqualityProducesIndexSeek() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE id = 1"));
        assertThat(plan).isInstanceOf(PlanNode.IndexSeek.class);
        PlanNode.IndexSeek seek = (PlanNode.IndexSeek) plan;
        assertThat(seek.table()).isEqualTo("users");
        assertThat(seek.column()).isEqualTo("id");
        assertThat(seek.op()).isEqualTo(Operator.EQ);
        assertThat(seek.value()).isEqualTo("1");
    }

    @Test
    void selectWhereIndexedColumnEqualityProducesIndexSeek() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE name = 'Alice'"));
        assertThat(plan).isInstanceOf(PlanNode.IndexSeek.class);
        PlanNode.IndexSeek seek = (PlanNode.IndexSeek) plan;
        assertThat(seek.indexName()).isEqualTo("idx_name");
    }

    // =========================================================
    // IndexScan (range)
    // =========================================================

    @Test
    void selectWhereIndexedColumnRangeProducesIndexScan() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE id > 5"));
        assertThat(plan).isInstanceOf(PlanNode.IndexScan.class);
        PlanNode.IndexScan scan = (PlanNode.IndexScan) plan;
        assertThat(scan.table()).isEqualTo("users");
        assertThat(scan.startKey()).isEqualTo("5");
        assertThat(scan.endKey()).isNull();
    }

    @Test
    void selectWhereIndexedColumnLtProducesIndexScan() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE id < 100"));
        assertThat(plan).isInstanceOf(PlanNode.IndexScan.class);
        PlanNode.IndexScan scan = (PlanNode.IndexScan) plan;
        assertThat(scan.startKey()).isNull();
        assertThat(scan.endKey()).isEqualTo("100");
    }

    // =========================================================
    // Project
    // =========================================================

    @Test
    void selectSpecificColumnsWrapsWithProject() {
        PlanNode plan = planner.plan(parser.parse("SELECT id, name FROM users"));
        assertThat(plan).isInstanceOf(PlanNode.Project.class);
        PlanNode.Project proj = (PlanNode.Project) plan;
        assertThat(proj.columns()).containsExactly("id", "name");
        assertThat(proj.child()).isInstanceOf(PlanNode.TableScan.class);
    }

    @Test
    void selectColumnsWithIndexSeekWrapsProject() {
        PlanNode plan = planner.plan(parser.parse("SELECT id, name FROM users WHERE id = 1"));
        assertThat(plan).isInstanceOf(PlanNode.Project.class);
        assertThat(((PlanNode.Project) plan).child()).isInstanceOf(PlanNode.IndexSeek.class);
    }

    // =========================================================
    // Sort
    // =========================================================

    @Test
    void selectOrderByWrapsWithSort() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users ORDER BY name ASC"));
        assertThat(plan).isInstanceOf(PlanNode.Sort.class);
        PlanNode.Sort sort = (PlanNode.Sort) plan;
        assertThat(sort.column()).isEqualTo("name");
        assertThat(sort.ascending()).isTrue();
        assertThat(sort.child()).isInstanceOf(PlanNode.TableScan.class);
    }

    @Test
    void selectOrderByDescWrapsWithSort() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users ORDER BY age DESC"));
        PlanNode.Sort sort = (PlanNode.Sort) plan;
        assertThat(sort.ascending()).isFalse();
    }

    // =========================================================
    // Combined plans
    // =========================================================

    @Test
    void selectColumnsWithWhereAndOrderBy() {
        // Project → Sort → Filter → TableScan
        PlanNode plan = planner.plan(parser.parse(
                "SELECT id, name FROM users WHERE age > 25 ORDER BY name ASC"));
        assertThat(plan).isInstanceOf(PlanNode.Sort.class);
        PlanNode.Sort sort = (PlanNode.Sort) plan;
        assertThat(sort.child()).isInstanceOf(PlanNode.Project.class);
        PlanNode.Project proj = (PlanNode.Project) sort.child();
        assertThat(proj.child()).isInstanceOf(PlanNode.Filter.class);
    }

    @Test
    void selectWithAndWherePicksIndexFromOneSide() {
        // id = 1 AND age > 5: id is indexed, so we use IndexSeek + residual Filter
        PlanNode plan = planner.plan(parser.parse(
                "SELECT * FROM users WHERE id = 1 AND age > 5"));
        // The base should be IndexSeek (picked from the AND left), then wrapped with Filter
        assertThat(plan).isInstanceOf(PlanNode.Filter.class);
        PlanNode inner = ((PlanNode.Filter) plan).child();
        assertThat(inner).isInstanceOf(PlanNode.IndexSeek.class);
    }

    // =========================================================
    // No plan for write statements
    // =========================================================

    @Test
    void insertProducesNoPlan() {
        PlanNode plan = planner.plan(
                parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')"));
        assertThat(plan).isNull();
    }

    @Test
    void updateProducesNoPlan() {
        PlanNode plan = planner.plan(
                parser.parse("UPDATE users SET name = 'Bob' WHERE id = 1"));
        assertThat(plan).isNull();
    }

    @Test
    void deleteProducesNoPlan() {
        PlanNode plan = planner.plan(
                parser.parse("DELETE FROM users WHERE id = 1"));
        assertThat(plan).isNull();
    }

    // =========================================================
    // EXPLAIN output
    // =========================================================

    @Test
    void explainTableScan() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users"));
        String explained = ExecutionPlan.explain(plan);
        assertThat(explained).contains("TableScan[users]");
    }

    @Test
    void explainIndexSeek() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users WHERE id = 1"));
        String explained = ExecutionPlan.explain(plan);
        assertThat(explained).contains("IndexSeek[users");
        assertThat(explained).contains("id");
        assertThat(explained).contains("1");
    }

    @Test
    void explainProjectFilterScan() {
        PlanNode plan = planner.plan(parser.parse("SELECT id, name FROM users WHERE age > 25"));
        String explained = ExecutionPlan.explain(plan);
        assertThat(explained).contains("Project[id, name]");
        assertThat(explained).contains("Filter[");
        assertThat(explained).contains("TableScan[users]");
    }

    @Test
    void explainSortScanIndented() {
        PlanNode plan = planner.plan(parser.parse("SELECT * FROM users ORDER BY name DESC"));
        String explained = ExecutionPlan.explain(plan);
        // Sort should appear before TableScan in the string
        assertThat(explained.indexOf("Sort[")).isLessThan(explained.indexOf("TableScan["));
    }

    @Test
    void explainStatementDelegatesInner() {
        // EXPLAIN SELECT * → same plan as SELECT *
        SQLStatement explainStmt = parser.parse("EXPLAIN SELECT * FROM users");
        PlanNode plan = planner.plan(explainStmt);
        assertThat(plan).isInstanceOf(PlanNode.TableScan.class);
    }
}
