package nexusdb.sql;

import nexusdb.transaction.TransactionManager;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD tests for QueryExecutor — end-to-end SQL execution against NexusDB storage.
 *
 * Tests cover:
 * - CREATE TABLE + INSERT + SELECT round-trip
 * - SELECT with WHERE filters
 * - UPDATE modifies existing rows
 * - DELETE removes rows
 * - INSERT into nonexistent table throws
 * - SELECT with multiple rows
 * - EXPLAIN returns plan string
 * - CREATE INDEX registers index
 * - ORDER BY sorts results
 * - Column type enforcement
 * - NOT NULL constraint enforcement
 * - Multiple tables are independent
 */
class QueryExecutorTest {

    private QueryExecutor executor;

    @BeforeEach
    void setUp() {
        TransactionManager txnManager = new TransactionManager();
        executor = new QueryExecutor(txnManager);
    }

    // ========== CREATE TABLE + INSERT + SELECT ==========

    @Test
    @DisplayName("CREATE TABLE + INSERT + SELECT round-trip")
    void createInsertSelectRoundTrip() {
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL)");
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

        var result = executor.execute("SELECT * FROM users");

        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().getFirst().get("id")).isEqualTo("1");
        assertThat(result.rows().getFirst().get("name")).isEqualTo("Alice");
    }

    @Test
    @DisplayName("SELECT returns multiple rows")
    void selectMultipleRows() {
        executor.execute("CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR)");
        executor.execute("INSERT INTO items (id, label) VALUES (1, 'A')");
        executor.execute("INSERT INTO items (id, label) VALUES (2, 'B')");
        executor.execute("INSERT INTO items (id, label) VALUES (3, 'C')");

        var result = executor.execute("SELECT * FROM items");

        assertThat(result.rows()).hasSize(3);
    }

    @Test
    @DisplayName("SELECT specific columns projects correctly")
    void selectSpecificColumns() {
        executor.execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR, age INT)");
        executor.execute("INSERT INTO people (id, name, age) VALUES (1, 'Bob', 30)");

        var result = executor.execute("SELECT name FROM people");

        assertThat(result.columns()).containsExactly("name");
        assertThat(result.rows().getFirst()).containsKey("name");
        assertThat(result.rows().getFirst()).doesNotContainKey("id");
    }

    // ========== WHERE Filtering ==========

    @Test
    @DisplayName("SELECT with WHERE filters correctly")
    void selectWithWhere() {
        executor.execute("CREATE TABLE scores (id INT PRIMARY KEY, score INT)");
        executor.execute("INSERT INTO scores (id, score) VALUES (1, 85)");
        executor.execute("INSERT INTO scores (id, score) VALUES (2, 92)");
        executor.execute("INSERT INTO scores (id, score) VALUES (3, 78)");

        var result = executor.execute("SELECT * FROM scores WHERE score > 80");

        assertThat(result.rows()).hasSize(2);
    }

    @Test
    @DisplayName("SELECT with WHERE equality on primary key")
    void selectWhereEqualityPK() {
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        executor.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");

        var result = executor.execute("SELECT * FROM users WHERE id = 1");

        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().getFirst().get("name")).isEqualTo("Alice");
    }

    @Test
    @DisplayName("SELECT with AND condition")
    void selectWithAndCondition() {
        executor.execute("CREATE TABLE employees (id INT PRIMARY KEY, dept VARCHAR, salary INT)");
        executor.execute("INSERT INTO employees (id, dept, salary) VALUES (1, 'eng', 100)");
        executor.execute("INSERT INTO employees (id, dept, salary) VALUES (2, 'eng', 200)");
        executor.execute("INSERT INTO employees (id, dept, salary) VALUES (3, 'sales', 150)");

        var result = executor.execute("SELECT * FROM employees WHERE dept = 'eng' AND salary > 150");

        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().getFirst().get("id")).isEqualTo("2");
    }

    // ========== UPDATE ==========

    @Test
    @DisplayName("UPDATE modifies existing row")
    void updateModifiesRow() {
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

        executor.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");

        var result = executor.execute("SELECT * FROM users WHERE id = 1");
        assertThat(result.rows().getFirst().get("name")).isEqualTo("Alicia");
    }

    // ========== DELETE ==========

    @Test
    @DisplayName("DELETE removes row")
    void deleteRemovesRow() {
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)");
        executor.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        executor.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");

        executor.execute("DELETE FROM users WHERE id = 1");

        var result = executor.execute("SELECT * FROM users");
        assertThat(result.rows()).hasSize(1);
        assertThat(result.rows().getFirst().get("name")).isEqualTo("Bob");
    }

    // ========== Error Cases ==========

    @Test
    @DisplayName("INSERT into nonexistent table throws")
    void insertNonexistentTableThrows() {
        assertThatThrownBy(() -> executor.execute("INSERT INTO ghost (id) VALUES (1)"))
                .isInstanceOf(SQLExecutionException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    @DisplayName("SELECT from nonexistent table throws")
    void selectNonexistentTableThrows() {
        assertThatThrownBy(() -> executor.execute("SELECT * FROM ghost"))
                .isInstanceOf(SQLExecutionException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    @DisplayName("Duplicate CREATE TABLE throws")
    void duplicateCreateTableThrows() {
        executor.execute("CREATE TABLE t (id INT PRIMARY KEY)");

        assertThatThrownBy(() -> executor.execute("CREATE TABLE t (id INT PRIMARY KEY)"))
                .isInstanceOf(SQLExecutionException.class)
                .hasMessageContaining("already exists");
    }

    // ========== Type Enforcement ==========

    @Test
    @DisplayName("INT column rejects non-numeric value")
    void intColumnRejectsNonNumeric() {
        executor.execute("CREATE TABLE typed (id INT PRIMARY KEY, count INT)");

        assertThatThrownBy(() ->
                executor.execute("INSERT INTO typed (id, count) VALUES (1, 'abc')"))
                .isInstanceOf(SQLExecutionException.class)
                .hasMessageContaining("expects INT");
    }

    @Test
    @DisplayName("NOT NULL column rejects null")
    void notNullRejectsNull() {
        executor.execute("CREATE TABLE strict (id INT PRIMARY KEY, name VARCHAR NOT NULL)");

        assertThatThrownBy(() ->
                executor.execute("INSERT INTO strict (id, name) VALUES (1, null)"))
                .isInstanceOf(SQLExecutionException.class)
                .hasMessageContaining("cannot be NULL");
    }

    // ========== ORDER BY ==========

    @Test
    @DisplayName("ORDER BY sorts results ascending")
    void orderByAscending() {
        executor.execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)");
        executor.execute("INSERT INTO nums (id, val) VALUES (1, 30)");
        executor.execute("INSERT INTO nums (id, val) VALUES (2, 10)");
        executor.execute("INSERT INTO nums (id, val) VALUES (3, 20)");

        var result = executor.execute("SELECT * FROM nums ORDER BY val ASC");

        assertThat(result.rows().get(0).get("val")).isEqualTo("10");
        assertThat(result.rows().get(1).get("val")).isEqualTo("20");
        assertThat(result.rows().get(2).get("val")).isEqualTo("30");
    }

    @Test
    @DisplayName("ORDER BY sorts results descending")
    void orderByDescending() {
        executor.execute("CREATE TABLE nums (id INT PRIMARY KEY, val INT)");
        executor.execute("INSERT INTO nums (id, val) VALUES (1, 30)");
        executor.execute("INSERT INTO nums (id, val) VALUES (2, 10)");
        executor.execute("INSERT INTO nums (id, val) VALUES (3, 20)");

        var result = executor.execute("SELECT * FROM nums ORDER BY val DESC");

        assertThat(result.rows().get(0).get("val")).isEqualTo("30");
        assertThat(result.rows().get(1).get("val")).isEqualTo("20");
        assertThat(result.rows().get(2).get("val")).isEqualTo("10");
    }

    // ========== EXPLAIN ==========

    @Test
    @DisplayName("EXPLAIN returns plan string")
    void explainReturnsPlan() {
        executor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR)");

        var result = executor.execute("EXPLAIN SELECT * FROM users WHERE id = 1");

        assertThat(result.columns()).containsExactly("plan");
        assertThat(result.rows().getFirst().get("plan")).isNotEmpty();
    }

    // ========== CREATE INDEX ==========

    @Test
    @DisplayName("CREATE INDEX registers index and EXPLAIN uses it")
    void createIndexUsedByExplain() {
        executor.execute("CREATE TABLE products (id INT PRIMARY KEY, category VARCHAR, price INT)");
        executor.execute("CREATE INDEX idx_category ON products (category)");

        var result = executor.execute("EXPLAIN SELECT * FROM products WHERE category = 'books'");

        String plan = result.rows().getFirst().get("plan");
        assertThat(plan).containsIgnoringCase("index");
    }

    // ========== Multiple Tables ==========

    @Test
    @DisplayName("Multiple tables are independent")
    void multipleTablesIndependent() {
        executor.execute("CREATE TABLE a (id INT PRIMARY KEY, val VARCHAR)");
        executor.execute("CREATE TABLE b (id INT PRIMARY KEY, val VARCHAR)");
        executor.execute("INSERT INTO a (id, val) VALUES (1, 'fromA')");
        executor.execute("INSERT INTO b (id, val) VALUES (1, 'fromB')");

        var resultA = executor.execute("SELECT * FROM a");
        var resultB = executor.execute("SELECT * FROM b");

        assertThat(resultA.rows().getFirst().get("val")).isEqualTo("fromA");
        assertThat(resultB.rows().getFirst().get("val")).isEqualTo("fromB");
    }

    // ========== Message Results ==========

    @Test
    @DisplayName("CREATE TABLE returns success message")
    void createTableReturnsMessage() {
        var result = executor.execute("CREATE TABLE t (id INT PRIMARY KEY)");

        assertThat(result.rows().getFirst().get("result")).contains("Table created");
    }

    @Test
    @DisplayName("INSERT returns OK message")
    void insertReturnsOk() {
        executor.execute("CREATE TABLE t (id INT PRIMARY KEY)");
        var result = executor.execute("INSERT INTO t (id) VALUES (1)");

        assertThat(result.rows().getFirst().get("result")).isEqualTo("OK");
    }

    @Test
    @DisplayName("UPDATE returns count message")
    void updateReturnsCount() {
        executor.execute("CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR)");
        executor.execute("INSERT INTO t (id, v) VALUES (1, 'a')");
        executor.execute("INSERT INTO t (id, v) VALUES (2, 'b')");

        var result = executor.execute("UPDATE t SET v = 'x' WHERE id = 1");

        assertThat(result.rows().getFirst().get("result")).contains("Updated 1");
    }

    @Test
    @DisplayName("DELETE returns count message")
    void deleteReturnsCount() {
        executor.execute("CREATE TABLE t (id INT PRIMARY KEY)");
        executor.execute("INSERT INTO t (id) VALUES (1)");

        var result = executor.execute("DELETE FROM t WHERE id = 1");

        assertThat(result.rows().getFirst().get("result")).contains("Deleted 1");
    }
}
