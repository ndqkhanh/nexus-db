package nexusdb.sql;

import nexusdb.sql.SQLParser.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SQLParserTest {

    private final SQLParser parser = new SQLParser();

    // =========================================================
    // SELECT
    // =========================================================

    @Test
    void selectStar() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("SELECT * FROM users");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).isEmpty();
        assertThat(stmt.where()).isNull();
        assertThat(stmt.orderBy()).isNull();
    }

    @Test
    void selectSpecificColumns() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("SELECT id, name FROM users");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).containsExactly("id", "name");
    }

    @Test
    void selectWhereEquality() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("SELECT id, name FROM users WHERE id = 1");
        assertThat(stmt.where()).isInstanceOf(WhereClause.Comparison.class);
        var cmp = (WhereClause.Comparison) stmt.where();
        assertThat(cmp.column()).isEqualTo("id");
        assertThat(cmp.operator()).isEqualTo(Operator.EQ);
        assertThat(cmp.value()).isEqualTo("1");
    }

    @Test
    void selectWhereGreaterThanAndEquality() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT name FROM users WHERE age > 25 AND active = true");
        assertThat(stmt.where()).isInstanceOf(WhereClause.And.class);
        var and = (WhereClause.And) stmt.where();
        var left  = (WhereClause.Comparison) and.left();
        var right = (WhereClause.Comparison) and.right();
        assertThat(left.operator()).isEqualTo(Operator.GT);
        assertThat(left.value()).isEqualTo("25");
        assertThat(right.operator()).isEqualTo(Operator.EQ);
        assertThat(right.value()).isEqualTo("true");
    }

    @Test
    void selectOrderByAsc() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM products ORDER BY price ASC");
        assertThat(stmt.orderBy()).isNotNull();
        assertThat(stmt.orderBy().column()).isEqualTo("price");
        assertThat(stmt.orderBy().ascending()).isTrue();
    }

    @Test
    void selectOrderByDesc() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM products ORDER BY price DESC");
        assertThat(stmt.orderBy().ascending()).isFalse();
    }

    @Test
    void selectOrderByDefaultAsc() {
        // No ASC/DESC keyword → defaults to ascending
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM products ORDER BY name");
        assertThat(stmt.orderBy().ascending()).isTrue();
    }

    @Test
    void selectWhereOrCondition() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM users WHERE id = 1 OR id = 2");
        assertThat(stmt.where()).isInstanceOf(WhereClause.Or.class);
    }

    @Test
    void selectWhereMultipleAndOr() {
        // a AND b OR c  →  (a AND b) OR c  (AND binds tighter)
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3");
        assertThat(stmt.where()).isInstanceOf(WhereClause.Or.class);
        var or = (WhereClause.Or) stmt.where();
        assertThat(or.left()).isInstanceOf(WhereClause.And.class);
    }

    @Test
    void selectColumnWithTablePrefix() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT users.name FROM users");
        assertThat(stmt.columns()).containsExactly("users.name");
    }

    @Test
    void selectCaseInsensitiveKeywords() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("select * from users");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).isEmpty();
    }

    @Test
    void selectMixedCaseKeywords() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("Select Id, Name From Users Where Id = 1");
        assertThat(stmt.table()).isEqualTo("Users");
        assertThat(stmt.columns()).containsExactly("Id", "Name");
    }

    @Test
    void selectWithTrailingSemicolon() {
        var stmt = (SQLStatement.SelectStatement) parser.parse("SELECT * FROM users;");
        assertThat(stmt.table()).isEqualTo("users");
    }

    @Test
    void selectWhereStringLiteral() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM users WHERE name = 'Alice'");
        var cmp = (WhereClause.Comparison) stmt.where();
        assertThat(cmp.value()).isEqualTo("Alice");
    }

    @Test
    void selectWhereLeAndGe() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM t WHERE age <= 30 AND score >= 90");
        var and = (WhereClause.And) stmt.where();
        assertThat(((WhereClause.Comparison) and.left()).operator()).isEqualTo(Operator.LE);
        assertThat(((WhereClause.Comparison) and.right()).operator()).isEqualTo(Operator.GE);
    }

    @Test
    void selectWhereNotEqual() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM t WHERE status != 'inactive'");
        var cmp = (WhereClause.Comparison) stmt.where();
        assertThat(cmp.operator()).isEqualTo(Operator.NE);
    }

    @Test
    void selectWhereNotEqualAngleBrackets() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "SELECT * FROM t WHERE status <> 'inactive'");
        var cmp = (WhereClause.Comparison) stmt.where();
        assertThat(cmp.operator()).isEqualTo(Operator.NE);
    }

    // =========================================================
    // INSERT
    // =========================================================

    @Test
    void insertSingleRow() {
        var stmt = (SQLStatement.InsertStatement) parser.parse(
                "INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).containsExactly("id", "name");
        assertThat(stmt.rows()).hasSize(1);
        assertThat(stmt.rows().get(0)).containsExactly("1", "Alice");
    }

    @Test
    void insertMultipleRows() {
        var stmt = (SQLStatement.InsertStatement) parser.parse(
                "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
        assertThat(stmt.rows()).hasSize(2);
        assertThat(stmt.rows().get(1)).containsExactly("2", "Bob");
    }

    // =========================================================
    // UPDATE
    // =========================================================

    @Test
    void updateSingleColumn() {
        var stmt = (SQLStatement.UpdateStatement) parser.parse(
                "UPDATE users SET name = 'Bob' WHERE id = 1");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.setClause()).containsEntry("name", "Bob");
        assertThat(stmt.where()).isNotNull();
    }

    @Test
    void updateMultipleColumns() {
        var stmt = (SQLStatement.UpdateStatement) parser.parse(
                "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1");
        assertThat(stmt.setClause()).hasSize(2);
        assertThat(stmt.setClause()).containsEntry("age", "30");
    }

    // =========================================================
    // DELETE
    // =========================================================

    @Test
    void deleteWithWhere() {
        var stmt = (SQLStatement.DeleteStatement) parser.parse(
                "DELETE FROM users WHERE id = 1");
        assertThat(stmt.table()).isEqualTo("users");
        var cmp = (WhereClause.Comparison) stmt.where();
        assertThat(cmp.value()).isEqualTo("1");
    }

    @Test
    void deleteWithoutWhere() {
        var stmt = (SQLStatement.DeleteStatement) parser.parse("DELETE FROM users");
        assertThat(stmt.where()).isNull();
    }

    // =========================================================
    // CREATE TABLE
    // =========================================================

    @Test
    void createTable() {
        var stmt = (SQLStatement.CreateTableStatement) parser.parse(
                "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR NOT NULL, age INT)");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).hasSize(3);

        ColumnDef id = stmt.columns().get(0);
        assertThat(id.name()).isEqualTo("id");
        assertThat(id.type()).isEqualTo(ColumnType.INT);
        assertThat(id.isPrimaryKey()).isTrue();
        assertThat(id.isNotNull()).isTrue();

        ColumnDef name = stmt.columns().get(1);
        assertThat(name.type()).isEqualTo(ColumnType.VARCHAR);
        assertThat(name.isNotNull()).isTrue();
        assertThat(name.isPrimaryKey()).isFalse();

        ColumnDef age = stmt.columns().get(2);
        assertThat(age.type()).isEqualTo(ColumnType.INT);
        assertThat(age.isPrimaryKey()).isFalse();
        assertThat(age.isNotNull()).isFalse();
    }

    @Test
    void createTableWithBoolean() {
        var stmt = (SQLStatement.CreateTableStatement) parser.parse(
                "CREATE TABLE flags (id INT PRIMARY KEY, active BOOLEAN)");
        assertThat(stmt.columns().get(1).type()).isEqualTo(ColumnType.BOOLEAN);
    }

    // =========================================================
    // CREATE INDEX
    // =========================================================

    @Test
    void createIndexSingleColumn() {
        var stmt = (SQLStatement.CreateIndexStatement) parser.parse(
                "CREATE INDEX idx_name ON users (name)");
        assertThat(stmt.indexName()).isEqualTo("idx_name");
        assertThat(stmt.table()).isEqualTo("users");
        assertThat(stmt.columns()).containsExactly("name");
    }

    @Test
    void createIndexMultipleColumns() {
        var stmt = (SQLStatement.CreateIndexStatement) parser.parse(
                "CREATE INDEX idx_multi ON orders (customer_id, product_id)");
        assertThat(stmt.columns()).containsExactly("customer_id", "product_id");
    }

    // =========================================================
    // EXPLAIN
    // =========================================================

    @Test
    void explainSelect() {
        var stmt = (SQLStatement.ExplainStatement) parser.parse(
                "EXPLAIN SELECT * FROM users WHERE id = 1");
        assertThat(stmt.inner()).isInstanceOf(SQLStatement.SelectStatement.class);
    }

    // =========================================================
    // Parse errors
    // =========================================================

    @Test
    void missingFromClause() {
        assertThatThrownBy(() -> parser.parse("SELECT id, name users"))
                .isInstanceOf(SQLParseException.class)
                .hasMessageContaining("FROM");
    }

    @Test
    void unterminatedStringLiteral() {
        assertThatThrownBy(() -> parser.parse("SELECT * FROM users WHERE name = 'Alice"))
                .isInstanceOf(SQLParseException.class)
                .hasMessageContaining("Unterminated string literal");
    }

    @Test
    void unknownStatementType() {
        assertThatThrownBy(() -> parser.parse("DROP TABLE users"))
                .isInstanceOf(SQLParseException.class);
    }

    @Test
    void invalidColumnType() {
        assertThatThrownBy(() -> parser.parse("CREATE TABLE t (id FLOAT)"))
                .isInstanceOf(SQLParseException.class)
                .hasMessageContaining("column type");
    }

    // =========================================================
    // Whitespace / formatting variations
    // =========================================================

    @Test
    void extraWhitespace() {
        var stmt = (SQLStatement.SelectStatement) parser.parse(
                "  SELECT   *   FROM   users  ");
        assertThat(stmt.table()).isEqualTo("users");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * FROM users",
        "select * from users",
        "Select * From Users",
        "SELECT * FROM USERS"
    })
    void caseInsensitiveVariants(String sql) {
        assertThatNoException().isThrownBy(() -> parser.parse(sql));
    }
}
