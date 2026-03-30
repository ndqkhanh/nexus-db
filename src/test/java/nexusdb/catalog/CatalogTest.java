package nexusdb.catalog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class CatalogTest {

    private Catalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new Catalog();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private TableSchema usersTable() {
        return new TableSchema(
                "users",
                List.of(
                        new ColumnSchema("id",    DataType.INT,     true,  true),
                        new ColumnSchema("email", DataType.VARCHAR, false, true),
                        new ColumnSchema("age",   DataType.INT,     false, false)
                ),
                "id"
        );
    }

    private TableSchema ordersTable() {
        return new TableSchema(
                "orders",
                List.of(
                        new ColumnSchema("order_id", DataType.INT,     true,  true),
                        new ColumnSchema("user_id",  DataType.INT,     false, true),
                        new ColumnSchema("total",    DataType.VARCHAR, false, false)
                ),
                "order_id"
        );
    }

    private IndexSchema emailIndex() {
        return new IndexSchema("idx_users_email", "users", List.of("email"), true);
    }

    private IndexSchema ageIndex() {
        return new IndexSchema("idx_users_age", "users", List.of("age"), false);
    }

    // ── Table tests ───────────────────────────────────────────────────────────

    @Test
    void createTable_registersSchema() {
        catalog.createTable(usersTable());
        assertThat(catalog.tableExists("users")).isTrue();
    }

    @Test
    void getTable_returnsRegisteredSchema() {
        TableSchema schema = usersTable();
        catalog.createTable(schema);
        assertThat(catalog.getTable("users")).hasValue(schema);
    }

    @Test
    void tableExists_returnsFalseForUnknownTable() {
        assertThat(catalog.tableExists("nonexistent")).isFalse();
    }

    @Test
    void tableExists_returnsTrueAfterCreate() {
        catalog.createTable(usersTable());
        assertThat(catalog.tableExists("users")).isTrue();
    }

    @Test
    void getTable_returnsEmptyForUnknownTable() {
        assertThat(catalog.getTable("missing")).isEmpty();
    }

    @Test
    void dropTable_removesTable() {
        catalog.createTable(usersTable());
        catalog.dropTable("users");
        assertThat(catalog.tableExists("users")).isFalse();
        assertThat(catalog.getTable("users")).isEmpty();
    }

    @Test
    void dropTable_removesAssociatedIndexes() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());
        catalog.createIndex(ageIndex());
        assertThat(catalog.getIndexesForTable("users")).hasSize(2);

        catalog.dropTable("users");

        assertThat(catalog.listIndexes()).doesNotContain("idx_users_email", "idx_users_age");
    }

    @Test
    void dropTable_throwsForUnknownTable() {
        assertThatThrownBy(() -> catalog.dropTable("ghost"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createTable_throwsOnDuplicateName() {
        catalog.createTable(usersTable());
        assertThatThrownBy(() -> catalog.createTable(usersTable()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("users");
    }

    @Test
    void listTables_returnsAllTableNames() {
        catalog.createTable(usersTable());
        catalog.createTable(ordersTable());
        assertThat(catalog.listTables()).containsExactlyInAnyOrder("users", "orders");
    }

    @Test
    void listTables_emptyWhenNoneRegistered() {
        assertThat(catalog.listTables()).isEmpty();
    }

    // ── Index tests ───────────────────────────────────────────────────────────

    @Test
    void createIndex_registersIndex() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());
        assertThat(catalog.listIndexes()).contains("idx_users_email");
    }

    @Test
    void getIndexesForTable_returnsAllIndexesForTable() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());
        catalog.createIndex(ageIndex());

        List<IndexSchema> indexes = catalog.getIndexesForTable("users");
        assertThat(indexes).hasSize(2);
        assertThat(indexes).extracting(IndexSchema::indexName)
                .containsExactlyInAnyOrder("idx_users_email", "idx_users_age");
    }

    @Test
    void getIndexesForTable_returnsEmptyWhenNoIndexes() {
        catalog.createTable(usersTable());
        assertThat(catalog.getIndexesForTable("users")).isEmpty();
    }

    @Test
    void getIndexForColumn_findsIndexCoveringColumn() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());

        assertThat(catalog.getIndexForColumn("users", "email"))
                .hasValueSatisfying(idx -> assertThat(idx.indexName()).isEqualTo("idx_users_email"));
    }

    @Test
    void getIndexForColumn_returnsEmptyWhenNoIndexOnColumn() {
        catalog.createTable(usersTable());
        assertThat(catalog.getIndexForColumn("users", "email")).isEmpty();
    }

    @Test
    void dropIndex_removesOnlyThatIndex() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());
        catalog.createIndex(ageIndex());

        catalog.dropIndex("idx_users_email");

        assertThat(catalog.listIndexes()).doesNotContain("idx_users_email");
        assertThat(catalog.listIndexes()).contains("idx_users_age");
    }

    @Test
    void dropIndex_throwsForUnknownIndex() {
        assertThatThrownBy(() -> catalog.dropIndex("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createIndex_throwsOnDuplicateName() {
        catalog.createTable(usersTable());
        catalog.createIndex(emailIndex());
        assertThatThrownBy(() -> catalog.createIndex(emailIndex()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("idx_users_email");
    }

    @Test
    void listIndexes_returnsAllIndexNames() {
        catalog.createTable(usersTable());
        catalog.createTable(ordersTable());
        catalog.createIndex(emailIndex());
        catalog.createIndex(ageIndex());
        catalog.createIndex(new IndexSchema("idx_orders_user", "orders", List.of("user_id"), false));

        assertThat(catalog.listIndexes())
                .containsExactlyInAnyOrder("idx_users_email", "idx_users_age", "idx_orders_user");
    }

    @Test
    void createIndex_throwsWhenTableDoesNotExist() {
        assertThatThrownBy(() -> catalog.createIndex(emailIndex()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("users");
    }
}
