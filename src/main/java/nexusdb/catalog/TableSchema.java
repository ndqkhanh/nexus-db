package nexusdb.catalog;

import java.util.List;

/**
 * Schema definition for a table.
 *
 * @param tableName        table name
 * @param columns          ordered list of column definitions
 * @param primaryKeyColumn name of the primary key column
 */
public record TableSchema(
        String tableName,
        List<ColumnSchema> columns,
        String primaryKeyColumn
) {}
