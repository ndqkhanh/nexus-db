package nexusdb.catalog;

/**
 * Schema definition for a single column.
 *
 * @param name         column name
 * @param type         data type
 * @param isPrimaryKey whether this column is the primary key
 * @param isNotNull    whether this column has a NOT NULL constraint
 */
public record ColumnSchema(
        String name,
        DataType type,
        boolean isPrimaryKey,
        boolean isNotNull
) {}
