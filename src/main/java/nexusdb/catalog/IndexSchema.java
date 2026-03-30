package nexusdb.catalog;

import java.util.List;

/**
 * Schema definition for a secondary index.
 *
 * @param indexName table-unique index name
 * @param tableName the table being indexed
 * @param columns   ordered list of indexed column names
 * @param isUnique  whether the index enforces uniqueness
 */
public record IndexSchema(
        String indexName,
        String tableName,
        List<String> columns,
        boolean isUnique
) {}
