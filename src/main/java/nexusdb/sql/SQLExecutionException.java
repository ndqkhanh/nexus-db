package nexusdb.sql;

/** Thrown when a SQL statement cannot be executed (e.g., table not found, type mismatch). */
public final class SQLExecutionException extends RuntimeException {
    public SQLExecutionException(String message) {
        super(message);
    }

    public SQLExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
