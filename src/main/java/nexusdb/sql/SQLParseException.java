package nexusdb.sql;

/** Thrown when the SQL parser encounters invalid or unsupported syntax. */
public final class SQLParseException extends RuntimeException {
    public SQLParseException(String message) {
        super(message);
    }
}
