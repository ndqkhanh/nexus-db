package nexusdb.transaction;

/**
 * Supported transaction isolation levels.
 *
 * SNAPSHOT (default): prevents dirty reads, non-repeatable reads, phantom reads, lost updates.
 * SERIALIZABLE: additionally prevents write skew via SSI rw-antidependency detection.
 */
public enum IsolationLevel {
    SNAPSHOT,
    SERIALIZABLE
}
