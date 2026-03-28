package nexusdb;

import nexusdb.transaction.IsolationLevel;
import nexusdb.transaction.Transaction;
import nexusdb.transaction.TransactionManager;

import java.nio.file.Path;

/**
 * Public API entry point for NexusDB — an adaptive concurrency transactional engine.
 *
 * Usage:
 *   NexusDB db = NexusDB.open();
 *   Transaction txn = db.begin();
 *   txn.put("key", "value".getBytes());
 *   byte[] val = txn.get("key");
 *   txn.commit();
 */
public final class NexusDB {

    private final TransactionManager txnManager;
    private final Path dataDir;

    private NexusDB(Path dataDir) {
        this.dataDir = dataDir;
        this.txnManager = new TransactionManager(dataDir);
    }

    /** Open an in-memory database (no WAL, no durability). */
    public static NexusDB open() {
        return new NexusDB(null);
    }

    /** Open a durable database with WAL at the given directory. */
    public static NexusDB open(Path dataDir) {
        return new NexusDB(dataDir);
    }

    public Transaction begin() {
        return txnManager.begin();
    }

    public Transaction begin(IsolationLevel isolation) {
        return txnManager.begin(isolation);
    }

    public void close() {
        txnManager.close();
    }

    /**
     * Simulate a crash: abandon all in-memory state without flushing.
     * Used by recovery tests.
     */
    public void crash() {
        // Do NOT flush WAL or clean up — simulate abrupt termination
    }
}
