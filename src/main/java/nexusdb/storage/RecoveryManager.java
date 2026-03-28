package nexusdb.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * ARIES-style recovery manager. Reads the WAL and reconstructs the
 * committed state of the database after a crash.
 *
 * <p>Three phases:
 * <ol>
 *   <li><b>Analysis</b> — scan all records to determine which transactions
 *       committed, aborted, or were in-flight at crash time.</li>
 *   <li><b>Redo</b> — replay all UPDATE/CLR records from committed transactions
 *       to rebuild the in-memory store.</li>
 *   <li><b>Undo</b> — discard effects of uncommitted/aborted transactions
 *       (in Phase 2's ConcurrentHashMap store, this means simply not applying them).</li>
 * </ol>
 *
 * <p>Phase 2 simplification: since the store is a ConcurrentHashMap (no dirty pages),
 * redo and undo are combined — we only apply records from committed transactions.
 */
public final class RecoveryManager {

    private long highestTxnId = 0;
    private long highestLsn = 0;

    /**
     * Recover the database state from the WAL in the given directory.
     *
     * @return a map of key → value representing the committed state
     */
    public Map<String, byte[]> recover(Path dataDir) throws IOException {
        List<WALRecord> records = WALWriter.readAll(dataDir);
        if (records.isEmpty()) {
            return new HashMap<>();
        }

        // ===== Analysis Phase =====
        // Determine transaction outcomes: committed, aborted, or in-flight
        Set<Long> committedTxns = new HashSet<>();
        Set<Long> abortedTxns = new HashSet<>();
        Set<Long> allTxns = new HashSet<>();

        for (WALRecord record : records) {
            if (record.txnId() > 0) {
                allTxns.add(record.txnId());
            }
            if (record.lsn() > highestLsn) {
                highestLsn = record.lsn();
            }
            if (record.txnId() > highestTxnId) {
                highestTxnId = record.txnId();
            }

            switch (record.type()) {
                case COMMIT -> committedTxns.add(record.txnId());
                case ABORT -> abortedTxns.add(record.txnId());
                default -> { }
            }
        }

        // In-flight transactions = all - committed - aborted (these are losers)

        // ===== Redo Phase =====
        // Replay UPDATE and CLR records from committed transactions only.
        // In-flight and aborted transactions are simply skipped (undo by omission).
        Map<String, byte[]> store = new HashMap<>();

        for (WALRecord record : records) {
            if (!record.isPhysical()) continue;
            if (!committedTxns.contains(record.txnId())) continue;

            String key = record.key();
            byte[] afterImage = record.afterImage();

            if (afterImage == null) {
                // Deletion
                store.remove(key);
            } else {
                store.put(key, afterImage);
            }
        }

        // ===== Undo Phase =====
        // In Phase 2 with ConcurrentHashMap, undo is implicit — we never applied
        // uncommitted records. In Phase 3 with B-Tree pages, this phase would
        // walk prevLsn chains and write CLRs.

        return store;
    }

    /** Highest transaction ID seen during recovery. */
    public long highestTxnId() {
        return highestTxnId;
    }

    /** Highest LSN seen during recovery. */
    public long highestLsn() {
        return highestLsn;
    }
}
