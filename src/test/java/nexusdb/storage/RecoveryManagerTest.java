package nexusdb.storage;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for ARIES-style RecoveryManager.
 * Tests the three phases: Analysis → Redo → Undo.
 */
class RecoveryManagerTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Recover committed transaction: data is present after recovery")
    void recoverCommittedTransaction() throws Exception {
        // Write WAL records: UPDATE + COMMIT
        try (WALWriter wal = new WALWriter(tempDir)) {
            long lsn1 = wal.append(1L, WALRecord.RecordType.UPDATE, "name",
                    null, "alice".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, lsn1);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store).containsKey("name");
        assertThat(store.get("name")).isEqualTo("alice".getBytes());
    }

    @Test
    @DisplayName("Uncommitted transaction: data is NOT present after recovery (undo)")
    void uncommittedTransactionRolledBack() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            // Txn 1: committed
            long lsn1 = wal.append(1L, WALRecord.RecordType.UPDATE, "committed-key",
                    null, "committed-value".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, lsn1);

            // Txn 2: NOT committed (no COMMIT record)
            wal.append(2L, WALRecord.RecordType.UPDATE, "uncommitted-key",
                    null, "uncommitted-value".getBytes(), 0L);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store.get("committed-key")).isEqualTo("committed-value".getBytes());
        assertThat(store).doesNotContainKey("uncommitted-key");
    }

    @Test
    @DisplayName("Multiple committed transactions: all data recovered")
    void multipleCommittedTransactions() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long l1 = wal.append(1L, WALRecord.RecordType.UPDATE, "a",
                    null, "1".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, l1);

            long l2 = wal.append(2L, WALRecord.RecordType.UPDATE, "b",
                    null, "2".getBytes(), 0L);
            wal.append(2L, WALRecord.RecordType.COMMIT, "", null, null, l2);

            long l3 = wal.append(3L, WALRecord.RecordType.UPDATE, "c",
                    null, "3".getBytes(), 0L);
            wal.append(3L, WALRecord.RecordType.COMMIT, "", null, null, l3);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store.get("a")).isEqualTo("1".getBytes());
        assertThat(store.get("b")).isEqualTo("2".getBytes());
        assertThat(store.get("c")).isEqualTo("3".getBytes());
    }

    @Test
    @DisplayName("Overwrite: latest committed value wins")
    void overwriteLatestValueWins() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long l1 = wal.append(1L, WALRecord.RecordType.UPDATE, "key",
                    null, "v1".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, l1);

            long l2 = wal.append(2L, WALRecord.RecordType.UPDATE, "key",
                    "v1".getBytes(), "v2".getBytes(), 0L);
            wal.append(2L, WALRecord.RecordType.COMMIT, "", null, null, l2);

            long l3 = wal.append(3L, WALRecord.RecordType.UPDATE, "key",
                    "v2".getBytes(), "v3".getBytes(), 0L);
            wal.append(3L, WALRecord.RecordType.COMMIT, "", null, null, l3);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store.get("key")).isEqualTo("v3".getBytes());
    }

    @Test
    @DisplayName("Aborted transaction: explicit ABORT record causes rollback")
    void abortedTransactionRolledBack() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long l1 = wal.append(1L, WALRecord.RecordType.UPDATE, "keep",
                    null, "yes".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, l1);

            long l2 = wal.append(2L, WALRecord.RecordType.UPDATE, "discard",
                    null, "no".getBytes(), 0L);
            wal.append(2L, WALRecord.RecordType.ABORT, "", null, null, l2);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store.get("keep")).isEqualTo("yes".getBytes());
        assertThat(store).doesNotContainKey("discard");
    }

    @Test
    @DisplayName("Empty WAL: recovery returns empty store")
    void emptyWalReturnsEmptyStore() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store).isEmpty();
    }

    @Test
    @DisplayName("Delete (null afterImage) removes key during recovery")
    void deleteRemovesKeyDuringRecovery() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long l1 = wal.append(1L, WALRecord.RecordType.UPDATE, "key",
                    null, "value".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, l1);

            // Delete: afterImage is null
            long l2 = wal.append(2L, WALRecord.RecordType.UPDATE, "key",
                    "value".getBytes(), null, 0L);
            wal.append(2L, WALRecord.RecordType.COMMIT, "", null, null, l2);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        Map<String, byte[]> store = recovery.recover(tempDir);

        assertThat(store).doesNotContainKey("key");
    }

    @Test
    @DisplayName("Recovery returns highest committed timestamp for TransactionManager")
    void recoveryReturnsHighestTimestamp() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long l1 = wal.append(1L, WALRecord.RecordType.UPDATE, "a",
                    null, "1".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, l1);

            long l2 = wal.append(5L, WALRecord.RecordType.UPDATE, "b",
                    null, "2".getBytes(), 0L);
            wal.append(5L, WALRecord.RecordType.COMMIT, "", null, null, l2);
            wal.flush();
        }

        RecoveryManager recovery = new RecoveryManager();
        recovery.recover(tempDir);

        assertThat(recovery.highestTxnId()).isEqualTo(5L);
        assertThat(recovery.highestLsn()).isGreaterThan(0L);
    }
}
