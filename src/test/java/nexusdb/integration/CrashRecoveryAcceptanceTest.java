package nexusdb.integration;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Acceptance tests for WAL-based crash recovery.
 * Verifies that committed data survives simulated crashes and
 * uncommitted data is rolled back during recovery.
 */
class CrashRecoveryAcceptanceTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Committed data survives crash and recovery")
    void committedDataSurvivedCrash() {
        // Phase 1: write data and commit
        NexusDB db = NexusDB.open(tempDir);
        Transaction txn = db.begin();
        txn.put("name", "alice".getBytes());
        txn.put("age", "30".getBytes());
        txn.commit();

        // Simulate crash: close without clean shutdown
        db.crash();

        // Phase 2: recover and verify committed data is present
        NexusDB recovered = NexusDB.open(tempDir);
        Transaction verify = recovered.begin();
        assertThat(verify.get("name")).isEqualTo("alice".getBytes());
        assertThat(verify.get("age")).isEqualTo("30".getBytes());
        verify.commit();
        recovered.close();
    }

    @Test
    @DisplayName("Uncommitted data is rolled back during recovery")
    void uncommittedDataRolledBack() {
        // Phase 1: write committed data, then write uncommitted data
        NexusDB db = NexusDB.open(tempDir);

        Transaction txn1 = db.begin();
        txn1.put("committed-key", "committed-value".getBytes());
        txn1.commit();

        Transaction txn2 = db.begin();
        txn2.put("uncommitted-key", "uncommitted-value".getBytes());
        // txn2 NOT committed — crash before commit

        db.crash();

        // Phase 2: recover — committed key present, uncommitted key absent
        NexusDB recovered = NexusDB.open(tempDir);
        Transaction verify = recovered.begin();
        assertThat(verify.get("committed-key")).isEqualTo("committed-value".getBytes());
        assertThat(verify.get("uncommitted-key")).isNull();
        verify.commit();
        recovered.close();
    }

    @Test
    @DisplayName("Multiple transactions: only committed ones survive recovery")
    void multipleTransactionsRecovery() {
        NexusDB db = NexusDB.open(tempDir);

        // Committed: keys a, b, c
        Transaction t1 = db.begin();
        t1.put("a", "1".getBytes());
        t1.commit();

        Transaction t2 = db.begin();
        t2.put("b", "2".getBytes());
        t2.commit();

        Transaction t3 = db.begin();
        t3.put("c", "3".getBytes());
        t3.commit();

        // Uncommitted: keys d, e
        Transaction t4 = db.begin();
        t4.put("d", "4".getBytes());

        Transaction t5 = db.begin();
        t5.put("e", "5".getBytes());

        db.crash();

        // Recover
        NexusDB recovered = NexusDB.open(tempDir);
        Transaction verify = recovered.begin();
        assertThat(verify.get("a")).isEqualTo("1".getBytes());
        assertThat(verify.get("b")).isEqualTo("2".getBytes());
        assertThat(verify.get("c")).isEqualTo("3".getBytes());
        assertThat(verify.get("d")).isNull();
        assertThat(verify.get("e")).isNull();
        verify.commit();
        recovered.close();
    }

    @Test
    @DisplayName("Overwrite survives crash: latest committed value is recovered")
    void overwriteSurvivesCrash() {
        NexusDB db = NexusDB.open(tempDir);

        Transaction t1 = db.begin();
        t1.put("key", "v1".getBytes());
        t1.commit();

        Transaction t2 = db.begin();
        t2.put("key", "v2".getBytes());
        t2.commit();

        Transaction t3 = db.begin();
        t3.put("key", "v3".getBytes());
        t3.commit();

        db.crash();

        NexusDB recovered = NexusDB.open(tempDir);
        Transaction verify = recovered.begin();
        assertThat(verify.get("key")).isEqualTo("v3".getBytes());
        verify.commit();
        recovered.close();
    }
}
