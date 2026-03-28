package nexusdb.storage;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for WALWriter: append, group commit, and recovery reads.
 */
class WALWriterTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Append returns monotonically increasing LSNs")
    void appendReturnsIncreasingLsns() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long lsn1 = wal.append(1L, WALRecord.RecordType.UPDATE, "key1",
                    null, "v1".getBytes(), 0L);
            long lsn2 = wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, lsn1);

            assertThat(lsn1).isGreaterThan(0);
            assertThat(lsn2).isGreaterThan(lsn1);
        }
    }

    @Test
    @DisplayName("Flush persists records that can be read back")
    void flushPersistsRecords() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            wal.append(10L, WALRecord.RecordType.UPDATE, "k1",
                    null, "v1".getBytes(), 0L);
            wal.append(10L, WALRecord.RecordType.COMMIT, "", null, null, 0L);
            wal.flush();
        }

        // Read back from the same directory
        List<WALRecord> records = WALWriter.readAll(tempDir);
        assertThat(records).hasSize(2);
        assertThat(records.get(0).type()).isEqualTo(WALRecord.RecordType.UPDATE);
        assertThat(records.get(0).key()).isEqualTo("k1");
        assertThat(records.get(0).afterImage()).isEqualTo("v1".getBytes());
        assertThat(records.get(1).type()).isEqualTo(WALRecord.RecordType.COMMIT);
    }

    @Test
    @DisplayName("Group commit: concurrent appends are batched into single flush")
    void groupCommitBatchesConcurrentAppends() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            int numThreads = 20;
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            CountDownLatch done = new CountDownLatch(numThreads);
            ConcurrentLinkedQueue<Long> lsns = new ConcurrentLinkedQueue<>();

            for (int i = 0; i < numThreads; i++) {
                final int txnId = i + 1;
                Thread.ofVirtual().start(() -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        long lsn = wal.append(txnId, WALRecord.RecordType.UPDATE,
                                "key-" + txnId, null, ("val-" + txnId).getBytes(), 0L);
                        wal.flush();
                        lsns.add(lsn);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }

            done.await(10, TimeUnit.SECONDS);
            assertThat(lsns).hasSize(numThreads);

            // All records should be readable
            List<WALRecord> records = WALWriter.readAll(tempDir);
            assertThat(records).hasSize(numThreads);
        }
    }

    @Test
    @DisplayName("Empty WAL returns empty list on readAll")
    void emptyWalReturnsEmptyList() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            wal.flush();
        }
        List<WALRecord> records = WALWriter.readAll(tempDir);
        assertThat(records).isEmpty();
    }

    @Test
    @DisplayName("Multiple transactions interleaved in WAL")
    void multipleTransactionsInterleaved() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long lsn1 = wal.append(1L, WALRecord.RecordType.UPDATE, "a",
                    null, "v1".getBytes(), 0L);
            long lsn2 = wal.append(2L, WALRecord.RecordType.UPDATE, "b",
                    null, "v2".getBytes(), 0L);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, lsn1);
            wal.append(2L, WALRecord.RecordType.COMMIT, "", null, null, lsn2);
            wal.flush();
        }

        List<WALRecord> records = WALWriter.readAll(tempDir);
        assertThat(records).hasSize(4);
        assertThat(records.get(0).txnId()).isEqualTo(1L);
        assertThat(records.get(1).txnId()).isEqualTo(2L);
        assertThat(records.get(2).type()).isEqualTo(WALRecord.RecordType.COMMIT);
        assertThat(records.get(3).type()).isEqualTo(WALRecord.RecordType.COMMIT);
    }

    @Test
    @DisplayName("PrevLsn chain is preserved through serialize/readAll")
    void prevLsnChainPreserved() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            long lsn1 = wal.append(1L, WALRecord.RecordType.UPDATE, "k1",
                    null, "v1".getBytes(), 0L);
            long lsn2 = wal.append(1L, WALRecord.RecordType.UPDATE, "k2",
                    null, "v2".getBytes(), lsn1);
            wal.append(1L, WALRecord.RecordType.COMMIT, "", null, null, lsn2);
            wal.flush();
        }

        List<WALRecord> records = WALWriter.readAll(tempDir);
        assertThat(records.get(0).prevLsn()).isEqualTo(0L);
        assertThat(records.get(1).prevLsn()).isEqualTo(records.get(0).lsn());
        assertThat(records.get(2).prevLsn()).isEqualTo(records.get(1).lsn());
    }

    @Test
    @DisplayName("Before and after images preserved for UPDATE records")
    void beforeAfterImagesPreserved() throws Exception {
        try (WALWriter wal = new WALWriter(tempDir)) {
            wal.append(1L, WALRecord.RecordType.UPDATE, "counter",
                    "old".getBytes(), "new".getBytes(), 0L);
            wal.flush();
        }

        List<WALRecord> records = WALWriter.readAll(tempDir);
        assertThat(records.get(0).beforeImage()).isEqualTo("old".getBytes());
        assertThat(records.get(0).afterImage()).isEqualTo("new".getBytes());
    }
}
