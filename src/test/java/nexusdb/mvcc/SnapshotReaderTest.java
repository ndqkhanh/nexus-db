package nexusdb.mvcc;

import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.locks.StampedLock;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for SnapshotReader — StampedLock optimistic reads on version chains.
 */
class SnapshotReaderTest {

    private SnapshotReader reader;

    @BeforeEach
    void setUp() {
        reader = new SnapshotReader();
    }

    @Test
    @DisplayName("Read from empty chain returns null")
    void emptyChainReturnsNull() {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        byte[] result = reader.read(chain, 100L, lock);

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Read returns committed version visible at snapshot")
    void readReturnsVisibleVersion() {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v1 = new Version(1L, "hello".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        byte[] result = reader.read(chain, 15L, lock);

        assertThat(result).isEqualTo("hello".getBytes());
    }

    @Test
    @DisplayName("Read skips versions committed after snapshot timestamp")
    void readSkipsFutureVersions() {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v1 = new Version(1L, "old".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        Version v2 = new Version(2L, "new".getBytes(), v1);
        chain.installVersion(v2);
        v2.commit(20L);

        // Snapshot at 15 should see v1, not v2
        byte[] result = reader.read(chain, 15L, lock);

        assertThat(result).isEqualTo("old".getBytes());
    }

    @Test
    @DisplayName("Read skips ACTIVE versions")
    void readSkipsActiveVersions() {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v1 = new Version(1L, "committed".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        Version v2 = new Version(2L, "in-progress".getBytes(), v1);
        chain.installVersion(v2);
        // v2 stays ACTIVE

        byte[] result = reader.read(chain, 20L, lock);

        assertThat(result).isEqualTo("committed".getBytes());
    }

    @Test
    @DisplayName("Read returns null for deletion tombstone")
    void readReturnNullForDeletion() {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v1 = new Version(1L, "data".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        // Deletion tombstone
        Version v2 = new Version(2L, null, v1);
        chain.installVersion(v2);
        v2.commit(20L);

        byte[] result = reader.read(chain, 25L, lock);

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Read falls back to read lock when write lock is held")
    void fallbackToReadLockWhenWriteLockHeld() throws Exception {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v1 = new Version(1L, "data".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        // Hold write lock to force fallback path
        long writestamp = lock.writeLock();

        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch readerDone = new CountDownLatch(1);
        byte[][] readerResult = new byte[1][];

        Thread readerThread = Thread.ofVirtual().start(() -> {
            readerStarted.countDown();
            readerResult[0] = reader.read(chain, 15L, lock);
            readerDone.countDown();
        });

        readerStarted.await(5, TimeUnit.SECONDS);
        Thread.sleep(50); // let reader block on readLock

        // Release write lock — reader should proceed via fallback
        lock.unlockWrite(writestamp);

        boolean done = readerDone.await(5, TimeUnit.SECONDS);
        assertThat(done).isTrue();
        assertThat(readerResult[0]).isEqualTo("data".getBytes());
    }

    @Test
    @DisplayName("Concurrent reads do not block each other")
    void concurrentReadsDoNotBlock() throws Exception {
        VersionChain chain = new VersionChain();
        StampedLock lock = new StampedLock();

        Version v = new Version(1L, "shared".getBytes(), null);
        chain.installVersion(v);
        v.commit(10L);

        int readerCount = 20;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(readerCount);
        ConcurrentLinkedQueue<byte[]> results = new ConcurrentLinkedQueue<>();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < readerCount; i++) {
            executor.submit(() -> {
                try { startGate.await(); } catch (InterruptedException e) { }
                results.add(reader.read(chain, 15L, lock));
                done.countDown();
            });
        }

        startGate.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.close();

        assertThat(completed).isTrue();
        assertThat(results)
                .hasSize(readerCount)
                .allSatisfy(r -> assertThat(r).isEqualTo("shared".getBytes()));
    }
}
