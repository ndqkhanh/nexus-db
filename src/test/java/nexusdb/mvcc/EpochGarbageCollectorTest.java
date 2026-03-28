package nexusdb.mvcc;

import org.junit.jupiter.api.*;

import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

class EpochGarbageCollectorTest {

    private EpochGarbageCollector gc;

    @BeforeEach
    void setUp() {
        gc = new EpochGarbageCollector();
    }

    @Test
    @DisplayName("Initial global epoch is 0")
    void initialEpochIsZero() {
        assertThat(gc.currentEpoch()).isEqualTo(0L);
    }

    @Test
    @DisplayName("enterEpoch returns current global epoch")
    void enterEpochReturnsCurrent() {
        long epoch = gc.enterEpoch();
        assertThat(epoch).isEqualTo(0L);
        gc.exitEpoch();
    }

    @Test
    @DisplayName("Epoch advances when no active readers exist")
    void epochAdvancesWithNoReaders() {
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(2L);
    }

    @Test
    @DisplayName("Epoch does NOT advance past a reader still in a previous epoch")
    void epochBlockedByLaggingReader() {
        gc.enterEpoch(); // reader enters at epoch 0

        // Epoch CAN advance from 0→1 (reader is at current epoch, not behind)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);

        // But epoch CANNOT advance from 1→2 because reader is still at epoch 0 (behind)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L); // blocked

        gc.exitEpoch(); // reader exits

        gc.gcSweep(); // now advances to 2
        assertThat(gc.currentEpoch()).isEqualTo(2L);
    }

    @Test
    @DisplayName("Retired version is reclaimed after 2 epoch advances")
    void retiredVersionReclaimedAfterTwoEpochs() {
        Version v = new Version(1L, "old".getBytes(), null);
        v.commit(1L);

        gc.retire(v); // retired at epoch 0
        assertThat(gc.retiredCount()).isEqualTo(1);

        gc.gcSweep(); // advance to epoch 1
        gc.gcSweep(); // advance to epoch 2
        // Now epoch is 2, version retired at 0 → 0 <= 2-2=0, reclaimable
        gc.gcSweep(); // reclaim at epoch 3 (reclaimBefore = 3-2=1, 0 <= 1 ✓)

        assertThat(gc.retiredCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Retired version is NOT reclaimed while reader from that epoch is still lagging")
    void retiredVersionNotReclaimedWhileReaderLagging() {
        gc.enterEpoch(); // reader enters at epoch 0

        Version v = new Version(1L, "data".getBytes(), null);
        v.commit(1L);
        gc.retire(v); // retired at epoch 0

        // Epoch advances 0→1 (reader at current epoch, allowed)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);
        // reclaimBefore = 1-2 = -1, nothing reclaimed
        assertThat(gc.retiredCount()).isEqualTo(1);

        // Epoch blocked at 1 (reader still at 0, lagging)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);
        assertThat(gc.retiredCount()).isEqualTo(1); // still NOT reclaimed

        gc.exitEpoch(); // reader exits

        // Now advance to reach reclamation threshold
        gc.gcSweep(); // epoch → 2, reclaimBefore = 0, retired at 0 <= 0 ✓
        assertThat(gc.currentEpoch()).isEqualTo(2L);

        // Need one more sweep to actually reclaim (epoch now 2, reclaimBefore=0)
        gc.gcSweep(); // epoch → 3, reclaimBefore = 1, retired at 0 <= 1 ✓
        assertThat(gc.retiredCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Multiple concurrent readers: epoch blocked when any reader lags behind")
    void multipleReadersBlockEpoch() throws Exception {
        CountDownLatch reader1Entered = new CountDownLatch(1);
        CountDownLatch reader1CanExit = new CountDownLatch(1);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        // Reader 1: enters at epoch 0 and holds
        executor.submit(() -> {
            gc.enterEpoch();
            reader1Entered.countDown();
            try { reader1CanExit.await(5, TimeUnit.SECONDS); } catch (InterruptedException e) { }
            gc.exitEpoch();
        });

        reader1Entered.await(5, TimeUnit.SECONDS);

        // Advance epoch 0→1 (reader1 at current epoch 0, allowed)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);

        // Epoch CANNOT advance 1→2 because reader1 is still at epoch 0 (lagging)
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(1L);

        // Let reader 1 exit
        reader1CanExit.countDown();
        Thread.sleep(50); // allow thread to complete

        // Now it can advance
        gc.gcSweep();
        assertThat(gc.currentEpoch()).isEqualTo(2L);

        executor.close();
    }

    @Test
    @DisplayName("retire + gcSweep is thread-safe under concurrent access")
    void concurrentRetireAndSweep() throws Exception {
        int writerCount = 10;
        int versionsPerWriter = 50;
        CountDownLatch done = new CountDownLatch(writerCount);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            executor.submit(() -> {
                for (int i = 0; i < versionsPerWriter; i++) {
                    Version v = new Version(writerId * 1000L + i, "data".getBytes(), null);
                    v.commit(writerId * 1000L + i);
                    gc.retire(v);
                }
                done.countDown();
            });
        }

        done.await(10, TimeUnit.SECONDS);

        // Advance epochs enough to reclaim everything
        for (int i = 0; i < 5; i++) {
            gc.gcSweep();
        }

        assertThat(gc.retiredCount()).isEqualTo(0);
        executor.close();
    }
}
