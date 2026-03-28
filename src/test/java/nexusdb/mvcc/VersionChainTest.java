package nexusdb.mvcc;

import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class VersionChainTest {

    @Test
    @DisplayName("Empty chain returns null head")
    void emptyChainReturnsNull() {
        VersionChain chain = new VersionChain();
        assertThat(chain.getHead()).isNull();
    }

    @Test
    @DisplayName("Install single version sets it as head")
    void installSingleVersion() {
        VersionChain chain = new VersionChain();
        Version v = new Version(1L, "data".getBytes(), null);

        boolean installed = chain.installVersion(v);

        assertThat(installed).isTrue();
        assertThat(chain.getHead()).isSameAs(v);
    }

    @Test
    @DisplayName("Install prepends: newest version becomes head")
    void installPrependsNewest() {
        VersionChain chain = new VersionChain();
        Version v1 = new Version(1L, "v1".getBytes(), null);
        chain.installVersion(v1);

        Version v2 = new Version(2L, "v2".getBytes(), v1);
        chain.installVersion(v2);

        assertThat(chain.getHead()).isSameAs(v2);
        assertThat(chain.getHead().previous()).isSameAs(v1);
    }

    @Test
    @DisplayName("findVisible returns newest committed version visible at snapshot")
    void findVisibleReturnsCorrectVersion() {
        VersionChain chain = new VersionChain();

        // v1 committed at ts=10
        Version v1 = new Version(1L, "v1".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        // v2 committed at ts=20
        Version v2 = new Version(2L, "v2".getBytes(), v1);
        chain.installVersion(v2);
        v2.commit(20L);

        // v3 still active
        Version v3 = new Version(3L, "v3".getBytes(), v2);
        chain.installVersion(v3);

        // Snapshot at 25 sees v2 (v3 is active)
        assertThat(chain.findVisible(25L)).isSameAs(v2);
        // Snapshot at 15 sees v1 (v2 committed at 20, too new)
        assertThat(chain.findVisible(15L)).isSameAs(v1);
        // Snapshot at 5 sees nothing
        assertThat(chain.findVisible(5L)).isNull();
        // Snapshot at 20 sees v2 (exact match)
        assertThat(chain.findVisible(20L)).isSameAs(v2);
    }

    @Test
    @DisplayName("findVisible skips aborted versions")
    void findVisibleSkipsAborted() {
        VersionChain chain = new VersionChain();

        Version v1 = new Version(1L, "v1".getBytes(), null);
        chain.installVersion(v1);
        v1.commit(10L);

        Version v2 = new Version(2L, "v2".getBytes(), v1);
        chain.installVersion(v2);
        v2.abort(); // aborted — should be skipped

        assertThat(chain.findVisible(20L)).isSameAs(v1);
    }

    @Test
    @DisplayName("Concurrent installs: all versions installed without loss")
    void concurrentInstalls() throws Exception {
        VersionChain chain = new VersionChain();
        int threadCount = 20;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        for (int i = 0; i < threadCount; i++) {
            final long txnId = i;
            executor.submit(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // Retry loop — CAS may fail under contention
                while (true) {
                    Version currentHead = chain.getHead();
                    Version v = new Version(txnId, ("v" + txnId).getBytes(), currentHead);
                    if (chain.installVersion(v)) {
                        successCount.incrementAndGet();
                        break;
                    }
                }
                done.countDown();
            });
        }

        startGate.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.close();

        assertThat(completed).isTrue();
        assertThat(successCount.get()).isEqualTo(threadCount);

        // Walk the chain — should have exactly threadCount versions
        int count = 0;
        Version current = chain.getHead();
        while (current != null) {
            count++;
            current = current.previous();
        }
        assertThat(count).isEqualTo(threadCount);
    }
}
