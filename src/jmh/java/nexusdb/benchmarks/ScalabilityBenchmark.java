package nexusdb.benchmarks;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH scalability benchmark: measures throughput across 1/4/16/64 threads
 * with uniform and Zipfian-like workload distributions.
 *
 * Demonstrates NexusDB's ability to scale with virtual threads and
 * adaptive concurrency under realistic access patterns.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class ScalabilityBenchmark {

    private static final int KEY_SPACE = 10_000;

    private NexusDB db;

    @Setup(Level.Trial)
    public void setup() {
        db = NexusDB.open();
        Transaction txn = db.begin();
        for (int i = 0; i < KEY_SPACE; i++) {
            txn.put(String.format("k%05d", i), ("v" + i).getBytes());
        }
        txn.commit();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        db.close();
    }

    // ========== Uniform Distribution ==========

    @Benchmark
    @Threads(1)
    public void uniformReadWrite_1thread(Blackhole bh) {
        doUniformReadWrite(bh);
    }

    @Benchmark
    @Threads(4)
    public void uniformReadWrite_4threads(Blackhole bh) {
        doUniformReadWrite(bh);
    }

    @Benchmark
    @Threads(16)
    public void uniformReadWrite_16threads(Blackhole bh) {
        doUniformReadWrite(bh);
    }

    @Benchmark
    @Threads(64)
    public void uniformReadWrite_64threads(Blackhole bh) {
        doUniformReadWrite(bh);
    }

    // ========== Zipfian-like (hot keys) ==========

    @Benchmark
    @Threads(1)
    public void zipfianReadWrite_1thread(Blackhole bh) {
        doZipfianReadWrite(bh);
    }

    @Benchmark
    @Threads(4)
    public void zipfianReadWrite_4threads(Blackhole bh) {
        doZipfianReadWrite(bh);
    }

    @Benchmark
    @Threads(16)
    public void zipfianReadWrite_16threads(Blackhole bh) {
        doZipfianReadWrite(bh);
    }

    @Benchmark
    @Threads(64)
    public void zipfianReadWrite_64threads(Blackhole bh) {
        doZipfianReadWrite(bh);
    }

    // ========== Range Scan ==========

    @Benchmark
    @Threads(4)
    public void rangeScan_4threads(Blackhole bh) {
        doRangeScan(bh);
    }

    @Benchmark
    @Threads(16)
    public void rangeScan_16threads(Blackhole bh) {
        doRangeScan(bh);
    }

    // ========== Workload Implementations ==========

    private void doUniformReadWrite(Blackhole bh) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Transaction txn = db.begin();
        try {
            String key = String.format("k%05d", rng.nextInt(KEY_SPACE));
            if (rng.nextInt(100) < 80) {
                bh.consume(txn.get(key));
            } else {
                txn.put(key, ("u" + rng.nextInt()).getBytes());
            }
            txn.commit();
        } catch (Exception e) {
            txn.abort();
        }
    }

    /**
     * Zipfian-like distribution: 20% of keys receive 80% of accesses.
     * Models real-world hot-spot patterns (popular products, active users).
     */
    private void doZipfianReadWrite(Blackhole bh) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Transaction txn = db.begin();
        try {
            int keyIdx;
            if (rng.nextInt(100) < 80) {
                // 80% of accesses hit the first 20% of keys (hot set)
                keyIdx = rng.nextInt(KEY_SPACE / 5);
            } else {
                // 20% of accesses hit the remaining 80% of keys
                keyIdx = KEY_SPACE / 5 + rng.nextInt(KEY_SPACE - KEY_SPACE / 5);
            }
            String key = String.format("k%05d", keyIdx);

            if (rng.nextInt(100) < 80) {
                bh.consume(txn.get(key));
            } else {
                txn.put(key, ("z" + rng.nextInt()).getBytes());
            }
            txn.commit();
        } catch (Exception e) {
            txn.abort();
        }
    }

    private void doRangeScan(Blackhole bh) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Transaction txn = db.begin();
        try {
            int start = rng.nextInt(KEY_SPACE - 100);
            String from = String.format("k%05d", start);
            String to = String.format("k%05d", start + 100);
            List<String> keys = txn.scan(from, to);
            bh.consume(keys.size());
            txn.commit();
        } catch (Exception e) {
            txn.abort();
        }
    }
}
