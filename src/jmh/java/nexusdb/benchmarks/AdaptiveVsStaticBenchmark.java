package nexusdb.benchmarks;

import nexusdb.NexusDB;
import nexusdb.transaction.Transaction;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark comparing NexusDB's adaptive concurrency engine throughput.
 *
 * Measures: transactions per second under varying contention levels.
 * Target: 80K+ txn/sec with adaptive lock elision.
 *
 * Workloads:
 * - Low contention: 10K key space (mostly CAS path)
 * - High contention: 10 key space (triggers 2PL path)
 * - Mixed read-write: 80% reads, 20% writes
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(2)
public class AdaptiveVsStaticBenchmark {

    @Param({"10", "1000", "10000"})
    private int keySpace;

    private NexusDB db;

    @Setup(Level.Trial)
    public void setup() {
        db = NexusDB.open();
        // Pre-populate
        Transaction txn = db.begin();
        for (int i = 0; i < keySpace; i++) {
            txn.put("key" + i, ("value" + i).getBytes());
        }
        txn.commit();
    }

    @TearDown(Level.Trial)
    public void teardown() {
        db.close();
    }

    @Benchmark
    @Threads(4)
    public void readWriteMixed_4threads(Blackhole bh) {
        doReadWriteMixed(bh);
    }

    @Benchmark
    @Threads(16)
    public void readWriteMixed_16threads(Blackhole bh) {
        doReadWriteMixed(bh);
    }

    @Benchmark
    @Threads(64)
    public void readWriteMixed_64threads(Blackhole bh) {
        doReadWriteMixed(bh);
    }

    @Benchmark
    @Threads(4)
    public void writeOnly_4threads(Blackhole bh) {
        doWriteOnly(bh);
    }

    @Benchmark
    @Threads(16)
    public void writeOnly_16threads(Blackhole bh) {
        doWriteOnly(bh);
    }

    @Benchmark
    @Threads(64)
    public void writeOnly_64threads(Blackhole bh) {
        doWriteOnly(bh);
    }

    private void doReadWriteMixed(Blackhole bh) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Transaction txn = db.begin();
        try {
            String key = "key" + rng.nextInt(keySpace);
            if (rng.nextInt(100) < 80) {
                // 80% reads
                bh.consume(txn.get(key));
            } else {
                // 20% writes
                txn.put(key, ("updated" + rng.nextInt()).getBytes());
            }
            txn.commit();
        } catch (Exception e) {
            txn.abort();
        }
    }

    private void doWriteOnly(Blackhole bh) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Transaction txn = db.begin();
        try {
            String key = "key" + rng.nextInt(keySpace);
            txn.put(key, ("val" + rng.nextInt()).getBytes());
            txn.commit();
            bh.consume(key);
        } catch (Exception e) {
            txn.abort();
        }
    }
}
