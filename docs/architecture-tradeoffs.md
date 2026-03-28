# NexusDB — Architecture Trade-offs

Every architectural decision involves a trade-off. This document explains the six most consequential design choices in NexusDB, what was sacrificed in each case, and when a different approach would be better.

---

## 1. Adaptive Lock Elision (CAS vs 2PL Switching)

**What was chosen.** `AdaptiveLockManager` dispatches writes to either a CAS optimistic path or a `ReentrantLock` pessimistic path. `ContentionMonitor` tracks per-range CAS failure rates across 256 key ranges (`NUM_RANGES = 256`) and triggers mode transitions via hysteresis: escalate to LOCK when failure ratio exceeds 30%, de-escalate to CAS when it drops below 15%.

**What's sacrificed.**
- ~10-20ns monitoring overhead per write (two `AtomicLong` increments in `recordOutcome()`)
- Risk of mode thrashing near the 15-30% threshold boundary
- False sharing between adjacent `RangeState` objects in the array (no `@Contended` padding)
- Up to `MAX_CAS_RETRIES = 8` wasted CAS attempts during transition from CAS to LOCK mode

**When the alternative wins.**
- **Stable high contention:** Pure 2PL avoids wasted CAS retries entirely
- **Very few distinct keys:** 256 ranges is wasteful when only a handful of keys exist — a single global lock would be simpler
- **Rapidly oscillating contention:** Workloads that flutter between 20-25% failure rate can cause mode thrashing despite hysteresis

**Engineering judgment.** Right default for an embedded database that doesn't know its workload a priori. The fallback at `AdaptiveLockManager.java:78` — even in CAS mode, if 8 retries exhaust, fall back to pessimistic for that single operation — guarantees forward progress under any contention level.

---

## 2. MVCC Version Chains (AtomicReference Linked List)

**What was chosen.** Per-key linked list of versions via `AtomicReference<Version>` head pointer in `VersionChain.java`. New versions are prepended via CAS. Reads traverse head-to-tail in `findVisible()` to find the first version visible at the snapshot timestamp.

**What's sacrificed.**
- O(V) read cost per key where V = number of active versions
- ~40-48 bytes memory overhead per version (txnId, commitTimestamp, value reference, previous pointer, status)
- No random access by timestamp — must walk from head every time

**When the alternative wins.**
- **Append-only version log (PostgreSQL-style):** Binary search on timestamp gives O(log V) reads. Better for keys with hundreds of versions.
- **Column-oriented storage:** For analytical queries scanning many keys, a columnar layout avoids per-key pointer chasing.

**Engineering judgment.** For 1-5 active versions per key (typical with epoch GC running), the linked-list approach is optimal: zero contention on reads (immutable `previous` pointers), CAS-based prepend for writes. `SnapshotReader` further optimizes with `StampedLock.tryOptimisticRead()` — the happy path is two volatile reads with no CAS at all.

---

## 3. Epoch-based GC vs VACUUM

**What was chosen.** `EpochGarbageCollector` implements a three-epoch RCU-inspired scheme. Readers register via `enterEpoch()`/`exitEpoch()`. The `gcSweep()` method advances the global epoch only when all readers have exited the previous one, reclaiming versions retired at epoch E-2 or earlier.

**What's sacrificed.**
- **Delayed reclamation:** Versions retired at epoch E aren't freed until epoch E+2. Long-lived readers block epoch advancement.
- **Unbounded retireList:** The `ConcurrentLinkedQueue` has no size cap. Under slow readers, memory grows unboundedly.
- **O(T) scan per sweep:** `gcSweep()` iterates all entries in the `threadEpochs` map. With thousands of virtual threads, this scan becomes expensive.

**When the alternative wins.**
- **Stop-the-world VACUUM:** Simpler for batch workloads with natural quiet periods. Guarantees complete cleanup.
- **Timestamp-based GC:** Track the oldest active snapshot timestamp globally. Reclaim any version with `commitTimestamp < oldest_snapshot`. More precise than epoch tracking, and `TransactionManager` already has the data via `ssiRegistry`.

**Engineering judgment.** Non-blocking GC is critical for an embedded database — readers never wait on GC, and GC never blocks readers. The three-epoch invariant provides a clean safety proof. The real risk is unbounded memory under long-lived readers, mitigable with a high-water mark that triggers warnings or forces stale readers to abort.

---

## 4. In-memory with WAL vs Disk-based

**What was chosen.** All data lives in the in-memory `BTree`. The `WALWriter` provides crash durability via append-only logging. On recovery, `RecoveryManager` replays the entire WAL via `Files.readAllBytes` to rebuild the in-memory state.

**What's sacrificed.**
- **Dataset must fit in memory.** No page eviction, no buffer pool, no disk overflow.
- **Recovery time proportional to WAL size.** `Files.readAllBytes` reads the entire WAL into memory. For a large WAL, this is both slow and memory-intensive.
- **No WAL truncation.** The WAL grows forever. No checkpointing mechanism to allow truncation. Recovery always replays from the beginning.

**When the alternative wins.**
- **Disk-based with buffer pool (SQLite, InnoDB):** Supports datasets larger than memory. Essential when data size is unpredictable.
- **LSM-tree (RocksDB):** For write-heavy workloads, memtable-to-SSTable flushing provides both durability and memory management without explicit WAL management.

**Engineering judgment.** For an embedded database targeting SQLite-class workloads, in-memory-with-WAL is pragmatic. It delivers microsecond read latency (no I/O on reads), simple implementation, and crash safety via WAL. The constraint is clearly scoped — this is an embedded engine for datasets that fit in memory, not a general-purpose DBMS.

---

## 5. SSI vs Simpler Snapshot Isolation

**What was chosen.** `TransactionManager` supports both `SNAPSHOT` and `SERIALIZABLE` isolation via the `IsolationLevel` enum. SSI is implemented via rw-antidependency tracking in `TransactionImpl` — each SERIALIZABLE transaction maintains `readSet`, `inboundRwAntideps`, and `outboundRwAntideps`. The "dangerous pivot" detection checks for the presence of both inbound and outbound edges.

**What's sacrificed.**
- **O(W x T) validation cost:** For W writes and T concurrent SERIALIZABLE transactions, the validation loop iterates all combinations.
- **Read set memory:** Every SERIALIZABLE transaction tracks every key it reads. Scan-heavy workloads create large sets.
- **False-positive aborts:** SSI is conservative — it aborts transactions that *might* form a cycle, even if the actual order is valid. This is inherent to SSI.
- **No predicate locks:** Only individual keys are tracked, not ranges. A phantom insert within a scanned range may not be detected.

**When the alternative wins.**
- **Plain Snapshot Isolation:** If applications handle write skew via explicit checks, SI is strictly cheaper. No read set tracking, no validation scan.
- **2PL Serializable:** Zero false-positive aborts at the cost of reduced concurrency from lock contention.

**Engineering judgment.** Offering both levels is the right architecture. Default is `SNAPSHOT` (zero SSI overhead), and `SERIALIZABLE` is opt-in. This means 99% of transactions pay no SSI cost. The implementation follows the canonical algorithm from Cahill et al. ("Serializable Isolation for Snapshot Databases") — the same approach PostgreSQL uses.

---

## 6. Single-module Embedded vs Client-Server

**What was chosen.** NexusDB is a single-module embedded library. `TransactionManager` is instantiated directly in-process. No network protocol, no separate server process, no connection pooling.

**What's sacrificed.**
- **No multi-process access.** Only one JVM can use the database at a time.
- **No access control.** No authentication, authorization, or audit logging at the protocol level.
- **Crash scope.** If the host application crashes, the database crashes with it.

**When the alternative wins.**
- **Client-server (PostgreSQL, MySQL):** When multiple applications need concurrent access, when access control is required, or when the database should outlive the application lifecycle.

**Engineering judgment.** Correct by definition for an embedded database. SQLite, H2, and RocksDB all make this choice. The WAL provides crash recovery, and the in-process architecture eliminates network latency entirely. The clean `TransactionManager` API could be wrapped in a gRPC server without architectural changes.

---

## Summary

| Dimension | Choice | Key Trade-off | Risk Level |
|-----------|--------|---------------|------------|
| Concurrency | Adaptive CAS/2PL | Monitoring overhead vs optimal throughput | Low |
| MVCC | AtomicReference chains | O(V) traversal vs zero-contention reads | Low |
| GC | Epoch-based (RCU) | Delayed reclamation vs non-blocking | Medium |
| Storage | In-memory + WAL | Memory-bound vs microsecond reads | Medium |
| Isolation | SSI + SI dual-mode | Validation cost vs correctness guarantee | Low |
| Architecture | Embedded library | No multi-process vs zero network latency | Low |
