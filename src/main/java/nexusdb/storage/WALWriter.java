package nexusdb.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Write-Ahead Log writer with group commit support.
 *
 * <p>All WAL records are appended to a single file. The {@link #append} method
 * assigns monotonically increasing LSNs. The {@link #flush} method forces all
 * buffered records to disk via fsync, enabling group commit when multiple
 * virtual threads call flush concurrently — they share a single fsync.
 *
 * <p>Thread-safe: append and flush can be called from any thread.
 */
public final class WALWriter implements AutoCloseable {

    private static final String WAL_FILE_NAME = "wal.log";

    private final AtomicLong nextLsn = new AtomicLong(1);
    private final ReentrantLock writeLock = new ReentrantLock();
    private final FileChannel channel;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(8192);
    private volatile long flushedLsn = 0;

    public WALWriter(Path dataDir) throws IOException {
        Files.createDirectories(dataDir);
        Path walPath = dataDir.resolve(WAL_FILE_NAME);
        this.channel = FileChannel.open(walPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    /**
     * Append a WAL record and return its assigned LSN.
     * The record is buffered in memory until {@link #flush} is called.
     */
    public long append(long txnId, WALRecord.RecordType type, String key,
                       byte[] beforeImage, byte[] afterImage, long prevLsn) {
        long lsn = nextLsn.getAndIncrement();
        WALRecord record = new WALRecord(lsn, txnId, type, key, beforeImage, afterImage, prevLsn);
        byte[] serialized = record.serialize();

        writeLock.lock();
        try {
            buffer.write(serialized);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            writeLock.unlock();
        }
        return lsn;
    }

    /**
     * Flush all buffered records to disk and fsync.
     * Multiple concurrent callers share a single fsync (group commit).
     */
    public void flush() throws IOException {
        writeLock.lock();
        try {
            if (buffer.size() == 0) return;
            byte[] data = buffer.toByteArray();
            buffer.reset();
            channel.write(ByteBuffer.wrap(data));
            channel.force(false);
            flushedLsn = nextLsn.get() - 1;
        } finally {
            writeLock.unlock();
        }
    }

    /** Returns the highest LSN that has been flushed to disk. */
    public long flushedLsn() {
        return flushedLsn;
    }

    @Override
    public void close() throws IOException {
        flush();
        channel.close();
    }

    /**
     * Read all WAL records from the log file in the given directory.
     * Used for recovery.
     */
    public static List<WALRecord> readAll(Path dataDir) throws IOException {
        Path walPath = dataDir.resolve(WAL_FILE_NAME);
        if (!Files.exists(walPath) || Files.size(walPath) == 0) {
            return List.of();
        }

        byte[] data = Files.readAllBytes(walPath);
        ByteBuffer buf = ByteBuffer.wrap(data);
        List<WALRecord> records = new ArrayList<>();

        while (buf.hasRemaining()) {
            try {
                records.add(WALRecord.deserialize(buf));
            } catch (Exception e) {
                // Partial record at end of file (torn write) — stop here
                break;
            }
        }

        return records;
    }
}
