package com.github.alexishuf.fastersparql.grep;

import com.github.alexishuf.fastersparql.util.ThrowingConsumer;
import com.github.alexishuf.fastersparql.util.concurrent.LIFOPool;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Thread.currentThread;
import static java.lang.foreign.MemorySegment.copy;

public class FileScanner implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileScanner.class);
    private static final AtomicInteger nextScannerId = new AtomicInteger();
    private static final VarHandle USER, ACTIVE_CHUNKS, LIVE_CHUNKS, ERROR, TASK_ST;
    static {
        try {
            USER           = MethodHandles.lookup().findVarHandle(FileScanner.class, "plainUser", Thread.class);
            ACTIVE_CHUNKS  = MethodHandles.lookup().findVarHandle(FileScanner.class, "plainActiveChunks", int.class);
            LIVE_CHUNKS    = MethodHandles.lookup().findVarHandle(FileScanner.class, "plainLiveChunks", int.class);
            ERROR          = MethodHandles.lookup().findVarHandle(FileScanner.class, "plainError", Throwable.class);
            TASK_ST        = MethodHandles.lookup().findVarHandle(FileChunkTask.class, "plainSt", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String scannerName;
    private final int chunkBytes, maxLiveChunks;
    private final ExecutorService threadPool;
    private final LIFOPool<FileChunkTask> chunks;
    private final Arena arena = Arena.ofShared();
    @SuppressWarnings("unused") private int plainActiveChunks, plainLiveChunks;
    @SuppressWarnings("unused") private @Nullable Throwable plainError;
    @SuppressWarnings("unused") private @Nullable Thread plainUser;

    public FileScanner(int threads, int chunkBytes, int maxChunksPerThread) {
        if (threads <= 0)
            throw new IllegalArgumentException("threads <= 0");
        if (maxChunksPerThread <= 0)
            throw new IllegalArgumentException("maxChunksPerThread <= 0");
        if (chunkBytes <= 0)
            throw new IllegalArgumentException("chunkBytes <= 0");
        this.scannerName = getClass().getSimpleName()+"-"+nextScannerId.getAndIncrement();
        this.maxLiveChunks = threads*maxChunksPerThread;
        this.chunks      = new LIFOPool<>(FileChunkTask.class, maxLiveChunks);
        this.chunkBytes  = chunkBytes;
        var grp          = new ThreadGroup(currentThread().getThreadGroup(), scannerName);
        this.threadPool  = Executors.newFixedThreadPool(threads, new ThreadFactory() {
            private final AtomicInteger nextId = new AtomicInteger(0);
            @Override public Thread newThread(@NonNull Runnable r) {
                var t = new Thread(grp, r, scannerName + '-' + nextId.getAndIncrement());
                if (t.getPriority() != Thread.NORM_PRIORITY)
                    t.setPriority(Thread.NORM_PRIORITY);
                t.setDaemon(true);
                return t;
            }
        });
    }

    @Override public void close() {
        Thread user, me = currentThread();
        while ((user=(Thread)USER.getOpaque(this)) != null && user != me)
            Thread.yield();
        arena.close();
    }

    /**
     * Exception that if thrown by a {@code chunkProcessor} given to
     * {@link #scan(SeekableByteChannel, ThrowingConsumer)} will cause
     * scanning to stop ASAP, and if no processor had previously failed with an exception
     * that was not a {@link StopScan}, {@link #scan(SeekableByteChannel, ThrowingConsumer)}
     * will return as if the file had been fully scanned.
     */
    public static final class StopScan extends RuntimeException {
        public static final StopScan INSTANCE = new StopScan("Normal stop scan");
        public StopScan(String message) {this(message, null);}
        public StopScan(String message, Throwable cause) {super(message, cause);}
    }

    public void scan(SeekableByteChannel ch,
                     ThrowingConsumer<FileChunk, Exception> chunkProcessor)
            throws ExecutionException {
        if ((Thread)USER.compareAndExchangeAcquire(this, null, currentThread()) != null)
            throw new IllegalStateException("Concurrent use of FileScanner");
        try {
            ERROR.setRelease(this, null);
            try {
                long totalBytes = ch.size(), chunkPosition = ch.position();
                int seq = 0;
                FileChunkTask t = null;
                while (true) {
                    if (t == null)
                        t = getTask(chunkProcessor);
                    int readBytes = ch.read(t.recvBuffer());
                    t.len += Math.max(0, readBytes);
                    if (t.len > 0) { // if read something or has leftover
                        int trimmedLen = t.len, partialLen = 0;
                        if (readBytes > 0) { // if read additional bytes
                            trimmedLen = t.skipUntilLast(0, t.len, '\n');
                            if (trimmedLen < t.len)
                                ++trimmedLen; // include trailing newline in this chunk
                            partialLen = t.len-trimmedLen;
                            t.len      = trimmedLen;
                        }
                        t.assignOrigin(chunkPosition, seq++);
                        chunkPosition += t.len;
                        ACTIVE_CHUNKS.getAndAddRelease(this, 1);
                        threadPool.execute(t); // processor does not see beyond t.len
                        if (partialLen > 0) { // copy partial line to next chunk
                            var next = getTask(chunkProcessor); // can be == t
                            copy(t.segment, trimmedLen, next.segment, 0, partialLen);
                            (t = next).len = partialLen;
                        } else if (trimmedLen == 0 && ch.position() != totalBytes) {
                            throw new IOException("There are lines with more than " +
                                                  chunkBytes+" bytes, increase chunkBytes");
                        } else {
                            t = null;
                        }
                    } else if (ch.position() == totalBytes) {
                        break; // reached end
                    } else if (ch.size() != totalBytes) { // concurrent write by another process
                        throw new IOException("File size changed during scanning");
                    }
                }
            } catch (Throwable t) {
                ERROR.compareAndExchangeRelease(this, null, t);
            }  finally {
                while ((int)ACTIVE_CHUNKS.getOpaque(this) > 0)
                    LockSupport.park(this);
            }
            var error = (Throwable)ERROR.getAcquire(this);
            if (error != null && !(error instanceof StopScan))
                throw new ExecutionException(error);
        } finally {
            USER.setRelease(this, null);
        }
    }

    private FileChunkTask getTask(ThrowingConsumer<FileChunk, Exception> processor) {
        assert currentThread() == (Thread)USER.getOpaque(this) : "not called from USER thread";
        FileChunkTask task;
        while ((task = chunks.get()) == null) {
            if (plainLiveChunks < maxLiveChunks) {
                if ((int) LIVE_CHUNKS.getAndAddRelease(this, 1) < maxLiveChunks) {
                    task = new FileChunkTask();
                    break;
                } else {
                    LIVE_CHUNKS.getAndAddRelease(this, -1);
                }
            }
            LockSupport.park(this);
        }
        task.reset(processor);
        return task;
    }

    private final class FileChunkTask extends FileChunk implements Runnable {
        private static final int ST_NEW  = 0;
        private static final int ST_DONE = 1;
        private @MonotonicNonNull ThrowingConsumer<FileChunk, Exception> processor;
        @SuppressWarnings("unused") private int plainSt;

        public FileChunkTask() {
            super(arena.allocate(chunkBytes));
        }

        public void reset(ThrowingConsumer<FileChunk, Exception> processor) {
            int old = (int)TASK_ST.getOpaque(this);
            if ((old != ST_NEW || this.processor == null)
                    && (int)TASK_ST.compareAndExchangeAcquire(this, old, ST_NEW) == old) {
                this.processor = processor;
                len = 0;
            } else {
                throw new IllegalStateException("observed concurrent reset on "+this);
            }
        }

        public void assignOrigin(long firstBytePos, int chunkNumber) {
            this.firstBytePos = firstBytePos;
            this.chunkNumber = chunkNumber;
        }

        public String taskString() {
            String mid = switch ((int)TASK_ST.getOpaque(this)) {
                case ST_NEW -> "$FileChunkTask[NEW]";
                case ST_DONE -> "$FileChunkTask[DONE]";
                default -> "$FileChunkTask[<<INVALID_STATE>>]";
            };
            return FileScanner.this+mid+"](proc="+processor+", buf="+toString(0, len)+")";
        }

        @Override public void run() {
            try {
                if ((Throwable)ERROR.getOpaque(FileScanner.this) == null)
                    processor.accept(this);
            } catch (Exception e) {
                ERROR.compareAndExchangeRelease(FileScanner.this, null, e);
                log.error("processor for task {} failed: ", taskString(), e);
            } finally {
                int ac = (int)TASK_ST.compareAndExchangeRelease(this, ST_NEW, ST_DONE);
                try {
                    if (ac != ST_NEW)
                        log.error("{} state after execution was {} instead of ST_NEW", taskString(), ac);
                } catch (Throwable ignored) {}
                try {
                    chunks.offer(this);
                } catch (Throwable e) {
                    try {
                        log.error("Failed to recycle {}", taskString());
                    } catch (Throwable ignored) {}
                    LIVE_CHUNKS.getAndAddRelease(FileScanner.this, -1); // allow new FileChunkTask()
                }
                ACTIVE_CHUNKS.getAndAddRelease(FileScanner.this, -1);
                Unparker.unpark((Thread)USER.getOpaque(FileScanner.this));
            }
        }

    }
}
