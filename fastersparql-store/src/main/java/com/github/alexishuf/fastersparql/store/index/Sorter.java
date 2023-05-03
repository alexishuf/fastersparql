package com.github.alexishuf.fastersparql.store.index;

import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static com.github.alexishuf.fastersparql.util.concurrent.Async.*;
import static java.lang.invoke.MethodHandles.lookup;

class Sorter<T> implements AutoCloseable {
    private static final VarHandle REC_BLOCK, WRITING;
    static {
        try {
            REC_BLOCK = lookup().findVarHandle(Sorter.class, "plainRecycled", BlockJob.class);
            WRITING = lookup().findVarHandle(Sorter.class, "plainWriting", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final Logger log = LoggerFactory.getLogger(DictSorter.class);

    @SuppressWarnings("unused") private BlockJob<T> plainRecycled;
    @SuppressWarnings("unused") private int plainWriting;

    protected final List<T> sorted = new ArrayList<>();
    private final Path tempDir;
    private final String tempFilePrefix, tempFileSuffix;

    private final ExceptionCondenser<IOException> blockJobError = new ExceptionCondenser<>(IOException.class, IOException::new);
    private final ArrayBlockingQueue<BlockJob<T>> blockJobs = new ArrayBlockingQueue<>(1);
    private final Thread blockJobsWorker = Thread.ofPlatform()
            .name(this+"-sorter").start(this::blockJobsWorker);

    public Sorter(Path tempDir, String tempFilePrefix, String tempFileSuffix) {
        this.tempDir = tempDir;
        this.tempFilePrefix = tempFilePrefix;
        this.tempFileSuffix = tempFileSuffix;
    }

    @Override public void close() {
        lock();
        try {
            cancelBlockJobs();
            closeBlock(REC_BLOCK.getAndSet(this, null));
            for (Object o : sorted)
                closeBlock(o);
            closeBlock(REC_BLOCK.getAndSet(this, null));
        } finally {
            WRITING.setRelease(this, 0);
        }
    }

    private static void closeBlock(Object o) {
        if (o instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception e) {
                log.info("Failed to close {}", c, e);
            }
        } else {
            if (o instanceof Path p)
                o = p.toFile();
            if (o instanceof File f && f.exists() && !f.delete())
                log.info("Failed to delete {}", f);
        }
    }

    @Override public String toString() {
        return String.format("%s@%x", getClass().getSimpleName(), System.identityHashCode(this));
    }

    protected void lock() {
        if ((int)WRITING.compareAndExchangeAcquire(this, 0, 1) != 0)
            throw new IllegalStateException("Concurrent close()/storeCopy()/writeDict()");
    }

    protected void unlock() {
        WRITING.setRelease(this, 0);
    }

    protected static int defaultBlockBytes() {
        return (int) Math.min(256*1024*1024, Runtime.getRuntime().maxMemory()/5);
    }

    protected Path createTempFile() throws IOException {
        return Files.createTempFile(tempDir, tempFilePrefix, tempFileSuffix);
    }

    protected void cancelBlockJobs() {
        while (blockJobsWorker.isAlive() && !blockJobs.offer(endJob())) {
            BlockJob<T> j = blockJobs.poll();
            if (j != null)
                j.close();
        }
        uninterruptibleJoin(blockJobsWorker);
    }

    protected void waitBlockJobs() throws IOException {
        if (blockJobsWorker.isAlive()) {
            uninterruptiblePut(blockJobs, endJob());
            uninterruptibleJoin(blockJobsWorker);
        }
        blockJobError.throwIf();
    }

    protected void scheduleBlockJob(BlockJob<T> job) {
        uninterruptiblePut(blockJobs, job);
    }


    protected BlockJob<T> recycled() { //noinspection unchecked
        BlockJob<T> j = (BlockJob<T>) REC_BLOCK.getAndSetAcquire(this, null);
        if (j != null)
            j.reset();
        return j;
    }

    protected T runResidual(@Nullable BlockJob<T> job) throws IOException {
        T result = null;
        if (job != null && !job.isEmpty()) {
            result = job.run();
            job.close();
        }
        return result;
    }

    protected interface BlockJob<T> extends AutoCloseable {
        @Override void close();
        default boolean isEnd() { return false; }
        @SuppressWarnings("BooleanMethodIsAlwaysInverted") boolean isEmpty();
        void reset();
        T run() throws IOException;
    }

    protected static final class EndBlockJob<T> implements BlockJob<T> {
        @Override public void    close()   { }
        @Override public boolean isEnd()   { return true; }
        @Override public boolean isEmpty() { return true; }
        @Override public void    reset()   { }
        @Override public T       run()     { throw new UnsupportedOperationException(); }
    }
    protected static final EndBlockJob<?> END_JOB = new EndBlockJob<>();
    protected EndBlockJob<T> endJob() { //noinspection unchecked
        return (EndBlockJob<T>) END_JOB;
    }

    private void blockJobsWorker() {
        boolean failed = false;
        for (BlockJob<T> j; !(j = uninterruptibleTake(blockJobs)).isEnd(); ) {
            boolean self = false;
            try {
                if (!failed) {
                    T result = j.run();
                    self = result == j;
                    sorted.add(result);
                }
            } catch (Throwable t) {
                blockJobError.condense(t);
                failed = true;
            } finally {
                if (!self) {
                    j.reset();
                    if ((BlockJob<?>) REC_BLOCK.compareAndExchangeRelease(this, null, j) != null)
                        j.close();
                }
            }
        }
    }
}
