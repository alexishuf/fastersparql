package com.github.alexishuf.fastersparql.client.model.batch.operators;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.BItClosedException;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.client.model.batch.base.BoundedBufferedBIt;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

public class MergeBIt<T> extends BoundedBufferedBIt<T> {
    private static final Logger log = LoggerFactory.getLogger(MergeBIt.class);
    private static final AtomicInteger nextId = new AtomicInteger(1);

    private final Condition stopped = lock.newCondition();
    private final List<? extends BIt<T>> sources;
    private int recycler, activeSources;
    private boolean started, stopping;

    public MergeBIt(Collection<? extends BIt<T>> sources,
                    Class<T> elementClass) {
        this(sources, elementClass, "Merge-"+nextId.getAndIncrement());
    }

    public MergeBIt(Collection<? extends BIt<T>> sources,
                    Class<T> elementClass, @Nullable String name) {
        super(elementClass, name);
        //noinspection unchecked
        this.sources = sources instanceof List<?> list
                     ? (List<? extends BIt<T>>) list : new ArrayList<>(sources);
        //noinspection resource
        maxReadyBatches(this.sources.size());
    }

    /* --- --- helper methods --- --- --- */

    private void closeSources() {
        Throwable error = null;
        for (BIt<T> source : sources) {
            try {
                source.close();
            } catch (Throwable t) {
                if (error == null) error = t;
                else               error.addSuppressed(t);
            }
        }
        if      (error instanceof RuntimeException re) throw re;
        else if (error instanceof Error             e) throw e;
    }

    @Override protected void updateBatchCapacity() {
        super.updateBatchCapacity();
        for (BIt<T> source : sources)
            source.minBatch(minBatch).maxBatch(maxBatch);
    }

    @Override protected void updateTimer() {
        super.updateTimer();
        for (BIt<T> source : sources) {
            // maxWait must only be enforced when aggregating multiple Batches smaller than minBatch
            // enforcing maxWait at each source would lead to waiting when a batch could be built
            // by combining two incomplete batches.
            source.minWait(minWaitNs, TimeUnit.NANOSECONDS)
                  .maxWait(minWaitNs, TimeUnit.NANOSECONDS);
        }
    }

    private void drain(BIt<T> source) {
        try (source) {
            while (!stopping) {
                var batch = source.nextBatch();
                if (batch.size > 0) feed(batch);
                else break;
            }
        } catch (BItClosedException | BItCompletedException ignored) {
        } catch (Throwable t) {
            lock.lock();
            try {
                complete(t);
            } finally { lock.unlock(); }
        } finally {
            lock.lock();
            try {
                --activeSources;
                if (activeSources == 0) {
                    stopped.signalAll();
                    if (!ended)
                        complete(null);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /* --- --- overrides --- --- --- */

    @Override protected boolean complete(@Nullable Throwable error) {
        boolean mustCloseSources;
        lock.lock();
        try {
            if (ended)
                return false;
            mustCloseSources = activeSources > 0;
            stopping = true;
            super.complete(error); // unblocks drain() threads waiting in feed()
        } finally { lock.unlock(); }
        if (mustCloseSources) {//unblock drain() threads waiting in nextBatch()
            try {
                closeSources();
            } catch (Throwable t) {
                log.error("{}: {} from closeSources() on complete({}) with activeSources > 0",
                          this, t.getClass().getSimpleName(), error, t);
            }
        }
        return true;
    }

    @Override protected void waitReady() {
        if (!started && !ended) {
            started = true;
            activeSources = sources.size();
            if (activeSources == 0) {
                complete(null);
            } else {
                for (BIt<T> src : sources)
                    Thread.ofVirtual().name(this+"-drainer["+src+"]").start(() -> drain(src));
            }
        }
        super.waitReady();
    }

    @Override public boolean recycle(Batch<T> batch) {
        // Only offer batch to at most two sources. Only call super.recycle() if it is possible
        // that a source produces a batch with less than minBatch items before its last batch.
        int size = sources.size();
        if      (sources.get(recycler = (recycler+1) % size).recycle(batch)) return true;
        else if (sources.get(recycler = (recycler+1) % size).recycle(batch)) return true;
        else if (minWaitNs > 0) return super.recycle(batch);
        return false; // let batch be collected
    }

    @Override protected void cleanup() {
        stopping = true;
        super.cleanup(); // calls lock()/unlock(), which makes stopping = true visible to workers
        try {
            closeSources();
        } finally {
            lock.lock();
            try {
                while (started && activeSources > 0)
                    stopped.awaitUninterruptibly();
            } finally { lock.unlock(); }
        }
    }
}
