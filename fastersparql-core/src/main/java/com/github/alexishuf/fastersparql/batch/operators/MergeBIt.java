package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.SPSCBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.util.ExceptionCondenser;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Thread.ofVirtual;

public class MergeBIt<T> extends SPSCBufferedBIt<T> {
    private static final Logger log = LoggerFactory.getLogger(MergeBIt.class);

    protected final List<? extends BIt<T>> sources;
    protected final ReentrantLock feedLock = new ReentrantLock();
    private boolean started = false;
    private int activeSources = 0;
    private long recycling = 0L;

    public MergeBIt(Collection<? extends BIt<T>> sources, RowType<T> rowType,
                            Vars vars) {
        super(rowType, vars);
        //noinspection unchecked
        int n = (this.sources = sources instanceof List<?> list
                ? (List<? extends BIt<T>>) list : new ArrayList<>(sources)).size();
        //noinspection resource
        maxReadyBatches(n);
    }

    /* --- --- helper methods --- --- --- */

    private void start() {
        started = true;
        int n = activeSources = sources.size();
        if (n == 0) {
            complete(null);
        } else {
            String parent = super.toString();
            var name = new StringBuilder(parent.length()+9+4);
            name.append(parent).append("-drainer-");
            int prefixLength = name.length();
            for (int i = 0; i < n; i++) {
                final int idx = i;
                name.append(idx);
                ofVirtual().name(name.append(idx).toString()).start(() -> drainTask(idx));
                name.setLength(prefixLength);
            }
        }
    }

    protected void process(Batch<T> batch, int sourceIdx,
                           RowType<T>.@Nullable Merger projector) {
        if (projector != null) projector.projectInPlace(batch);
        feedLock.lock();
        try {
            feed(batch);
        } finally { feedLock.unlock(); }
    }

    private void drainTask(int i) {
        try (var source = sources.get(i)) {
            var projector = vars.equals(source.vars()) ? null
                    : Objects.requireNonNull(rowType).projector(vars, source.vars());
            long recyclingBit = 1L << i;
            for (var b = source.nextBatch(); b.size > 0; b = source.nextBatch()) {
                process(b, i, projector);
                recycling |= recyclingBit; // see recycle() for rationale
            }
        } catch (Throwable t) {
            lock();
            try {
                if (this.error == null || (t instanceof BItReadClosedException))
                    complete(t); // ignore BItClosedException caused by closeSources().
            } finally { unlock(); }
        } finally {
            lock();
            try {
                --activeSources;
                if (activeSources == 0) {
                    if (!terminated)
                        complete(null);
                    signal();
                }
            } finally {
                unlock();
            }
        }
    }

    /* --- --- overrides --- --- --- */

    @Override protected void updatedBatchConstraints() {
        super.updatedBatchConstraints();
        for (BIt<T> source : sources)
            // maxWait must only be enforced when aggregating multiple Batches smaller than minBatch
            // enforcing maxWait at each source would lead to waiting when a batch could be built
            // by combining two incomplete batches.
            source.minBatch(minBatch).maxBatch(maxBatch)
                    .minWait(minWaitNs, TimeUnit.NANOSECONDS)
                    .maxWait(minWaitNs, TimeUnit.NANOSECONDS);
    }

    @Override public @This BIt<T> tempEager() {
        for (BIt<T> source : sources)
            source.tempEager();
        return this;
    }

    @Override public void complete(@Nullable Throwable error) {
        boolean mustCloseSources;
        lock();
        try {
            if (terminated) return;
            mustCloseSources = activeSources > 0;
            super.complete(error); // unblocks drain() threads waiting in feed()
        } finally { unlock(); }
        if (mustCloseSources) {//unblock drain() threads waiting in nextBatch()
            try {
                ExceptionCondenser.closeAll(sources);
            } catch (Throwable t) {
                log.error("{}: {} from closeSources() on complete({}) with activeSources > 0",
                        this, t.getClass().getSimpleName(), error, t);
            }
        }
    }

    @Override protected void  waitReady() {
        if (!started && !terminated) start();
        super.waitReady();
    }

    @Override public boolean recycle(Batch<T> batch) {
        // Everytime the i-th source feed()s the MergeBIt, it sets the (i&63)-th bit in recycling,
        // Here, we offer batch will to the sources that share the first bit set in recycling.
        int n = sources.size(), i = numberOfTrailingZeros(recycling);
        while (i < n && !sources.get(i).recycle(batch)) i += 64;
        return i < n;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause); // calls lock()/unlock(), which makes stopping = true visible to workers
        if (activeSources > 0)
            ExceptionCondenser.closeAll(sources);
    }

    @Override public void close() {
        super.close();
        lock();
        try {
            while (activeSources > 0) await();
        } finally { unlock(); }
    }

    @Override public String toString() { return toStringWithOperands(sources); }
}
