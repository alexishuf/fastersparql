package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.BoundedBufferedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Thread.ofVirtual;

public class MergeBIt<T> extends BoundedBufferedBIt<T> {
    private static final Logger log = LoggerFactory.getLogger(MergeBIt.class);

    private final Condition stopped = lock.newCondition();
    protected final List<? extends BIt<T>> sources;
    private int activeSources;
    private boolean started;
    private long recycling = 0L;

    public MergeBIt(Collection<? extends BIt<T>> sources, RowType<T> rowType,
                    Vars vars) {
        super(rowType, vars);
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

    protected void process(Batch<T> batch, int sourceIdx,
                           RowType<T>.@Nullable Merger projector) {
        if (projector != null) projector.projectInPlace(batch);
        feed(batch);
    }

    private void drain(BIt<T> source, int i) {
        var projector = vars.equals(source.vars()) ? null
                      : Objects.requireNonNull(rowType).projector(vars, source.vars());
        long recyclingBit = 1L << i;
        for (var b = source.nextBatch(); b.size > 0; b = source.nextBatch()) {
            process(b, i, projector);
            recycling |= recyclingBit; // see recycle() for rationale
        }
    }

    private void drainTask(int i) {
        try (var source = sources.get(i)) {
            drain(source, i);
        } catch (Throwable t) {
            lock.lock();
            try {
                if (this.error == null || (t instanceof BItReadClosedException))
                    complete(t); // ignore BItClosedException caused by closeSources().
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

    @Override protected void updateBatchCapacity() {
        super.updateBatchCapacity();
        for (BIt<T> source : sources)
            source.minBatch(minBatch).maxBatch(maxBatch);
    }

    @Override public BIt<T> minWait(long time, TimeUnit unit) {
        super.minWait(time, unit);
        for (BIt<T> source : sources) {
            // maxWait must only be enforced when aggregating multiple Batches smaller than minBatch
            // enforcing maxWait at each source would lead to waiting when a batch could be built
            // by combining two incomplete batches.
            source.minWait(minWaitNs, TimeUnit.NANOSECONDS)
                  .maxWait(minWaitNs, TimeUnit.NANOSECONDS);
        }
        return this;
    }

    @Override public @This BIt<T> tempEager() {
        for (BIt<T> source : sources)
            source.tempEager();
        return this;
    }

    @Override protected void complete(@Nullable Throwable error) {
        boolean mustCloseSources;
        lock.lock();
        try {
            if (ended)
                return;
            mustCloseSources = activeSources > 0;
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
    }

    @Override protected void waitReady() {
        if (!started && !ended) {
            started = true;
            activeSources = sources.size();
            if (activeSources == 0) {
                complete(null);
            } else {
                String parent = super.toString();
                var name = new StringBuilder(parent.length()+9+4);
                name.append(parent).append("-drainer-");
                int prefixLength = name.length();
                for (int i = 0, n = sources.size(); i < n; i++) {
                    final int idx = i;
                    name.append(idx);
                    ofVirtual().name(name.append(idx).toString()).start(() -> drainTask(idx));
                    name.setLength(prefixLength);
                }
            }
        }
        super.waitReady();
    }

    @Override public boolean recycle(Batch<T> batch) {
        // everytime the i-th source feed()s the MergeBIt, it sets the i-th bit in recycling,
        // marking it as "accepting recycle() calls".
        //
        // If there are more than 64 sources, a bit in recycling will represent many sources.
        // In that case recycle() is tried on the colliding sources until one accepts. However,
        // aside from tests and uncommon scenarios, n < 64 and recycle() is called only once.
        int n = sources.size(), i = numberOfTrailingZeros(recycling);
        while (i < n && !sources.get(i).recycle(batch)) i += 64;
        return i < n;
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause); // calls lock()/unlock(), which makes stopping = true visible to workers
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

    @Override public String toString() { return toStringWithOperands(sources); }
}
