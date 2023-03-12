package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link BIt} implementation of {@link Stream#flatMap(Function)}: For every value {@code x}
 * from an input {@link BIt}, {@link FlatMapBIt#map(Object)} maps it to a {@link BIt}
 * that is then consumed by callers of {@link BIt#nextBatch()}/{@link BIt#next()} and alike on the
 * {@link FlatMapBIt}.
 */
public abstract class FlatMapBIt<I, O> extends AbstractBIt<O> {
    protected final BIt<I> original;
    private final BIt<O> empty;
    private BIt<O> mapped;
    private boolean pendingEager;

    public FlatMapBIt(RowType<O> rowType, BIt<I> original, Vars vars) {
        super(rowType, vars);
        (this.original = original).minBatch(1).minWait(0, NANOSECONDS)
                                  .maxWait(0, NANOSECONDS);
        this.mapped = this.empty = new EmptyBIt<>(rowType, vars);
    }

    protected abstract BIt<O> map(I input);

    /* --- --- --- delegate batching parameters --- --- --- */

    @Override public BIt<O> minWait(long time, TimeUnit unit) {
        super.minWait(time, unit);
        if (mapped != empty)
            mapped.minWait(time, unit);
        return this;
    }

    @Override public BIt<O> maxWait(long time, TimeUnit unit) {
        super.maxWait(time, unit);
        if (mapped != empty)
            mapped.maxWait(time, unit);
        return this;
    }

    @Override public BIt<O> minBatch(int size) {
        super.minBatch(size);
        if (mapped != empty)
            mapped.minBatch(size);
        return this;
    }

    @Override public BIt<O> maxBatch(int size) {
        super.maxBatch(size);
        if (mapped != empty)
            mapped.maxBatch(size);
        return this;
    }

    @Override public @This BIt<O> tempEager() {
        if (mapped == empty) pendingEager = true;
        else                 mapped.tempEager();
        return this;
    }

    /* --- --- --- state management --- --- --- */

    @Override protected void cleanup(@Nullable Throwable cause) {
        if (cause != null) {
            original.close();
            if (mapped != empty) { mapped.close(); mapped = empty; }
        }
    }

    /* --- --- --- iteration --- --- --- */

    /** Get the next non-empty {@code map(original.next())} or set mapped to {@code empty} */
    private void nextMapped() {
        while (original.hasNext()) {
            var it = map(original.next());
            it.minBatch(minBatch).maxBatch(maxBatch)
              .minWait(minWaitNs, NANOSECONDS)
              .maxWait(maxWaitNs, NANOSECONDS);
            if (pendingEager)
                it.tempEager();
            if (it.hasNext()) {
                mapped = it;
                if (pendingEager)
                    pendingEager = false;
                return;
            }
        }
        mapped = empty;
    }

    @Override public Batch<O> nextBatch() {
        while (true) {
            Batch<O> b = mapped.nextBatch();
            if (b.size == 0 && original.hasNext()) nextMapped();
            else                                   return b;
        }
    }

    @Override public boolean hasNext() {
        boolean has = mapped.hasNext();
        while (!has && original.hasNext()) {
            nextMapped();
            has = mapped.hasNext();
        }
        return has;
    }

    @Override public O next() {
        if (!hasNext()) throw new NoSuchElementException();
        return mapped.next();
    }

    @Override public boolean recycle(Batch<O> batch) {
        return mapped != null && mapped.recycle(batch);
    }

    @Override public String toString() {
        return toStringWithOperands(List.of(original));
    }
}
