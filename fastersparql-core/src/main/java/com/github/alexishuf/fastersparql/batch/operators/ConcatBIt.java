package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Collection;
import java.util.Iterator;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConcatBIt<T> extends AbstractBIt<T> {
    private final Collection<? extends BIt<T>> sources;
    private final Iterator<? extends BIt<T>> sourcesIt;
    protected BIt<T> source;

    public ConcatBIt(Collection<? extends BIt<T>> sources, RowType<T> rowType,
                     Vars vars) {
        super(rowType, vars);
        this.source = new EmptyBIt<>(rowType, vars);
        this.sourcesIt = (this.sources = sources).iterator();
    }
    
    /* --- --- --- helper --- --- --- */

    protected boolean nextSource() {
        if (sourcesIt.hasNext()) {
            source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                     .minWait(minWait(NANOSECONDS), NANOSECONDS);
            return true;
        }
        return false;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @This BIt<T> tempEager() {
        source.tempEager();
        return this;
    }

    @Override public Batch<T> nextBatch() {
        try {
            do {
                Batch<T> batch = source.nextBatch();
                if (batch.size > 0)
                    return batch;
            } while (nextSource());
            onTermination(null);
            return Batch.terminal();
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
    }

    @Override public boolean hasNext() {
        try {
            do {
                if (source.hasNext()) return true;
            } while (nextSource());
            onTermination(null);
            return false;
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
    }

    @Override public T next() {
        try {
            return source.next();
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
    }
    @Override public boolean recycle(Batch<T> batch) { return source.recycle(batch); }

    @Override protected void cleanup(Throwable cause) {
        if (cause == null)
            return; // source.close() is a no-op and sourcesIt.hasNext() == false
        Throwable error = null;
        try {
            source.close();
        } catch (Throwable t) { error = t; }
        while (sourcesIt.hasNext()) {
            try {
                sourcesIt.next().close();
            } catch (Throwable t) {
                if (error == null) error = t;
                else               error.addSuppressed(t);
            }
        }
        if      (error instanceof RuntimeException re) throw re;
        else if (error instanceof Error             e) throw e;
    }

    @Override public String toString() { return toStringWithOperands(sources); }
}
