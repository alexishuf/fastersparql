package com.github.alexishuf.fastersparql.client.model.batch.operators;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConcatBIt<T> extends AbstractBIt<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Iterator<? extends BIt<T>> sourcesIt;
    private BIt<T> source;

    public ConcatBIt(Iterator<? extends BIt<T>> sourcesIt, Class<T> elementClass) {
        this(sourcesIt, elementClass, null);
    }

    public ConcatBIt(Iterator<? extends BIt<T>> sourcesIt, Class<T> elementClass,
                     @Nullable String name) {
        super(elementClass, name == null ? "ConcatBatchIt-"+nextId.getAndIncrement() : name);
        this.sourcesIt = sourcesIt;
        if (!hasNextSource())
            this.source = new EmptyBIt<>(elementClass);
    }
    
    /* --- --- --- helper --- --- --- */


    private boolean hasNextSource() {
        if (sourcesIt.hasNext()) {
            this.source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                          .minWait(minWait(NANOSECONDS), NANOSECONDS);
            return true;
        }
        return false;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public Batch<T> nextBatch() {
        do {
            Batch<T> batch = source.nextBatch();
            if (batch.size > 0)
                return batch;
        } while (hasNextSource());
        onExhausted();
        return new Batch<>(elementClass, 0);
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        do {
            int size = source.nextBatch(destination);
            if (size > 0) return size;
        } while (hasNextSource());
        onExhausted();
        return 0;
    }


    @Override public boolean hasNext() {
        do {
            if (source.hasNext()) return true;
        } while (hasNextSource());
        onExhausted();
        return false;
    }

    @Override public T next() { return source.next(); }
    @Override public boolean recycle(Batch<T> batch) { return source.recycle(batch); }

    @Override protected void cleanup() {
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
}
