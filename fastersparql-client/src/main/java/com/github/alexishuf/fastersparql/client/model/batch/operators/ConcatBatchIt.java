package com.github.alexishuf.fastersparql.client.model.batch.operators;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;
import com.github.alexishuf.fastersparql.client.model.batch.EmptyBatchIt;
import com.github.alexishuf.fastersparql.client.model.batch.base.AbstractBatchIt;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ConcatBatchIt<T> extends AbstractBatchIt<T> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Iterator<BatchIt<? extends T>> sourcesIt;
    private BatchIt<? extends T> source;

    public ConcatBatchIt(Iterator<BatchIt<? extends T>> sourcesIt, Class<T> elementClass,
                         String name) {
        super(elementClass, name == null ? "ConcatBatchIt-"+nextId.getAndIncrement() : name);
        this.sourcesIt = sourcesIt;
        if (!advance())
            this.source = new EmptyBatchIt<>(elementClass);
    }

    /* --- --- --- helper --- --- --- */

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean advance() {
        if (sourcesIt.hasNext()) {
            this.source = sourcesIt.next().minBatch(minBatch()).maxBatch(maxBatch())
                                          .minWait(minWait(NANOSECONDS), NANOSECONDS);
            return true;
        }
        return false;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public Batch<T> nextBatch() {
        checkOpen();
        while (true) {
            Batch<? extends T> batch = source.nextBatch();
            if (batch.size > 0) {
                //noinspection unchecked
                return (Batch<T>) batch;
            } else if (!advance()) {
                return new Batch<>(elementClass);
            }
        }
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        checkOpen();
        while (true) {
            int size = source.nextBatch(destination);
            if (size > 0)
                return size;
            else if (!advance())
                return 0;
        }
    }

    @Override public boolean hasNext() {
        while (true) {
            if      (source.hasNext()) return true;
            else if (!advance())       return false;
        }
    }

    @Override public T next() {
        if (!hasNext()) throw new NoSuchElementException(this+" at end");
        return source.next();
    }

    @Override protected void cleanup() {
        source.close();
        while (sourcesIt.hasNext())
            sourcesIt.next().close();
    }
}
