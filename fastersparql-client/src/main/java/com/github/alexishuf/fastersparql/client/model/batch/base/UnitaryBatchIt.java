package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.BatchIt;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;

/** Implements {@link BatchIt} methods around {@code hasNext()/next()}. */
public abstract class UnitaryBatchIt<T> extends AbstractBatchIt<T> {
    public UnitaryBatchIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    @Override public Batch<T> nextBatch() {
        long minTs = nanoTime() + minWait(TimeUnit.NANOSECONDS);
        Batch<T> batch = new Batch<>(elementClass(), minBatch, maxBatch);
        while (hasNext() && batch.size < maxBatch && (batch.size < minBatch && nanoTime() < minTs))
            batch.add(next());
        return batch;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        long minTs = nanoTime() + minWait(TimeUnit.NANOSECONDS);
        int size = 0;
        while (hasNext() && size < maxBatch && (size < minBatch || nanoTime() < minTs)) {
            destination.add(next());
            ++size;
        }
        return size;
    }
}
