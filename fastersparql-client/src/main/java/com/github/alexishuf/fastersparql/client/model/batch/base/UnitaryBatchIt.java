package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public abstract class UnitaryBatchIt<T> extends AbstractBatchIt<T> {
    public UnitaryBatchIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    @Override public Batch<T> nextBatch() {
        Batch.Builder<T> builder = new Batch.Builder<>(this);
        while (hasNext() & !builder.ready())
            builder.add(next());
        return builder;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        long minTs = System.nanoTime() + minWait(TimeUnit.NANOSECONDS);
        int size = 0;
        while (hasNext() && (size < minBatch || System.nanoTime() < minTs) && size < maxBatch) {
            destination.add(next());
            ++size;
        }
        return size;
    }
}
