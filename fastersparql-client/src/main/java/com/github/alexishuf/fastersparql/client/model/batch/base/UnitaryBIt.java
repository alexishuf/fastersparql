package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;

import java.util.Collection;

/** Implements {@link BIt} methods around {@code hasNext()/next()}. */
public abstract class UnitaryBIt<T> extends AbstractBIt<T> {
    public UnitaryBIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }
    private @Nullable Throwable pendingError;
    private @Nullable Batch<T> recycled;
    private int capacity = 10;

    /* --- --- --- helpers --- --- --- */

    private boolean shouldFetch(int size, long start) {
        if (ready(size, start))
            return false;
        try {
            return hasNext();
        } catch (Throwable t) {
            if (size == 0)
                throw t; // batch is empty, thus throwing will not cause items to be lost.
            pendingError = t;
            return false;
        }
    }

    @RequiresNonNull("pendingError")
    private void throwPending() {
        RuntimeException e = pendingError instanceof RuntimeException r
                           ? r : new RuntimeException(pendingError);
        pendingError = null;
        throw e;
    }

    private BIt<T> updateCapacity() {
        if (maxBatch < 22) {
            capacity = maxBatch <= 10 ? 10 : (maxBatch < 16 ? 15 : 22);
        } else {
            int capacity = 10;
            while (capacity < minBatch) capacity += capacity>>1;
            this.capacity = capacity;
        }
        return this;
    }

    /* --- --- --- overrides --- --- --- */

    @Override public BIt<T> minBatch(int size) {
        super.minBatch(size);
        return updateCapacity();
    }

    @Override public BIt<T> maxBatch(int size) {
        super.maxBatch(size);
        return updateCapacity();
    }

    /* --- --- --- implementations --- --- --- */

    @Override public Batch<T> nextBatch() {
        if (pendingError != null)
            throwPending();
        Batch<T> batch;
        if (recycled == null) {
            batch = new Batch<>(elementClass, capacity);
        } else {
            batch = recycled;
            batch.clear();
            recycled = null;
        }
        long start = needsStartTime ? System.nanoTime() : ORIGIN_TIMESTAMP;
        while (shouldFetch(batch.size, start))
            batch.add(next());
        if (batch.size == 0)
            onExhausted();
        return batch;
    }

    @Override public int nextBatch(Collection<? super T> destination) {
        if (pendingError != null)
            throwPending();
        long start = needsStartTime ? System.nanoTime() : ORIGIN_TIMESTAMP;
        int size = 0;
        while (shouldFetch(size, start)) {
            destination.add(next());
            ++size;
        }
        if (size == 0)
            onExhausted();
        return size;
    }

    @Override public boolean recycle(Batch<T> batch) {
        if (recycled == null && batch.array.length >= capacity) {
            recycled = batch;
            return true;
        }
        return false;
    }
}
