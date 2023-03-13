package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.NoSuchElementException;

import static java.lang.System.arraycopy;

/**
 * A {@link BIt} that acts as a queue holding at most one {@link Batch}.
 *
 * <p>The consumer thread consumes using {@link BIt} iteration methods, while the producer
 * thread feeds the queue using {@link ShortQueueBIt#copy(Batch)} or
 * {@link ShortQueueBIt#feed(Batch)} (if it wants to transfer ownership of the {@link Batch}).
 * After the producer calls {@link ShortQueueBIt#complete(Throwable)} and the
 * consumer consumes the last queue {@link Batch} (if any), the consumer will
 * receive the {@link Batch#terminal()} or the Throwable given by the producer.</p>
 *
 * @param <T> the item type
 */
public final class ShortQueueBIt<T> extends SPSCBIt<T> {
    private @Nullable Batch<T> ready, recycled;
    private int readyConsumed = 0;

    public ShortQueueBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public @This BIt<T> tempEager() { return this; }

    @Override public Batch<T> nextBatch() {
        lock();
        try {
            while (ready == null && !terminated)
                await();
            if (ready == null || readyConsumed > 0)
                return coldNextBatch();
            Batch<T> b = ready;
            ready = null;
            signal();
            return b;
        } finally { unlock(); }
    }


    private Batch<T> coldNextBatch() {
        if (readyConsumed > 0 && ready != null) {
            T[] a = ready.array;
            arraycopy(a, readyConsumed, a, 0, ready.size-=readyConsumed);
            readyConsumed = 0;
        } else if (ready == null) {
            checkError();
            return Batch.terminal();
        }
        Batch<T> b = ready;
        ready = null;
        signal();
        return b;
    }

    @Override public boolean recycle(Batch<T> batch) {
        if (recycled == null || recycled.array.length < batch.array.length) {
            recycled = batch;
            return true;
        }
        return false;
    }

    @Override public boolean hasNext() {
        lock();
        try {
            while (ready == null && !terminated) await();
            if    (ready == null)                checkError();
            return ready != null;
        } finally {
            unlock();
        }
    }

    @Override public T next() {
        lock();
        try {
            if (!hasNext()) throw new NoSuchElementException();
            assert ready != null;
            T item = ready.array[readyConsumed++];
            if (readyConsumed == ready.size) {
                ready = null;
                readyConsumed = 0;
                signal();
            }
            return item;
        } finally { unlock(); }
    }

    /* --- --- --- producer methods --- --- --- */

    @Override public void feed(Batch<T> batch) throws BItCompletedException {
        lock();
        try {
            while (ready != null && !terminated) await();
            if (terminated) throw mkCompleted();
            ready = batch;
            signal();
        } finally { unlock(); }
    }

    @Override public void feed(T item) throws BItCompletedException {
        lock();
        try {
            while (ready != null && ready.size >= maxBatch && !terminated)
                await(); // wait for capacity
            if (terminated) throw mkCompleted();
            if (ready == null) {
                if (recycled != null) {
                    (ready = recycled).clear();
                    recycled = null;
                } else {
                    ready = new Batch<>(rowType.rowClass, Math.max(10, minBatch));
                }
            }
            ready.add(item);
            signal();
        } finally { unlock(); }
    }

    public void copy(Batch<T> batch) {
        lock();
        try {
            while (ready != null && ready.size >= maxBatch && !terminated)
                await(); // wait for capacity
            if (terminated) {
                throw mkCompleted();
            } else if (ready != null) {
                ready.add(batch.array, 0, batch.size);
            } else if (recycled != null && recycled.array.length >= batch.size) {
                ready = recycled;
                recycled = null;
                arraycopy(batch.array, 0, ready.array, 0, ready.size = batch.size);
            } else {
                ready = new Batch<>(batch);
            }
            signal();
        } finally { unlock(); }
    }
}
