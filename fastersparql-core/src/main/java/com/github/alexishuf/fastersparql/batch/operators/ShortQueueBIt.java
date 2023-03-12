package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.System.arraycopy;

/**
 * A {@link BIt} that acts as a queue holding at most one {@link Batch}.
 *
 * <p>The consumer thread consumes using {@link BIt} iteration methods, while the producer
 * thread feeds the queue using {@link ShortQueueBIt#copy(Batch)} or
 * {@link ShortQueueBIt#put(Batch)} (if it wants to transfer ownership of the {@link Batch}).
 * After the producer calls {@link ShortQueueBIt#complete(Throwable)} and the
 * consumer consumes the last queue {@link Batch} (if any), the consumer will
 * receive the {@link Batch#terminal()} or the Throwable given by the producer.</p>
 *
 * @param <T> the item type
 */
public final class ShortQueueBIt<T> extends AbstractBIt<T> {
    private final Lock lock = new ReentrantLock();
    private final Condition hasReady = lock.newCondition();
    private @Nullable Batch<T> ready, recycled;
    private int readyConsumed = 0;
    private boolean ended;

    public ShortQueueBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    /* --- --- --- consumer methods --- --- --- */

    @Override public @This BIt<T> tempEager() { return this; }

    @Override public Batch<T> nextBatch() {
        lock.lock();
        try {
            while (ready == null && !ended)
                hasReady.awaitUninterruptibly();
            if (ready == null || readyConsumed > 0)
                return infrequentNextBatch();
            Batch<T> b = ready;
            ready = null;
            hasReady.signal();
            return b;
        } finally { lock.unlock(); }
    }


    private Batch<T> infrequentNextBatch() {
        if (readyConsumed > 0 && ready != null) {
            T[] a = ready.array;
            arraycopy(a, readyConsumed, a, 0, ready.size-=readyConsumed);
            readyConsumed = 0;
        } else if (ready == null) {
            assert ended;
            checkError();
            return Batch.terminal();
        }
        Batch<T> b = ready;
        ready = null;
        hasReady.signal();
        return b;
    }

    @Override public boolean recycle(Batch<T> batch) {
        lock.lock();
        try {
            if (recycled == null) {
                recycled = batch;
                return true;
            } else if (recycled.array.length < batch.array.length) {
                recycled = batch;
                return true;
            } else {
                return false; //our recycled is larger than offered batch
            }
        } finally { lock.unlock(); }
    }

    @Override public boolean hasNext() {
        lock.lock();
        try {
            while (ready == null && !ended) hasReady.awaitUninterruptibly();
            if (ready == null)
                checkError();
            return ready != null;
        } finally {
            lock.unlock();
        }
    }

    @Override public T next() {
        lock.lock();
        try {
            if (!hasNext()) throw new NoSuchElementException();
            assert ready != null;
            T item = ready.array[readyConsumed++];
            if (readyConsumed == ready.size) {
                ready = null;
                readyConsumed = 0;
                hasReady.signal();
            }
            return item;
        } finally { lock.unlock(); }
    }

    /* --- --- --- producer methods --- --- --- */

    public void complete(@Nullable Throwable error) {
        lock.lock();
        try {
            onTermination(error);
            if (ended) return;
            ended = true;
            hasReady.signalAll();
        } finally { lock.unlock(); }
    }

    public void put(Batch<T> batch) {
        lock.lock();
        try {
            if (error != null) checkError();
            while (ready != null)
                hasReady.awaitUninterruptibly();
            ready = batch;
            hasReady.signal();
        } finally { lock.unlock(); }
    }

    public void copy(Batch<T> batch) {
        lock.lock();
        try {
            if (error != null)
                checkError();
            while (ready != null)
                 hasReady.awaitUninterruptibly();
            if (recycled != null && recycled.array.length >= batch.size) {
                ready = recycled;
                arraycopy(batch.array, 0, ready.array, 0, ready.size = batch.size);
                recycled = null;
            } else {
                ready = new Batch<>(batch);
            }
            hasReady.signal();
        } finally { lock.unlock(); }
    }
}
