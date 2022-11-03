package com.github.alexishuf.fastersparql.batch.operators;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedException;
import com.github.alexishuf.fastersparql.batch.BItIllegalStateException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
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
    private static final Logger log = LoggerFactory.getLogger(ShortQueueBIt.class);

    private final Lock lock = new ReentrantLock();
    private final Condition hasReady = lock.newCondition();
    private @Nullable Batch<T> ready, recycled0, recycled1;
    private int readyConsumed = 0;
    private boolean ended, errorRethrown;
    private @Nullable Throwable error;

    public ShortQueueBIt(Class<? super T> elementClass, Vars vars) {
        super(elementClass, vars);
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


    private void checkError() {
        if (error != null && !errorRethrown) {
            errorRethrown = true;
            if (error instanceof RuntimeException re) throw re;
            else if (error instanceof Error e) throw e;
            else throw new RuntimeException(error);
        }
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
            if (recycled0 == null) {
                recycled0 = batch;
            } else if (recycled1 == null) {
                recycled1 = batch;
            } else {
                int offer = batch.array.length;
                if (recycled0.array.length < offer) recycled0 = batch;
                else if (recycled1.array.length < offer) recycled1 = batch;
                else return false;
            }
            return true;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup(boolean interrupted) { }

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
            if (ended) {
                boolean bad = this.error == null && error != null
                           && !(error instanceof BItIllegalStateException);
                if (bad) {
                    log.info("{}.complete({}) ignored: previous complete(null)",
                             this, error.getClass().getSimpleName(), error);
                }
            } else {
                ended = true;
                this.error = error;
                hasReady.signal();
            }
        } finally { lock.unlock(); }
    }

    public void put(Batch<T> batch) {
        lock.lock();
        try {
            if (closed)
                throw new BItClosedException(this);
            while (ready != null)
                hasReady.awaitUninterruptibly();
            ready = batch;
            hasReady.signal();
        } finally { lock.unlock(); }
    }

    public void copy(Batch<T> batch) {
        lock.lock();
        try {
            if (closed)
                throw new BItClosedException(this);
            while (ready != null)
                 hasReady.awaitUninterruptibly();
            if (recycled0 != null) {
                ready = recycled0;
                recycled0 = null;
            } else if (recycled1 != null) {
                ready = recycled1;
                recycled1 = null;
            }
            if (ready == null) {
                ready = new Batch<>(batch);
            } else {
                if (ready.array.length < batch.size)  //noinspection unchecked
                    ready.array = (T[]) Array.newInstance(elementClass, batch.size);
                arraycopy(batch.array, 0, ready.array, 0, ready.size = batch.size);
            }
            hasReady.signal();
        } finally { lock.unlock(); }
    }
}
