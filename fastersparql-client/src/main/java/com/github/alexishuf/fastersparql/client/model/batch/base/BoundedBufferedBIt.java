package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.locks.Condition;

/**
 * Implements a limit to how many ready {@link Batch}es can be held. If such upper bound is
 * reached, callers to the {@code feed()} methods are blocked.
 */
public abstract class BoundedBufferedBIt<T> extends BufferedBIt<T> {
    private final Condition empty = lock.newCondition();
    private int maxReadyBatches = Integer.MAX_VALUE;
    private long readyElements = 0, maxReadyElements = Long.MAX_VALUE;

    public BoundedBufferedBIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    /* --- --- --- methods --- --- --- */

    /**
     * Set the maximum number of ready batches at any point.
     *
     * <p>If this limit is reached, calls to {@link BoundedBufferedBIt#feed(Object)} will block
     * until enough batches are consumed to bring the number of ready buffered batches below
     * {@code max}.</p>
     *
     * The default is {@link Integer#MAX_VALUE}, meaning there is no limit.
     *
     * @param max the maximum number of buffered ready batches.
     */
    public @This BoundedBufferedBIt<T> maxReadyBatches(int max) {
        lock.lock();
        try {
            this.maxReadyBatches = max;
            empty.signalAll();
        } finally { lock.unlock(); }
        return this;
    }

    /**
     * Set the maximum number of elements distributed across all buffered ready batches.
     *
     * <p>If there are more than {@code max} elements ready,
     * {@link BoundedBufferedBIt#feed(Object)} will block until a batch gets consumed.</p>
     *
     * The default is {@link Long#MAX_VALUE}, meaning there is no limit.
     *
     * @param max the maximum number of elements that can be ready.
     */
    public @This BoundedBufferedBIt<T> maxReadyItems(long max) {
        lock.lock();
        try {
            this.maxReadyElements = max;
            empty.signalAll();
        } finally {
            lock.unlock();
        }
        return this;
    }

    /* --- --- --- overrides --- --- --- */

    @Override protected void feed(T item) {
        lock.lock();
        try {
            //block if above buffer limits
            while (!ended && (readyElements > maxReadyElements || ready.size() > maxReadyBatches))
                empty.awaitUninterruptibly();
            super.feed(item);
            ++readyElements;
        } finally { lock.unlock(); }
    }

    @Override protected void feed(Batch<T> batch) {
        lock.lock();
        try {
            //block if above buffer limits
            while (!ended && (readyElements > maxReadyElements || ready.size() > maxReadyBatches))
                empty.awaitUninterruptibly();
            super.feed(batch);
            readyElements += batch.size;
        } finally {
            lock.unlock();
        }
    }

    @Override protected @Nullable Batch<T> fetch() {
        lock.lock();
        try {
            Batch<T> batch = super.fetch();
            if (batch != null) {
                readyElements -= batch.size();
                empty.signalAll();
            }
            return batch;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup() {
        lock.lock();
        try {
            super.cleanup();
            empty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
