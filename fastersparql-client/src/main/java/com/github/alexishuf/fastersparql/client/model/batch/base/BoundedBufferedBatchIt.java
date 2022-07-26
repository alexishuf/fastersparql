package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;

import java.util.concurrent.locks.Condition;

public abstract class BoundedBufferedBatchIt<T> extends BufferedBatchIt<T> {
    private final Condition empty = lock.newCondition();
    private int maxReadyBatches = Integer.MAX_VALUE;
    private long readyElements = 0, maxReadyElements = Long.MAX_VALUE;

    public BoundedBufferedBatchIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    /* --- --- --- methods --- --- --- */

    /**
     * Set the maximum number of ready batches at any point.
     *
     * If this limit is reached, calls to {@link BoundedBufferedBatchIt#feed(Object)} will block
     * until enough batches are consumed to bring the number of ready buffered batches below
     * {@code max}.
     *
     * The default is {@link Integer#MAX_VALUE}, meaning there is no limit.
     *
     * @param max the maximum number of buffered ready batches.
     */
    public void maxReadyBatches(int max) {
        lock.lock();
        try {
            this.maxReadyBatches = max;
            empty.signalAll();
        } finally { lock.unlock(); }
    }

    /**
     * Set the maximum number of elements distributed across all buffered ready batches.
     *
     * If there are more than {@code max} elements ready,
     * {@link BoundedBufferedBatchIt#feed(Object)} will block until a batch gets consumed.
     *
     * The default is {@link Long#MAX_VALUE}, meaning there is no limit.
     *
     * @param max the maximum number of elements that can be ready.
     */
    public void maxReadyElements(long max) {
        lock.lock();
        try {
            this.maxReadyElements = max;
            empty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /* --- --- --- helpers --- --- --- */

    private void waitBufferLimits() {
        if (!ended) {
            //block if above buffer limits
            try {
                while (!ended && readyElements > maxReadyElements || ready.size() > maxReadyBatches)
                    empty.await();
            } catch (InterruptedException e) { complete(e); }
            ++readyElements;
        }
    }

    /* --- --- --- overrides --- --- --- */

    @Override protected void feed(T item) {
        lock.lock();
        try {
            waitBufferLimits();
            super.feed(item);
        } finally { lock.unlock(); }
    }

    @Override protected void feed(Batch<T> batch) {
        lock.lock();
        try {
            waitBufferLimits();
            super.feed(batch);
        } finally {
            lock.unlock();
        }
    }

    @Override protected Batch<T> fetch() {
        lock.lock();
        try {
            Batch<T> builder = super.fetch();
            if (builder != null)
                readyElements -= builder.size();
            return builder;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup() {
        lock.lock();
        try {
            empty.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
