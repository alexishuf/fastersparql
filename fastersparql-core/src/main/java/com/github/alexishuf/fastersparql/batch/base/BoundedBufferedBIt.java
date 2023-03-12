package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.BoundedBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.concurrent.locks.Condition;

/**
 * Implements a limit to how many ready {@link Batch}es can be held. If such upper bound is
 * reached, callers to the {@code feed()} methods are blocked.
 */
public abstract class BoundedBufferedBIt<T> extends BufferedBIt<T> implements BoundedBIt<T> {
    protected final Condition empty = lock.newCondition();
    protected int maxReadyBatches = Integer.MAX_VALUE;
    protected long readyItems = 0, maxReadyItems = Long.MAX_VALUE;

    public BoundedBufferedBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    /* --- --- --- methods --- --- --- */

    @Override public @This BoundedBufferedBIt<T> maxReadyBatches(int max) {
        lock.lock();
        try {
            this.maxReadyBatches = max;
            empty.signalAll();
        } finally { lock.unlock(); }
        return this;
    }

    @Override public int maxReadyBatches() { return maxReadyBatches; }

    @Override public @This BoundedBufferedBIt<T> maxReadyItems(long max) {
        lock.lock();
        try {
            this.maxReadyItems = max;
            empty.signalAll();
        } finally {
            lock.unlock();
        }
        return this;
    }

    @Override public long maxReadyItems() { return maxReadyItems; }

    /** Blocks until new at least one batch with one item can be buffered. */
    protected void waitForCapacity() {
        while (!ended && (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches))
            empty.awaitUninterruptibly();
    }

    /* --- --- --- overrides --- --- --- */

    @Override protected void feed(T item) {
        lock.lock();
        try {
            //block if above buffer limits
            if (!ended && (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches))
                waitForCapacity();
            super.feed(item);
            ++readyItems;
        } finally { lock.unlock(); }
    }

    @Override protected void feed(Batch<T> batch) {
        lock.lock();
        try {
            if (!ended && (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches))
                waitForCapacity();
            super.feed(batch);
            readyItems += batch.size;
        } finally {
            lock.unlock();
        }
    }

    @Override protected @Nullable Batch<T> fetch() {
        lock.lock();
        try {
            Batch<T> batch = super.fetch();
            if (batch != null) {
                readyItems -= batch.size();
                empty.signalAll();
            }
            return batch;
        } finally { lock.unlock(); }
    }

    @Override public T next() {
        lock.lock();
        try {
            boolean noFetch = !ready.isEmpty();
            T item = super.next();
            if (noFetch) {
                readyItems -= 1;
                empty.signal();
            }
            return item;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (cause != null) {
            lock.lock();
            try {
                empty.signalAll();
            } finally { lock.unlock(); }
        }
    }
}
