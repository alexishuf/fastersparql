package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BufferedBatchIt<T> extends AbstractBatchIt<T> {
    private final Logger log = LoggerFactory.getLogger(BufferedBatchIt.class);

    protected final ReentrantLock lock = new ReentrantLock();
    protected final Condition full = lock.newCondition();
    protected final ArrayDeque<Batch<T>> ready = new ArrayDeque<>();
    private Batch.Builder<T> filling = null, recycled = null;
    protected boolean ended, warnedPostEndFeed;
    protected @MonotonicNonNull Throwable error;

    /* --- --- --- constructors --- --- --- */

    public BufferedBatchIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    /* --- --- --- helper methods --- --- --- */

    protected Batch<T> fetch() {
        checkOpen();
        lock.lock();
        try {
            while (!ended && error == null && ready.isEmpty())
                full.awaitUninterruptibly();
            if      (!ready.isEmpty()) return ready.remove();
            else if (error != null   ) throw new RuntimeExecutionException(error);
            return null;
        } finally { lock.unlock(); }
    }

    protected void complete(@Nullable Throwable error) {
        lock.lock();
        try {
            if (this.ended) {
                if (error == null) {
                    log.trace("{}.end(null) ignored: previous end({})", this, this.error);
                } else if (this.error == null) {
                    log.warn("{}.end({}) ignored: previous end(null)", this, error);
                } else {
                    log.debug("{}.end({}) ignored: previous end({})", this, error, this.error);
                }
            } else {
                log.debug("{}.complete({})", this, error);
                this.ended = true;
                this.error = error;
                full.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    protected void feed(T item) {
        lock.lock();
        try {
            if (ended) {
                if (!warnedPostEndFeed) {
                    warnedPostEndFeed = true;
                    log.warn("{}.feed({}) after ended. Silently discarding", this, item);
                }
            } else {
                // get/reset/create current filling Batch
                if (filling == null) {
                    if (recycled == null) {
                        filling = new Batch.Builder<>(this);
                    } else {
                        filling = recycled;
                        recycled = null;
                        filling.update(this).clear();
                    }
                }
                // add and maybe queue the ready batch
                filling.add(item);
                if (filling.ready()) {
                    ready.add(filling);
                    filling = null;
                    full.signal();
                }
            }
        } finally { lock.unlock(); }
    }

    protected void feed(Batch<T> batch) {
        if (batch.size < minBatch()) {
            T[] array = batch.array;
            for (int i = 0, size = batch.size; i < size; i++)
                feed(array[i]);
        } else {
            ready.add(batch);
            full.signal();
        }
    }

    /* --- --- --- implementations --- --- --- */

    @Override public final Batch<T> nextBatch() {
        Batch<T> batch = fetch();
        if (batch == null) {
            return new Batch<>(elementClass);
        } else if (batch.needsTrimming() && batch instanceof Batch.Builder<T> builder) {
            lock.lock();
            try {
                if (recycled == null || recycled.array.length < builder.array.length) {
                    Batch<T> trimmed = builder.trimmedCopy();
                    recycled = builder;
                    return trimmed;
                }
            } finally { lock.unlock(); }
        }
        // no trimming, not a builder or recycled was bigger
        return batch;
    }

    @Override public final int nextBatch(Collection<? super T> destination) {
        Batch<T> batch = fetch();
        if (batch == null) {
            return 0;
        } else {
            int size = batch.drainTo(destination);
            if (batch instanceof Batch.Builder<T> builder) {
                lock.lock();
                try {
                    if (recycled == null || builder.array.length > recycled.array.length)
                        recycled = builder;
                } finally { lock.unlock(); }
            }
            return size;
        }
    }

    @Override public final boolean hasNext() {
        return fetch() != null;
    }

    @Override public final T next() {
        lock.lock();
        try {
            Batch<T> batch = fetch();
            if (batch == null || batch.size == 0)
                throw new NoSuchElementException();
            T item = batch.array[0];
            if (--batch.size > 0) {
                System.arraycopy(batch.array, 1, batch.array, 0, --batch.size);
                ready.addFirst(batch);
            }
            return item;
        } finally { lock.unlock(); }
    }
}
