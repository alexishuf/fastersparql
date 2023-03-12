package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItIllegalStateException;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BIt} that can be asynchronously fed with batches or items, which are then
 * buffered into a ring of {@link Batch}es to be delivered to consumers of this {@link BIt}.
 */
public abstract class BufferedBIt<T> extends AbstractBIt<T> {
    protected final ReentrantLock lock = new ReentrantLock();
    protected final Condition hasReady = lock.newCondition();
    protected final ArrayDeque<Batch<T>> ready = new ArrayDeque<>();
    private boolean eager = false;
    private Batch<T> filling = null;
    protected Batch<T> recycled = null;
    private long fillingStart = ORIGIN_TIMESTAMP;
    private int batchCapacity = 10;
    private @NonNegative int readyOffset;
    protected boolean ended;
    private boolean hasNextThrew;

    /* --- --- --- constructors --- --- --- */

    public BufferedBIt(RowType<T> rowType, Vars vars) {
        super(rowType, vars);
    }

    /* --- --- --- helper methods --- --- --- */

    /**
     * Moves {@code filling} to the {@code ready} queue and update capacity estimation.
     * Caller thread MUST hold {@code lock}
     */
    private void completeBatch() {
        int size = filling.size, halfCapacity = batchCapacity>>1;
        if (size > batchCapacity)
            batchCapacity = batchCapacity + halfCapacity;
        else if (size < halfCapacity && batchCapacity > 10)
            batchCapacity = (int)Math.ceil(batchCapacity/1.5);
        ready.add(filling);
        filling = null;
        fillingStart = ORIGIN_TIMESTAMP;
        hasReady.signal();
    }

    /**
     * Sets {@code filling} (and {@code fillingStart}, if needed).
     * Caller thread MUST hold the {@code lock}.
     */
    private void initFilling() {
        if (recycled == null) {
            filling = new Batch<>(rowType.rowClass, batchCapacity);
        } else { // by recycling we avoid 2 allocations (Batch and T[])
            filling = recycled;
            recycled = null;
            filling.clear();
        }
        if (needsStartTime && fillingStart == ORIGIN_TIMESTAMP)
            fillingStart = System.nanoTime();
    }

    /**
     * Recomputes {@code batchCapacity} when batch size setters are called.
     * Caller thread MUST hold {@code lock}.
     */
    protected void updateBatchCapacity() {
        int tgt = (maxBatch>>6 == 0) ? maxBatch : minBatch;
        int capacity = 10;
        while (capacity < tgt) capacity += capacity>>1;
        batchCapacity = capacity;
    }

    /**
     * Block until a {@link Batch} is ready or {@code complete(error)} has been called.
     * The caller thread MUST hold {@code lock}.
     */
    protected void waitReady() {
        if (needsStartTime) {
            waitReadyTimed();
        } else {
            while (!ended && ready.isEmpty())
                hasReady.awaitUninterruptibly();
        }
    }

    private void waitReadyTimed() {
        boolean interrupted = false;
        long delta = -1;

        while (ready.isEmpty() && !ended) {
            if (delta < 64) {
                long now = System.nanoTime();
                if (fillingStart == ORIGIN_TIMESTAMP)
                    fillingStart = now;
                delta = (fillingStart + minWaitNs) - now;
                if (delta < 0) { // minWait deadline is in the past
                    delta = (fillingStart + maxWaitNs) - now;
                    if (delta < 0) // maxWait deadline is in the past
                        delta = Long.MAX_VALUE; // wait forever until we get an item
                }
            }
            try {
                delta = hasReady.awaitNanos(delta);
            } catch (InterruptedException e) { interrupted = true; }
            if (ready.isEmpty() && filling != null && ready(filling.size, fillingStart))
                completeBatch(); //will add to ready and exit this loop
        }
        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Remove (waiting if necessary) the first batch from the {@code ready} queue.
     * If the queue is empty but {@code complete(error)} has been called, either
     * re-throw the given error or return null to signal a non-error completion.
     * @return {@code null} if ready queue is empty and {@code complete(null)} has been called
     * @throws RuntimeException if {@code complete(err)} has been called and the ready queue
     *                          is empty. {@code err} will be wrapped iff it is not a
     *                          {@link RuntimeException}.
     */
    protected @Nullable Batch<T> fetch() {
        lock.lock();
        try {
            waitReady();
            if (!ready.isEmpty()) {
                var batch = ready.remove();
                if (readyOffset > 0) { // physically delete items consumed by next()
                    batch.remove(0, readyOffset);
                    readyOffset = 0;
                }
                return batch;
            }
            checkError();
            return null;
        } finally { lock.unlock(); }
    }

    protected void complete(@Nullable Throwable error) {
        lock.lock();
        try {
            if (!ended) {
                if (filling != null && filling.size > 0)  // add incomplete batch
                    completeBatch();
                ended = true;
                hasReady.signal();
            }
            onTermination(error);
        } finally { lock.unlock(); }
    }

    /**
     * Poll if this {@link BufferedBIt#complete(Throwable)} has been previously called.
     *
     * <p> Using this method is not thread-safe. It should be used only if the caller is the
     * single thread responsible for calling {@link BufferedBIt#complete(Throwable)} or for
     * debugging reasons.</p>
     *
     * @return {@code true} if {@code this.complete(cause)} was previously called.
     */
    public boolean isComplete() {
        return ended;
    }

    /**
     * Whether {@code this} was {@link BufferedBIt#complete(Throwable)}ed with a non-null
     * {@code cause}.
     *
     * <p>Like {@link BufferedBIt#complete(Throwable)}, this should be called by the only
     * thread that calls {@link BufferedBIt#complete(Throwable)} otr for debugging reasons.</p>
     *
     * @return {@code true} if {@link BufferedBIt#isComplete()} and the
     *         {@link BufferedBIt#complete(Throwable)} in effect call used a non-null {@code cause}.
     */
    public boolean isFailed() {
        return ended && error != null;
    }

    protected void feed(T item) throws BItCompletedException {
        lock.lock();
        try {
            if (ended)
                throw new BItCompletedException("Previous complete("+error+") on "+this, this);
            if (filling == null) { // get/reset/create current filling Batch
                Batch<T> last = ready.peekLast(); // try adding to last ready batch
                if (last != null) {
                    int capacity = last.array.length, size = last.size;
                    if (capacity > size && size < maxBatch) { // has space and is legal
                        last.add(item);
                        return;
                    } else if (capacity >= batchCapacity && batchCapacity < maxBatch) {
                        batchCapacity = batchCapacity + (batchCapacity >> 1);
                    }
                }
                initFilling();
            }
            filling.add(item);
            if (eager || ready(filling.size, fillingStart)) {
                eager = false;
                completeBatch();
            }
        } finally { lock.unlock(); }
    }

    protected void feed(Batch<T> batch) throws BItCompletedException {
        if (batch.size == 0)
            return;
        lock.lock();
        try {
            if (ended)
                throw new BItCompletedException("Previous complete("+error+") on "+this, this);
            int start = 0, size = batch.size;
            if (size >= minBatch) {
                ready.add(batch);
                hasReady.signal();
            } else {
                Batch<T> last = ready.peekLast();
                if (last != null) { // there is a ready batch
                    int lSize = last.size, n = Math.min(last.array.length-lSize, maxBatch-lSize);
                    if (n > 0) { // >= 1 item fits in 'last'
                        System.arraycopy(batch.array, 0, last.array, lSize, n);
                        start += n;
                    }
                }
                int length = size - start;
                if (length > 0) {
                    if (filling == null)
                        initFilling();
                    filling.add(batch.array, start, length);
                    if (eager || ready(filling.size, fillingStart)) {
                        eager = false;
                        completeBatch();
                    }
                }
            }
        } finally { lock.unlock(); }
    }

    /* --- --- --- overrides --- --- --- */

    @Override public BIt<T> minBatch(int size) {
        lock.lock();
        try {
            super.minBatch(size);
            updateBatchCapacity();
        } finally { lock.unlock(); }
        return this;
    }

    @Override public BIt<T> maxBatch(int size) {
        lock.lock();
        try {
            super.maxBatch(size);
            updateBatchCapacity();
        } finally { lock.unlock(); }
        return this;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @This BIt<T> tempEager() {
        lock.lock();
        try {
            if (ready.isEmpty()) {
                if (filling != null && filling.size > 0)
                    completeBatch();
                else
                    eager = true;
            }
        } finally { lock.unlock(); }
        return this;
    }

    @Override public final Batch<T> nextBatch() {
        var batch = fetch();
        return batch == null ? Batch.terminal() : batch;
    }

    @Override public final Batch<T> nextBatch(Batch<T> offer) {
        recycle(offer);
        var batch = fetch();
        return batch == null ? Batch.terminal() : batch;
    }

    @Override public boolean recycle(Batch<T> batch) {
        if (recycled == null && batch.array.length >= batchCapacity) {
            // replace values with null to make pointed objects collectable
            T[] a = batch.array;
            for (int i = 0, end = batch.size; i < end; i++)
                a[i] = null;
            recycled = batch;
            return true;
        }
        return false;
    }

    @Override public boolean hasNext() {
        lock.lock();
        try {
            waitReady();
            if (ready.isEmpty()) {
                if (error != null && !hasNextThrew) {
                    // on error, throw ONLY on first hasNext() that would return false
                    // else, a loop that retries after hasNext() threw would keep retrying
                    hasNextThrew = true;
                    checkError();
                }
                return false;
            } else {
                return true;
            }
        } finally { lock.unlock(); }
    }

    @Override public T next() {
        lock.lock();
        try {
            Batch<T> batch = ready.isEmpty() ? fetch() : ready.pollFirst();
            if (batch == null || batch.size == 0)
                throw new NoSuchElementException();
            T item = batch.array[readyOffset++];
            if (readyOffset < batch.size) { // there are remaining items in batch
                ready.addFirst(batch);
            } else { // batch exhausted, reset offset and try to recycle it
                readyOffset = 0;
                if (recycled == null && batch.array.length >= batchCapacity)
                    recycled = batch;
            }
            return item;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        lock.lock();
        try {
            if (!ended && cause == null)
                throw new BItIllegalStateException("cleanup(null) before complete()", this);
            ended = true;
            hasReady.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
