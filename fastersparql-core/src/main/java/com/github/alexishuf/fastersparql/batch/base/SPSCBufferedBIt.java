package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.BoundedBIt;
import com.github.alexishuf.fastersparql.batch.BoundedCallbackBIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.row.RowType;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;

/**
 * A {@link BIt} that can be asynchronously fed with batches or items, which are then
 * buffered into a ring of {@link Batch}es to be delivered to consumers of this {@link BIt}.
 */
public class SPSCBufferedBIt<T> extends SPSCBIt<T> implements BoundedCallbackBIt<T> {
    protected final ArrayDeque<Batch<T>> ready = new ArrayDeque<>();
    private boolean eager = false;
    private Batch<T> filling = null;
    protected Batch<T> recycled = null;
    private long fillingStart = ORIGIN_TIMESTAMP;
    private int batchCapacity = 10;
    private @NonNegative int readyOffset;
    private boolean hasNextThrew;
    protected int maxReadyBatches = Integer.MAX_VALUE;
    protected long readyItems = 0, maxReadyItems = Long.MAX_VALUE;

    /* --- --- --- constructors --- --- --- */

    public SPSCBufferedBIt(RowType<T> rowType, Vars vars) {
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
        signal();
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
     * Wait until constraints imposed by {@link BoundedBIt#maxReadyItems(long)} and
     * {@link BoundedBIt#maxReadyBatches(int)} are satisfied.
     */
    protected void waitForCapacity() {
        while (!terminated && (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches))
           await();
    }

    /**
     * Block until a {@link Batch} is ready or {@code complete(error)} has been called.
     * The caller thread MUST hold {@code lock}.
     */
    protected void waitReady() {
        while (!terminated && ready.isEmpty()) {
            if (needsStartTime) waitReadyTimed();
            else                await();
        }
    }

    private void waitReadyTimed() {
        // wait until minWaitNs
        long ns = System.nanoTime();
        if (fillingStart == ORIGIN_TIMESTAMP) {
            fillingStart = ns;
            ns = minWaitNs;
        } else {
            ns = fillingStart+minWaitNs-ns;
        }
        while (ns > 0 && !terminated && ready.isEmpty()) ns = awaitNanos(ns);

        // wait until maxWaitNs is elapsed, if already elapsed, this will be negative
        ns = fillingStart + maxWaitNs - System.nanoTime();

        while (!terminated && ready.isEmpty()) {
            if (ns <= 0) // once we reached maxWaitNs, wait until filling becomes ready
                ns = Long.MAX_VALUE;
            if (filling != null && ready(filling.size, fillingStart))
                completeBatch(); // filling became ready due to elapsed maxWaitNs
            else
                ns = awaitNanos(ns);
        }
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
        lock();
        try {
            waitReady();
            boolean atCapacity = readyItems >= maxReadyItems || ready.size() >= maxReadyBatches;
            var batch = ready.poll();
            if (batch != null) {
                if (readyOffset > 0) { // physically delete items consumed by next()
                    batch.remove(0, readyOffset);
                    readyOffset = 0;
                }
                readyItems -= batch.size;
                if (atCapacity) signal(); // wake waitForCapacity
                return batch;
            }
            checkError();
            return null;
        } finally { unlock(); }
    }

    @Override public void feed(T item) throws BItCompletedException {
        lock();
        try {
            //block if above buffer limits
            if (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches)
                waitForCapacity();
            if (terminated) throw mkCompleted();
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
        } finally {
            try {
                readyItems++;
            } finally { unlock(); }
        }
    }

    @Override public void feed(Batch<T> batch) throws BItCompletedException {
        int size = batch.size, start = 0;
        if (size == 0)
            return;
        lock();
        try {
            //block if above buffer limits
            if (readyItems >= maxReadyItems || ready.size() >= maxReadyBatches)
                waitForCapacity();
            if (terminated) throw mkCompleted();
            if (size >= minBatch) { // batch qualifies, just add it
                ready.add(batch);
                signal();
            } else { // batch below minBatch, try appending to last or filling
                Batch<T> last = ready.peekLast();
                if (last != null) { // there is a ready batch
                    int lSize = last.size;
                    start = Math.min(last.array.length-lSize, maxBatch-lSize);
                    if (start > 0) { // >= 1 item fits in 'last'
                        System.arraycopy(batch.array, 0, last.array, lSize, start);
                        last.size += start;
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
        } finally {
            try {
                if (!terminated) readyItems += size;
            } finally { unlock(); }
        }
    }

    /* --- --- --- overrides --- --- --- */

    @Override protected void updatedBatchConstraints() {
        int capacity = 10, tgt = Math.min(maxBatch, Math.max(minBatch, BIt.PREFERRED_MIN_BATCH));
        while (capacity < tgt) capacity += capacity>>1;
        lock();
        try {
            if (batchCapacity < capacity || batchCapacity > maxBatch)
                batchCapacity = capacity;
            if (signalWaiter != null && filling != null && ready.isEmpty() && ready(filling.size, fillingStart))
                    completeBatch();
        } finally {
            unlock();
        }
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @This SPSCBufferedBIt<T> maxReadyBatches(int max) {
        lock();
        try {
            this.maxReadyBatches = max;
            signal();
        } finally { unlock(); }
        return this;
    }

    @Override public int maxReadyBatches() { return maxReadyBatches; }

    @Override public @This SPSCBufferedBIt<T> maxReadyItems(long max) {
        lock();
        try {
            this.maxReadyItems = max;
            signal();
        } finally {
            unlock();
        }
        return this;
    }

    @Override public long maxReadyItems() { return maxReadyItems; }

    @Override public @This BIt<T> tempEager() {
        lock();
        try {
            if (ready.isEmpty()) {
                if (filling != null && filling.size > 0) completeBatch();
                else                                     eager = true;
            }
        } finally { unlock(); }
        return this;
    }

    @Override public final Batch<T> nextBatch() {
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
        lock();
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
        } finally { unlock(); }
    }

    @Override public T next() {
        lock();
        try {
            boolean atCapacity = readyItems >= maxReadyItems || ready.size() >= maxReadyBatches;
            var batch = ready.pollFirst();
            if (batch == null) {
                batch = fetch();
                if (batch != null) readyItems += batch.size;
            }
            if (batch == null || batch.size == 0)
                throw new NoSuchElementException();
            --readyItems;
            T item = batch.array[readyOffset++];
            if (readyOffset < batch.size) { // there are remaining items in batch
                ready.addFirst(batch);
            } else { // batch exhausted, reset offset and try to recycle it
                readyOffset = 0;
                if (recycled == null && batch.array.length >= batchCapacity)
                    recycled = batch;
            }
            if (atCapacity) signal(); // wake waitForCapacity()
            return item;
        } finally { unlock(); }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        lock();
        try {
            if (filling != null && filling.size > 0) completeBatch();
        } finally { unlock(); }
        super.cleanup(cause);
    }
}
