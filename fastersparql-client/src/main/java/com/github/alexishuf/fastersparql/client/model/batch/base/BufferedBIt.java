package com.github.alexishuf.fastersparql.client.model.batch.base;

import com.github.alexishuf.fastersparql.client.model.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.batch.BItClosedException;
import com.github.alexishuf.fastersparql.client.model.batch.BItIllegalStateException;
import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link BIt} that can be asynchronously fed with batches or items, which are then
 * buffered into a ring of {@link Batch}es to be delivered to consumers of this {@link BIt}.
 */
public abstract class BufferedBIt<T> extends AbstractBIt<T> {
    private final Logger log = LoggerFactory.getLogger(BufferedBIt.class);

    protected final ReentrantLock lock = new ReentrantLock();
    protected final Condition hasReady = lock.newCondition(), timerWork = lock.newCondition();
    protected final ArrayDeque<Batch<T>> ready = new ArrayDeque<>();
    private Batch<T> filling = null, recycled = null;
    private long fillingStart = ORIGIN_TIMESTAMP;
    private int batchCapacity = 10;
    protected boolean ended;
    private boolean hasNextThrew;
    private @MonotonicNonNull Thread timerThread;
    protected @MonotonicNonNull Throwable error;

    /* --- --- --- constructors --- --- --- */

    public BufferedBIt(Class<T> elementClass, String name) {
        super(elementClass, name);
    }

    /* --- --- --- helper methods --- --- --- */

    /**
     * Virtual thread entrypoint that triggers {@link BufferedBIt#completeBatch()}
     * when time-based conditions make the non-null {@code filling} batch ready.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored") private void timer() {
        lock.lock();
        try {
            while (!ended) {
                long wait = Long.MAX_VALUE; // how many nanoseconds to sleep
                if (fillingStart != ORIGIN_TIMESTAMP) { // maxWaitNs != MAX_VALUE
                    long size = filling == null ? 0 : filling.size;
                    long elapsed = System.nanoTime() - fillingStart;
                    if      (elapsed <  minWaitNs) wait = minWaitNs - elapsed;
                    else if (size    >= minBatch)  wait = -1; //ready
                    else if (elapsed <  maxWaitNs) wait = maxWaitNs - elapsed;
                    else if (size    >  0)         wait = -1; //ready
                    // else: elapsed >= minWaitNs && size == 0, thus wait == MAX_VALUE
                }
                if (wait > 0) {
                    try { timerWork.awaitNanos(wait); } catch (InterruptedException ignored) { }
                }
                if (filling != null && ready(filling.size, fillingStart))
                    completeBatch();
            }
        } finally { lock.unlock(); }
    }

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
            filling = new Batch<>(elementClass, batchCapacity);
        } else { // by recycling we avoid 2 allocations (Batch and T[])
            filling = recycled;
            recycled = null;
            filling.clear();
        }
        if (needsStartTime && fillingStart == ORIGIN_TIMESTAMP) {
            fillingStart = System.nanoTime();
            timerWork.signal();
        }
    }

    /**
     * Recomputes {@code batchCapacity} when batch size setters are called.
     * Caller thread MUST hold {@code lock}.
     */
    protected void updateBatchCapacity() {
        if (maxBatch <= 22) {
            batchCapacity = maxBatch <= 10 ? 10 : (maxBatch <= 15 ? 15 : 22);
        } else {
            int capacity = 10;
            while (capacity < minBatch)
                capacity += capacity>>1;
            batchCapacity = capacity;
        }
    }

    /** Creates/reconfigure timer() virtual thread. Caller thread MUST hold {@code lock}. */
    protected void updateTimer() {
        if (needsStartTime && timerThread == null)
            timerThread = Thread.ofVirtual().name(this+"-timer").start(this::timer);
        else if (timerThread != null && filling != null)
            timerWork.signal();
    }

    /**
     * Block until a {@link Batch} is ready or {@code complete(error)} has been called.
     * The caller thread MUST hold {@code lock}.
     */
    protected void waitReady() {
        if (!ended && ready.isEmpty() && filling == null && needsStartTime) {
            fillingStart = System.nanoTime();
            timerWork.signal(); // makes timer() start new sleep
        }
        while (!ended && ready.isEmpty())
            hasReady.awaitUninterruptibly();
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
        if (closed)
            throw new BItClosedException(this);
        lock.lock();
        try {
            waitReady();
            if (!ready.isEmpty())
                return ready.remove();
            else
                onExhausted();
            if (error != null)
                throw error instanceof RuntimeException re ? re : new RuntimeException(error);
            return null;
        } finally { lock.unlock(); }
    }

    protected boolean complete(@Nullable Throwable error) {
        lock.lock();
        try {
            if (ended) {
                if (error != null && this.error == null
                        && !(error instanceof BItIllegalStateException)) {
                    log.info("{}.complete({}) ignored: previous complete(null)",
                             this, error.getClass().getSimpleName(), error);
                } else {
                    log.trace("{}.end({}) ignored: previous end({})",
                              this, error, Objects.toString(this.error));
                }
                return false;
            }
            log.trace("{}.complete({})", this, Objects.toString(error));
            if (filling != null && filling.size > 0)  // add incomplete batch
                completeBatch();
            this.error = error;
            ended = true;
            timerWork.signal(); // allow timer(), if started, to exit
            hasReady.signal();
            return true;
        } finally { lock.unlock(); }
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
            if (ready(filling.size, fillingStart))
                completeBatch();
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
                    if (ready(filling.size, fillingStart))
                        completeBatch();
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
            updateTimer();
        } finally { lock.unlock(); }
        return this;
    }

    @Override public BIt<T> maxBatch(int size) {
        lock.lock();
        try {
            super.maxBatch(size);
            updateBatchCapacity();
            updateTimer();
        } finally { lock.unlock(); }
        return this;
    }

    @Override public BIt<T> minWait(long time, TimeUnit unit) {
        lock.lock();
        try {
            super.minWait(time, unit);
            updateTimer();
        } finally { lock.unlock(); }
        return this;
    }

    @Override public BIt<T> maxWait(long time, TimeUnit unit) {
        lock.lock();
         try {
             super.maxWait(time, unit);
             updateTimer();
         } finally { lock.unlock(); }
        return this;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public final Batch<T> nextBatch() {
        var batch = fetch();
        return batch == null ? new Batch<>(elementClass, 0) : batch;
    }

    @Override public final Batch<T> nextBatch(Batch<T> offer) {
        recycle(offer);
        var batch = fetch();
        return batch == null ? new Batch<>(elementClass, 0) : batch;
    }

    @Override public final int nextBatch(Collection<? super T> destination) {
        var batch = fetch();
        if (batch == null)
            return 0;
        int size = batch.drainTo(destination);
        recycle(batch);
        return size;
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
        if (closed)
            throw new BItClosedException(this);
        lock.lock();
        try {
            waitReady();
            if (ready.isEmpty()) {
                onExhausted();
                if (error != null && !hasNextThrew) {
                    // on error, throw ONLY on first hasNext() that would return false
                    // else, a loop that retries after hasNext() threw would keep retrying
                    hasNextThrew = true;
                    throw error instanceof RuntimeException re ? re : new RuntimeException(error);
                }
                return false;
            } else {
                return true;
            }
        } finally { lock.unlock(); }
    }

    @Override public final T next() {
        lock.lock();
        try {
            Batch<T> batch = fetch();
            if (batch == null || batch.size == 0)
                throw new NoSuchElementException();
            T item = batch.array[0];
            if (batch.size > 1) {
                batch.size--;
                System.arraycopy(batch.array, 1, batch.array, 0, batch.size);
                ready.addFirst(batch);
            } else if (recycled == null && batch.array.length >= batchCapacity) {
                recycled = batch;
            }
            return item;
        } finally { lock.unlock(); }
    }

    @Override protected void cleanup() {
        if (ended) return;
        lock.lock();
        try {
            complete(new BItClosedException(this));
        } finally { lock.unlock(); }
    }
}
