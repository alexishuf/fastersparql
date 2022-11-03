package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.calledmethods.qual.CalledMethods;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterators.spliteratorUnknownSize;

public interface BIt<T> extends Iterator<T>, AutoCloseable {
    /** {@link Class} of elements produced by this iterator. */
    Class<T> elementClass();

    /**
     * If {@code T} represents something akin to rows in a table, this returns the
     * column names for such table. Else, return an empty list. */
    Vars vars();

    /**
     * Sets the minimum amount of time to wait before declaring a filling batch ready
     * and delivering it to {@code nextBatch()} and {@code hasNext()} callers.
     *
     * <p>If a batch being filled reaches {@link BIt#minBatch()} it will only be deemed ready if
     * this minimum wait has been satisfied. The timestamp that serves as the wait start is either
     * the arrival of the first item in the batch or the entry on the
     * {@code nextBatch()}/{@code hasNext()} method, whichever occurs first.</p>
     *
     * <p>The default is zero, meaning a non-empty batch is immediately ready for consumption.
     * If {@code minWait} is set and {@link BIt#maxWait(TimeUnit)} is zero, then
     * {@link BIt#maxWait(long, TimeUnit)} will be implicitly set to {@link Long#MAX_VALUE}.</p>
     *
     * @param time minimum amount of time to wait for a batch to reach at least
     *             {@link BIt#minBatch()} items.
     * @param unit the {@link TimeUnit} of {@code time}
     * @return {@code this} {@link BIt}
     */
    @This BIt<T> minWait(long time, TimeUnit unit);

    /**
     * Equivalent to {@code minWait(duration.toMillis(), MILLISECONDS)}
     */
    default @This BIt<T> minWait(Duration duration) {
        return minWait(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Get current {@link BIt#minWait(long, TimeUnit)} constraint. */
    long minWait(TimeUnit unit);

    /**
     * The maximum amount of time to wait for a batch to reach {@link BIt#minBatch()}. A non-empty
     * batch smaller than {@link BIt#minBatch()} is only deemed ready after this wait has elapsed.
     * An empty batch is never ready by definition.
     *
     * <p>As in {@link BIt#minWait(long, TimeUnit)}, the wait starts on the arrival of the first
     * item or on the entry on {@code nextBatch()}/{@code hasNext()}, whichever happens first.</p>
     *
     * <p>The default is zero, meaning a non-empty batch is immediately ready for consumption.
     * However, if {@link BIt#minWait(long, TimeUnit)} is set, then the default becomes
     * {@link Long#MAX_VALUE}, meaning a batch with less than {@code minBatch} items will only
     * be ready if the {@link BIt} is itself complete.</p>
     *
     * @param time maximum amount of time to wait for a batch to reach at least
     *             {@link BIt#minBatch()} items.
     * @param unit the {@link TimeUnit} of {@code time}
     * @return {@code this} {@link BIt}
     */
    @This BIt<T> maxWait(long time, TimeUnit unit);

    /**
     * Equivalent to {@code minWait(duration.toMillis(), MILLISECONDS)}
     */
    default @This BIt<T> maxWait(Duration duration) {
        return maxWait(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Get current {@link BIt#maxWait(long, TimeUnit)} constraint. */
    long maxWait(TimeUnit unit);

    /**
     * The minimum number of items in a batch produced by this {@link BIt}. Smaller batches may
     * nevertheless be produced if {@link BIt#maxWait(TimeUnit)} is zero (the default) or if
     * the iterator source is exhausted.
     *
     * <p>The default is {@code 1}, making any non-empty ready.</p>
     *
     * @param size the minimum batch size. Note that batches will be smaller if
     *             {@link BIt#maxWait(TimeUnit)} is reached or empty
     *             if the iteration end is reached (and thus there are no more items).
     * @return {@code this} {@link BIt}, for chaining
     */
    BIt<T> minBatch(int size);

    /** Get current {@link BIt#minBatch(int)} constraint. */
    int minBatch();

    /**
     * The maximum number of items in a batch produced by this iterator.
     *
     * <p>The default is implementation defined but should be large enough to allow tight
     * loops when processing the batch but still tractable in terms of memory usage.</p>
     *
     * @param size the maximum batch size.
     * @return {@code this} {@link BIt}, for chaining
     */
    @This BIt<T> maxBatch(int size);

    /**
     * If there is no batch ready at the time of the call, temporarily set
     * {@link BIt#minWait(long, TimeUnit)} and {@link BIt#maxWait(long, TimeUnit)} to zero,
     * making a batch become ready ASAP. After a batch becomes ready under the zero-wait
     * policy, the original {@link BIt#minWait(long, TimeUnit)} and
     * {@link BIt#maxWait(long, TimeUnit)} are restored.
     *
     * @return {@code this} {@link BIt}, for chaining.
     */
    @This BIt<T> tempEager();

    /** Get current {@link BIt#maxBatch(int)} constraint. */
    int maxBatch();

    /**
     * Get the next batch as an array.
     *
     * <p>The array ownership is transferred to the called, meaning it can be mutated or held without
     * affecting or being affected by subsequent {@code nextBatch()} calls.</p>
     *
     * @return the next batch as an array. An empty batch signals the iterator end ,
     *         i.e., {@code hasNext() == false}.
     */
    Batch<T> nextBatch();

    /**
     * Equivalent to {@code recycle(offer); return nextBatch();}.
     * @param offer A {@link Batch} to be offered for reuse via {@link BIt#recycle(Batch)}.
     * @return the next {@link Batch}, as returned by {@link BIt#nextBatch()}
     */
    default Batch<T> nextBatch(Batch<T> offer) {
        recycle(offer);
        return nextBatch();
    }

    /**
     * Similar to {@link BIt#nextBatch()}, but adds elements to a pre-existing
     * collection instead of allocating a new array.
     *
     * <p>Note: {@link Collection#clear()} is not called before the elements are
     * {@link Collection#add(Object)}ed. See {@link BIt#replaceWithNextBatch(Collection)}
     * if a {@code clear} is desired.</p>
     *
     * @param destination where to add batch elements.
     * @return the batch size: number of elements added to {@code destination}.
     */
    default int nextBatch(Collection<? super T> destination) {
        var b = nextBatch();
        int size = b.drainTo(destination);
        recycle(b);
        return size;
    }

    /**
     * Equivalent to {@code destination.clear(); return this.nextBatch(destination)}.
     */
    @SuppressWarnings("unused")
    default int replaceWithNextBatch(Collection<? super T> destination) {
        destination.clear();
        return nextBatch(destination);
    }

    /**
     * Return ownership of {@link Batch} to this {@link BIt} so that it can be used when
     * assembling new batches.
     *
     * <p>This method is a hint: implementations are allowed to reuse the given {@link Batch}
     * but are not required to.</p>
     *
     * @param batch the batch whose ownership will be returned to this {@link BIt}
     * @return {@code true} iff ownership of {@code batch} has been taken by this {@link BIt}.
     */
    boolean recycle(Batch<T> batch);

    /** Drain all remaining items into {@code dest} */
    default <Coll extends Collection<? super T>> Coll drainTo(Coll dest) {
        try {
            for (var b = nextBatch(); b.size > 0; b = nextBatch(b))
                b.drainTo(dest);
            return dest;
        } finally { close(); }
    }

    /** Drain all remaining items into a new {@link ArrayList}. */
    @CalledMethods("close")
    default List<T> toList() { return drainTo(new ArrayList<>()); }

    /** Drain all remaining items into a new {@link HashSet}. */
    @CalledMethods("close")
    default Set<T>  toSet()  { return drainTo(new HashSet<>()); }

    /** Get a {@link Stream} over this {@link BIt} contents */
    default Stream<T> stream() {
        return StreamSupport.stream(spliteratorUnknownSize(this, IMMUTABLE), false);
    }

    /**
     * Signals the iterator will not be used anymore and background processing (if any) as
     * well as resources (opn files and connections) may be safely closed.
     *
     * <p>Calling {@code nextBatch()} after this method will result in a
     * {@link BItClosedException}.</p>
     *
     * <p>The cleanup performed by this method occurs implicitly if the {@link BIt} is
     * consumed until its end with {@code nextBatch()}/{@code next()} calls. However, if the
     * cleanup is implicit, subsequent {@code nextBatch()} calls will simply return empty batches
     * instead of raising an {@link BItClosedException}.</p>
     */
    @Override void close();
}
