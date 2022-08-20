package com.github.alexishuf.fastersparql.client.model.batch;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public interface BatchIt<T> extends Iterator<T>, AutoCloseable {
    /** {@link Class} of elements produced by this iterator. */
    Class<T> elementClass();

    /**
     * Once called, subsequent calls to {@code nextBatch()} will block until:
     *
     * <ul>
     *     <li>{@code} time {@code unit}s have elapsed since the call started; or</li>
     *     <li>the iterator reached its end</li>
     * </ul>
     *
     * @param time minimum amount of time to wait in {@code nextBatch()}.
     * @param unit the {@link TimeUnit} of {@code time}
     * @return {@code this} {@link BatchIt}, for chaining
     */
    BatchIt<T> minWait(long time, TimeUnit unit);

    /**
     * Equivalent to {@code minWait(duration.toMillis(), MILLISECONDS)}
     */
    default BatchIt<T> minWait(Duration duration) {
        return minWait(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Get current {@link BatchIt#minWait(long, TimeUnit)} constraint. */
    long minWait(TimeUnit unit);

    /**
     * Makes subsequent calls to {@code nextBatch()}  block until:
     *
     * <ul>
     *     <li>A batch of {@code size} elements has been obtained</li>
     *     <li>the iterator reached its end</li>
     * </ul>
     *
     * @param size the minimum batch size. Note that batch. Note that batches will be smaller
     *             (or empty) if the iterator reached its end.
     * @return {@code this} {@link BatchIt}, for chaining
     */
    BatchIt<T> minBatch(int size);

    /** Get current {@link BatchIt#minBatch(int)} constraint. */
    int minBatch();

    /**
     * Makes subsequent calls to {@code nextBatch()}  return early (despite other active
     * constraints) if a batch of this size has been obtained.
     *
     * @param size the maximum batch size. Note that empty or smaller batches may still result if
     *             the iterator reaches its end.
     * @return {@code this} {@link BatchIt}, for chaining
     */
    BatchIt<T> maxBatch(int size);

    /** Get current {@link BatchIt#maxBatch(int)} constraint. */
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
     * Similar to {@link BatchIt#nextBatch()}, but adds elements to a pre-existing
     * collection instead of allocating a new array.
     *
     * <p>Note: {@link Collection#clear()} is not called before the elements are
     * {@link Collection#add(Object)}ed. See {@link BatchIt#replaceWithNextBatch(Collection)}
     * if a {@code clear} is desired.</p>
     *
     * @param destination where to add batch elements.
     * @return the batch size: number of elements added to {@code destination}.
     */
    int nextBatch(Collection<? super T> destination);

    /**
     * Equivalent to {@code destination.clear(); return this.nextBatch(destination)}.
     */
    default int replaceWithNextBatch(Collection<? super T> destination) {
        destination.clear();
        return nextBatch(destination);
    }

    /**
     * Signals the iterator will not be used anymore and background processing (if any) as
     * well as resources (opn files and connections) may be safely closed.
     *
     * <p>Calling {@code nextBatch()} after this method will result in a {@link IllegalStateException}.</p>
     *
     * <p>The cleanup performed by this method occurs implicitly if the {@link BatchIt} is
     * consumed until its end with {@code nextBatch()}/{@code next()} calls. However, if the
     * cleanup is implicit, subsequent {@code nextBatch()} calls will simply return empty batches
     * instead of raising an {@link IllegalStateException}.</p>
     */
    @Override void close();
}
