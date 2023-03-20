package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public interface BIt<B extends Batch<B>> extends AutoCloseable {
    /**
     * Preferred {@link BIt#minBatch(int)} value if values above 1 (the default) are possible.
     *
     * <p>This default is based on two typical properties of x86 CPUs:</p>
     *
     * <ol>
     *     <li>Cache lines have 64 bytes</li>
     *     <li>Cache lines are fetched/evicted/invalidated in pairs</li>
     * </ol>
     *
     * <p>On the software side, the following is assumed:</p>
     * <ol>
     *     <li>Consumers of BIt will be bottlenecked by RAM (i.e., they are not doing expensive
     *         processing on each {@link Batch} item</li>
     *     <li>The JVM will be using compressed pointers</li>
     *     <li>The JVM overhead for an array (including the {@code length}) is 24 bytes</li>
     * </ol>
     *
     * Between {@code TERM} and {@code COMPRESSED} {@code Term} has the smallest bytes/term ratio
     * of 4 bytes. Thus, we try to fill 2 cache lines with 1-column rows using a {@code TERM} batch.
     */
    int PREFERRED_MIN_BATCH = (2*64-24)/4;

    /**
     * Preferred value for {@link BIt#maxBatch(int)}.
     */
    int DEF_MAX_BATCH = 1<<16;

    /** Set of methods to manipulate elements produced by this iterator. */
    BatchType<B> batchType();

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
    @This BIt<B> minWait(long time, TimeUnit unit);

    /**
     * Equivalent to {@code minWait(duration.toMillis(), MILLISECONDS)}
     */
    default @This BIt<B> minWait(Duration duration) {
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
    @This BIt<B> maxWait(long time, TimeUnit unit);

    /**
     * Equivalent to {@code minWait(duration.toMillis(), MILLISECONDS)}
     */
    default @This BIt<B> maxWait(Duration duration) {
        return maxWait(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Get current {@link BIt#maxWait(long, TimeUnit)} constraint. */
    long maxWait(TimeUnit unit);

    /**
     * The minimum number of rows in a batch produced by this {@link BIt}. Smaller batches may
     * nevertheless be produced if {@link BIt#maxWait(TimeUnit)} is zero (the default) or if
     * the iterator source is exhausted.
     *
     * <p>The default is {@code 1}, making any non-empty ready.</p>
     *
     * @param rows the minimum batch rows. Note that batches will be smaller if
     *             {@link BIt#maxWait(TimeUnit)} is reached or empty
     *             if the iteration end is reached (and thus there are no more items).
     * @return {@code this} {@link BIt}, for chaining
     */
    BIt<B> minBatch(int rows);

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
    @This BIt<B> maxBatch(int size);

    /**
     * If there is no batch ready at the time of the call, temporarily set
     * {@link BIt#minWait(long, TimeUnit)} and {@link BIt#maxWait(long, TimeUnit)} to zero,
     * making a batch become ready ASAP. After a batch becomes ready under the zero-wait
     * policy, the original {@link BIt#minWait(long, TimeUnit)} and
     * {@link BIt#maxWait(long, TimeUnit)} are restored.
     *
     * @return {@code this} {@link BIt}, for chaining.
     */
    @This BIt<B> tempEager();

    /**
     * Sets min/max batch size and batch wait to preferred values. This will introduce a
     * delay per {@link BIt#nextBatch(B)} call of at least
     * {@link FSProperties#batchMinWait(TimeUnit)} and at most
     * {@link FSProperties#batchMaxWait(TimeUnit)}.
     *
     * @return {@code this}, for chaining.
     */
    BIt<B> preferred();

    /**
     * Sets min/max batch size and wait time so that {@link BIt#nextBatch(B)} delay is minimal.
     *
     * <p>Equivalent to:</p>
     *
     * <pre>
     *     minBatch(1)
     *     minWait(0, NANOSECONDS)
     *     maxWait(0, NANOSECONDS)
     * </pre>
     * @return {@code this}, for chaining.
     */
    @This BIt<B> eager();

    /** Get current {@link BIt#maxBatch(int)} constraint. */
    int maxBatch();

    /**
     * Get the next batch or {@code null} if there are no more batches.
     *
     * <p>Ownership of the batch is passed to the caller, who should release the batch
     * only once through a subsequent call to this method or {@link BIt#recycle(Batch)}, or
     * {@link BatchType#recycle(Batch)}.</p>
     *
     * @param offer if non-null this batch may be clear()ed and used as the return batch in this
     *              call or in subsequent calls. Note that the caller
     *              <strong>ALWAYS</strong> looses ownership of {@code offer} and
     *              should not read nor write to the batch.
     * @return {@code null} if the iterator is exhausted or a non-empty batch whose
     *          ownership is transferred to the caller.
     */
    @Nullable B nextBatch(@Nullable B offer);

    /**
     * Return ownership of {@link Batch} to this {@link BIt} so that it can be used when
     * assembling new batches.
     *
     * <p>This method is a hint: implementations are allowed to reuse the given {@link Batch}
     * but are not required to.</p>
     *
     * @param batch the batch whose ownership will be returned to this {@link BIt}
     * @return {@code batch} if the caller remains owner of batch or {@code null} if the
     *         caller lost ownership of {@code batch}.
     */
    @Nullable B recycle(B batch);

    /** Take ownership of a previously {@link BIt#recycle(Batch)}ed batch. */
    @Nullable B stealRecycled();

    /**
     * Signals the iterator will not be used anymore and background processing (if any) as
     * well as resources (opn files and connections) may be safely closed.
     *
     * <p>Calling {@code nextBatch()} after this method will result in a
     * {@link BItReadClosedException}.</p>
     *
     * <p>The cleanup performed by this method occurs implicitly if the {@link BIt} is
     * consumed until its end with {@code nextBatch()}/{@code next()} calls. However, if the
     * cleanup is implicit, subsequent {@code nextBatch()} calls will simply return empty batches
     * instead of raising an {@link BItReadClosedException}.</p>
     */
    @Override void close();
}
