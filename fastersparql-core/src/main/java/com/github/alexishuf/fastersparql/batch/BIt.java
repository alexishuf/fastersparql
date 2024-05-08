package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public interface BIt<B extends Batch<B>> extends AutoCloseable, StreamNode {
    /**
     * Value to use with {@link BIt#minWait(long, TimeUnit)} when one desires the lowest possible
     * wait time without introducing wasteful overhead.
     *
     * <p>This was determined by benchmarking with JMH the average time of
     * {@code LockSupport.parkNanos(blocker, 1_000)}. On an AMD Ryzen 7 5700U running linux
     * this was 64_034.905 ns. Note that most of such time (50 us) is due to Linux <a href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/">
     * limits on precision of process wake-ups</a>.</p>
     *
     */
    int QUICK_MIN_WAIT_NS = 75_000;

    /**
     * Value to use with {@link #maxWait(long, TimeUnit)} when the smallest practical value above
     * zero is desired.
     */
    int QUICK_MAX_WAIT_NS = QUICK_MIN_WAIT_NS+1;

    /** Set of methods to manipulate elements produced by this iterator. */
    BatchType<B> batchType();

    /**
     * If {@code T} represents something akin to rows in a table, this returns the
     * column names for such table. Else, return an empty list. */
    Vars vars();

    /**
     * Report batches and completion events to {@code metrics}.
     *
     * <p>If this {@link BIt} is already terminated,
     * {@link MetricsFeeder#completeAndDeliver(Throwable, boolean)} shall be immediately called.
     * For queue-based {@link BIt}, {@link MetricsFeeder#batch(int)} may be called either from
     * {@link BIt#nextBatch(Orphan)} or from {@link CallbackBIt#offer(Orphan)}, but there must be
     * exactly one call per batch that passeed through the queue.
     *
     * <p>A call to this method replaces any listener set by a previous call.
     * {@link MetricsFeeder#completeAndDeliver(Throwable, boolean)} is <strong>NOT</strong>
     * called when a listener is replaced. If {@code metrics} is null, there will be no
     * listener for future events.</p>
     *
     * @param metrics listener for batch and termination events on this {@link BIt}.
     *                A {@code null} simply causes the removal of the current listener (if set),
     *                without any other effects.
     * @return {@code this} {@link BIt}, for chaining further methods.
     */
    @This BIt<B> metrics(@Nullable MetricsFeeder metrics);

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
     * delay per {@link BIt#nextBatch(Orphan)} call of at least
     * {@link FSProperties#batchMinWait(TimeUnit)} and at most
     * {@link FSProperties#batchMaxWait(TimeUnit)}.
     *
     * @return {@code this}, for chaining.
     */
    BIt<B> preferred();

    /**
     * Sets {@link #minWait(long, TimeUnit)} and {@link #maxWait(long, TimeUnit)} to the
     * minimum practical values, without touching min/max batch size. This should
     * enable larger batches and better data/instruction locality with the minimal latency
     * increase.
     *
     * <p>To keep latency increase really minimal, this should be done on leaf iterators and not
     * iterators representing inner nodes of the plan.</p>
     * @return {@code this}, for chaining.
     */
    @SuppressWarnings("UnusedReturnValue") BIt<B> quickWait();

    /**
     * Sets min/max batch size and wait time so that {@link BIt#nextBatch(Orphan)} delay is minimal.
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
     * @param offer if non-null this batch may be clear()ed and used as the return batch in this
     *              call or in subsequent calls. Note that the caller
     *              <strong>ALWAYS</strong> looses ownership of {@code offer} and
     *              should not read nor write to the batch.
     * @return {@code null} if the iterator is exhausted or a non-empty batch whose
     *          ownership is transferred to the caller.
     */
    @Nullable Orphan<B> nextBatch(@Nullable Orphan<B> offer);

    /**
     * Null-safe equivalent to {@link #nextBatch(Orphan)} with
     * {@code offer.releaseOwnership(offerOwner)} followed by {@code takeOwnership(offerOwner)}
     * on the {@link Orphan} returned by {@link #nextBatch(Orphan)}.
     *
     * @param offer {@code null} or a batch, owned by {@code offerOwner}, to be offered
     *                          to {@link #nextBatch(Orphan)}
     * @param offerOwner the current owner of {@code offer} and future owner of the batch
     *                   returned by this method
     * @return the result of {@link #nextBatch(Orphan)}, owned by {@code offerOwner}, if not null.
     */
    default @Nullable B nextBatch(@Nullable B offer, Object offerOwner) {
        return Orphan.takeOwnership(nextBatch(Owned.releaseOwnership(offer, offerOwner)), offerOwner);
    }

    /**
     * Signals the iterator will not be used anymore and background processing (if any) as
     * well as resources (opn files and connections) may be safely closed.
     *
     * <p>Calling {@code nextBatch()} after this method will result in a
     * {@link BItReadCancelledException}.</p>
     *
     * <p>The cleanup performed by this method occurs implicitly if the {@link BIt} is
     * consumed until its end with {@code nextBatch()}/{@code next()} calls. However, if the
     * cleanup is implicit, subsequent {@code nextBatch()} calls will simply return empty batches
     * instead of raising an {@link BItReadCancelledException}.</p>
     */
    @Override void close();

    /**
     * Cancel the {@link BIt} as {@link #close()} would, but also returns a boolean indicating
     * if termination occurred due to this call ({@code true}) or if the {@link BIt} was
     * already in a {@link State#isTerminated()} state ({@code false})
     *
     * <p>Note that after this method returns another thread might still be calling
     * {@link #nextBatch(Orphan)} and such calls might return non-null and non-empty batches
     * for an arbitrary window after this method or {@link #close()} were called, depending on
     * how the {@link BIt} implementation</p>
     *
     * @return {@code true} if this call caused termination of the {@link BIt}
     */
    boolean tryCancel();

    /**
     * Get the current state without ensuring any memory ordering semantics nor expecting
     * or granting exclusive access. The value returned by this method might be stale even
     * before control returns to the caller.
     *
     * @return the current {@link State}
     */
    State state();

    enum State {
        ACTIVE,
        COMPLETED,
        FAILED,
        CANCELLED;

        public boolean isTerminated() { return this != ACTIVE; }
    }
}
