package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

/**
 * An asynchronous {@link BIt} that internally holds a bounded queue of batches.
 */
public interface CallbackBIt<B extends Batch<B>> extends BIt<B> {
    /**
     * Offers the contents of the batch and the batch itself.
     *
     * <p>Contents are always taken, even at the cost of blocking the calling
     * thread. Ownership of the batch itself will only be taken if the batch satisfies
     * {@link BIt#minBatch(int)}</p>
     *
     * @param batch the batch with items to publish
     * @return {@code batch} if ownership remains with caller, {@code null} if ownership
     *         of {@code batch} was taken by the {@link CallbackBIt}.
     * @throws BItCompletedException if this {@link CallbackBIt} has
     *         previously {@link CallbackBIt#complete(Throwable)}ed.
     */
    @Nullable B offer(B batch) throws BItCompletedException;

    /**
     * How many batches this iterator can internally queue before the next
     * {@link CallbackBIt#offer(Batch)} call blocks. Note that an {@code offer(b)} might not
     * queue {@code b} and rather add its contents to an already queued batch.
     * */
    int maxReadyBatches();

    /**
     * How many items, spread across all queued batches can be hold without causing the
     * next {@link CallbackBIt#offer(Batch)} call to block.
     *
     * @return the maximum number of queued items or {@link Long#MAX_VALUE} if not enforced.
     */
    @SuppressWarnings("unused") int maxReadyItems();

    /**
     * Set a value for {@link CallbackBIt#maxReadyItems()}.
     *
     * <p>If this implementation does not support enforcing such bounds, this call return normally
     * with no effect and {@link CallbackBIt#maxReadyItems()} will continue returning
     * {@link Long#MAX_VALUE}.</p>
     *
     * @return {@code this}
     */
    @This CallbackBIt<B> maxReadyItems(int items);

    void complete(@Nullable Throwable error);
}
