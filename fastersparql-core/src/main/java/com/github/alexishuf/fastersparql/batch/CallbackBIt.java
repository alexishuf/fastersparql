package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.common.returnsreceiver.qual.This;

/**
 * An asynchronous {@link BIt} that internally holds a bounded queue of batches.
 */
public interface CallbackBIt<B extends Batch<B>> extends BIt<B>, CompletableBatchQueue<B> {
    /**
     * Copies the content of {@code batch} into internal storage managed by {@code this} so that
     * the rows can later by consumed via {@link #nextBatch(Batch)}
     * @param batch batch whose rows will be copied. It will not be modified and caller ALWAYS
     *              retains ownership
     * @throws CancelledException see {@link #offer(Batch)}
     * @throws TerminatedException see {@link #offer(Batch)}
     */
    void copy(B batch) throws CancelledException, TerminatedException;

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

    /**
     * Current state of the {@link CallbackBIt}
     *
     * <p>Interpretations: </p>
     *
     * <ul>
     *     <li>{@link State#FAILED}: {@link #complete(Throwable)} called with non-null cause</li>
     *     <li>{@link State#COMPLETED}: {@link #complete(Throwable)} called with null</li>
     *     <li>{@link State#ACTIVE}: neither {@link #complete(Throwable)} nor {@link #close()} were called</li>
     *     <li>{@link State#CANCELLED}: {@link #close()} was called</li>
     * </ul>
     */
    State state();
}
