package com.github.alexishuf.fastersparql.batch.adapters;

import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.batch.base.BoundedBufferedBIt;
import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A concrete implementation of {@link BoundedBufferedBIt}, exposing the {@code feed} and
 * {@code complete} methods.
 *
 * <p>The use case for this is to feed/complete from one thread while another consumes the
 * {@link Batch}es.</p>. Use the {@link BoundedBufferedBIt#maxReadyBatches(int)} and
 * {@link BoundedBufferedBIt#maxReadyItems(long)} methods to set the upper bound of
 * batches/elements that when reached will cause the feeding thread to block.
 */
public class CallbackBIt<T> extends BoundedBufferedBIt<T> {
    public CallbackBIt(Class<T> elementClass, Vars vars) { super(elementClass, vars); }

    /**
     * Queue an {@code item} for delivery in a future batch.
     *
     * @throws BItCompletedException if {@link CallbackBIt#complete(Throwable)}
     *                                   has been called before this method
     */
    @Override public void feed(T item) throws BItCompletedException { super.feed(item); }

    /**
     * Feed an entire batch. This may be more efficient than feeding each element
     *
     * @throws BItCompletedException if {@link CallbackBIt#complete(Throwable)}
     *                                   has been called before this method
     */
    @Override public void feed(Batch<T> batch) throws BItCompletedException { super.feed(batch); }

    /**
     * Mark the iterator as complete after all already queued batches are consumed.
     *
     * @param error the error that caused the completion. If non-null it will be wrapped
     *              in a {@link RuntimeException} and rethrow to the iterator consumer.
     *              If null, this signals a successful completion and the iterator will simply
     *              report the end of the batches/elements.
     */
    @Override public void complete(@Nullable Throwable error) { super.complete(error); }
}
