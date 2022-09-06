package com.github.alexishuf.fastersparql.client.model.batch.adapters;

import com.github.alexishuf.fastersparql.client.model.batch.Batch;
import com.github.alexishuf.fastersparql.client.model.batch.base.BItCompletedException;
import com.github.alexishuf.fastersparql.client.model.batch.base.BoundedBufferedBIt;
import com.github.alexishuf.fastersparql.client.util.async.RuntimeExecutionException;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

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
    private static final AtomicInteger nextId = new AtomicInteger(1);

    /* --- --- --- constructors --- --- --- */

    public CallbackBIt(Class<T> elementClass) {
        this(elementClass, null);
    }

    public CallbackBIt(Class<T> elementClass, @Nullable String name) {
        super(elementClass, name == null ? "CallbackBatchIterator-"+nextId.getAndIncrement() : name);
    }


    /* --- --- --- methods --- --- --- */
    
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
     *              in a {@link RuntimeExecutionException} and rethrow to the iterator consumer.
     *              If null, this signals a successful completion and the iterator will simply
     *              report the end of the batches/elements.
     * @return {@code true} iff this is the first {@code complete()} call. After the iterator
     *          is completed once, subsequent calls to this method will return {@code false}.
     */
    @Override public boolean complete(@Nullable Throwable error) { return super.complete(error); }
}
