package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Receiver<B extends Batch<B>> extends StreamNode {
    /**
     * Delivers a batch for processing.
     *
     * <p>Processing may happen during this method call or some time after this call returns.
     * Ownership of {@code batch} is transferred from the caller to the receiver. The receiver
     * MAY return ownership to the caller by returning {@code batch}. However, the receiver must
     * observe the following conditions before returning {@code batch}: </p>
     *
     * <ul>
     *     <li>All processing has completed before the return of this method call</li>
     *     <li>The contents of {@code batch} have not been modified</li>
     *     <li>No references to {@code batch} or its internals that were created as result of
     *         the execution of this method are reachable. I.e., the receiver has not ceded
     *         ownership of {@code batch}.</li>
     * </ul>
     *
     * @param batch a batch, whose ownership is transferred from the caller to this method
     * @return {@code batch} if the caller remains the exclusive owner of the unmodified
     *         {@code batch}, {@code null} if the caller lost its ownership of {@code batch}.
     */
    @Nullable B onBatch(B batch);

    /**
     * Called once the {@link Emitter} has exhausted its data source and no more batches
     * will be delivered.
     */
    void onComplete();

    /**
     * Notifies that after a {@link Emitter#cancel()} call, the emitter safely reached a stop
     * point and will not send more batches to this receiver.
     */
    void onCancelled();

    /**
     * Notifies the emitter has met a non-recoverable error and will stop emitting new batches.
     *
     * @param cause the exception that caused the termination.
     */
    void onError(Throwable cause);
}
