package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Receiver<B extends Batch<B>> {
    /**
     * Delivers a batch for processing.
     *
     * @param batch a batch, whose ownership is transfered from the caller to this method
     * @return A batch that the caller will own in exchange for the batch it gave
     *         this method. If processing of {@code batch} is complete at time this call
     *         returns, {@code batch} itself should be returned.
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
