package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface Receiver<B extends Batch<B>> extends StreamNode {
    /**
     * Delivers a batch for processing.
     *
     * @param batch a batch, whose ownership is transferred from the caller to this method
     * @return A batch that the caller will own in exchange for the batch it gave
     *         this method. If processing of {@code batch} is complete at time this call
     *         returns, {@code batch} itself should be returned.
     */
    @Nullable B onBatch(B batch);

    /**
     * Delivers a single row for processing.
     *
     * <p>Unlike, {@link #onBatch(Batch)} ownership of {@code batch} stays with the caller and
     * implementations must not retain references to to {@code batch} or to data within it.
     * A typical implementation will simply {@link Batch#putRow(Batch, int)} onto a batch it owns</p>
     *
     * @param batch a batch containing a row that is being delivered.
     * @param row the row in {@code batch} being delivered
     */
    default void onRow(B batch, int row) {
        if (batch == null) return;
        B tmp = batch.type().create(1, batch.cols, batch.localBytesUsed(row));
        tmp.putRow(batch, row);
        tmp = onBatch(tmp);
        if (tmp != null)
            tmp.recycle();
    }

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
