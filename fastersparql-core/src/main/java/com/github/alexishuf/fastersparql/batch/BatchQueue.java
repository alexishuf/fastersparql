package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface BatchQueue<B extends Batch<B>> {

    /** {@link BatchType} for {@link #offer(Batch)}ed batches. */
    BatchType<B> batchType();

    /** Names for the columns in {@link #offer(Batch)}ed batches. */
    Vars vars();

    /** Get a (not necessarily empty) batch where new rows may be appended.
     *  Such batch <strong>MUST</strong> be passed to {@link #offer(Batch)}
     *  even if no rows are appended, since it may contain rows previously
     *  {@link #offer(Batch)}ed and not yet delivered.
     *
     * @return a batch with {@link #vars()}{@code .size()} columns that may have pre-existing
     * rows and must be {@link #offer(Batch)}ed back even if no rows are appended to it.
     */
    B fillingBatch();

    class QueueStateException extends Exception {
        public QueueStateException(String message) { super(message); }
    }
    final class CancelledException extends QueueStateException {
        public static final CancelledException INSTANCE = new CancelledException();
        private CancelledException() {super("CompletableBatchQueue cancel()ed, cannot offer/copy");}
    }
    final class TerminatedException extends QueueStateException {
        public static final TerminatedException INSTANCE = new TerminatedException();
        private TerminatedException() {
            super("AsyncEmitter cancelled/completed/failed by the producer");
        }
    }

    /**
     * Offers {@code b} to this queue, which may take {@code b} or make a copy of {@code b}
     * and return {@code b} back to the caller.
     *
     * @param b the batch that may be passed on to a consumer of the queue
     * @return {@code null} or a (likely non-empty) batch to replace {@code b} if ownership of
     *         {@code b} has been taken by {@link TaskEmitter} or by its receiver. {@code b}
     *         itself may be returned if its contents were copied elsewhere, and thus ownership
     *         was retained by the caller.
     * @throws TerminatedException if the queue is in a terminal completed or failed state, which
     *                             makes it impossible to eventually deliver {@code b} (or a copy)
     *                             downstream. The caller <strong>still looses</strong> ownership
     *                             of {@code b}, as if {@code null} had been returned
     * @throws CancelledException if the queue is in a terminal cancelled state. The caller
     *                            <strong>still loses</strong> ownership of {@code b}, as if
     *                            this call had returned {@code null}.
     * @throws RuntimeException if something unexpectedly goes wrong. the caller MUST assume
     *                          it lost ownership of {@code b} and implementations should attempt
     *                          to recycle {@code b} if it has neither been queued nor already
     *                          recycled when the exception is originally raised.
     */
    @Nullable B offer(B b) throws TerminatedException, CancelledException;
}
