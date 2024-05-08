package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.HasFillingBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

public interface BatchQueue<B extends Batch<B>> extends HasFillingBatch<B> {

    /** {@link BatchType} for {@link #offer(Orphan)}ed batches. */
    BatchType<B> batchType();

    /** Names for the columns in {@link #offer(Orphan)}ed batches. */
    Vars vars();

    /** Get a (not necessarily empty) batch where new rows may be appended.
     *  Such batch <strong>MUST</strong> be passed to {@link #offer(Orphan)}
     *  even if no rows are appended, since it may contain rows previously
     *  {@link #offer(Orphan)}ed and not yet delivered.
     *
     * @return a batch with {@link #vars()}{@code .size()} columns that may have pre-existing
     * rows and must be {@link #offer(Orphan)}ed back even if no rows are appended to it.
     */
    Orphan<B> fillingBatch();

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
     void offer(Orphan<B> b) throws TerminatedException, CancelledException;

    /**
     * Similar to {@link #offer(Orphan)}, but <strong>the caller retains ownership of </strong>
     * {@code b}. Implementations must not keep references to {@code b} (or to any of its nodes)
     * for longer than the duration of this call. The contents of {@code b} also cannot be
     * modified. Implementations should pass {@code b} to {@link Batch#copy(Batch)} on a new
     * or already queued batch owned by the queue itself.
     *
     * @param b a batch which will not be modified of which the caller will remain the owner.
     * @throws TerminatedException see {@link #offer(Orphan)}
     * @throws CancelledException see {@link #offer(Orphan)}
     */
     default void copy(B b) throws TerminatedException, CancelledException {
         B filling = fillingBatch().takeOwnership(this);
         filling.copy(b);
         offer(filling.releaseOwnership(this));
     }
}
