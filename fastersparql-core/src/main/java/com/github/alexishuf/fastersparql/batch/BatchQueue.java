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

    /** Create an empty batch with {@link #vars()}{@code .size()} columns. */
    default B createBatch() { return batchType().create(vars().size()); }

    class QueueStateException extends Exception {
        public QueueStateException(String message) { super(message); }
    }
    final class CancelledException extends QueueStateException {
        public static final CancelledException INSTANCE = new CancelledException();
        private CancelledException() {super("AsyncEmitter cancel()ed, cannot offer/copy");}
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
     *                             downstream. The caller will retain ownership of {@code b}
     * @throws CancelledException if the queue is in a terminal cancelled state. The caller will
     *                            retain ownership of {@code b}
     */
    @Nullable B offer(B b) throws TerminatedException, CancelledException;


}
