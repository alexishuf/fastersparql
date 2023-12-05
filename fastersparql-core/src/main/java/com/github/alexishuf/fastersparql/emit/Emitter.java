package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;


public interface Emitter<B extends Batch<B>> extends StreamNode, Rebindable {
    /** Set of vars naming the columns in batches delivered to {@link Receiver#onBatch(Batch)} */
    Vars vars();

    /**
     * {@link BatchType} of batches emitted by this {@link Emitter}.
     */
    BatchType<B> batchType();

    /**
     * Attaches a receiver to this Emitter. This <strong>MUST</strong> be called before
     * the first {@link #request(long)} call.
     *
     * @param receiver the {@link Receiver} to receive events
     * @throws RegisterAfterStartException if called after {@link #request(long)}. Doing so
     *         is prone to race conditions, since it is uncertain which events the late
     *         {@link Receiver} would get.
     */
    void subscribe(Receiver<B> receiver)
            throws RegisterAfterStartException, MultipleRegistrationUnsupportedException;


    /**
     * Notifies that the {@link Receiver} wishes to stop receiving batches. Once the
     * emitter can ensure it will not deliver new batches, it must call
     * {@link Receiver#onCancelled()}
     */
    void cancel();

    class NoReceiverException extends IllegalStateException {
        public NoReceiverException() {
            super("No Receiver attached");}
    }

    /**
     * Notifies the {@link Receiver} expects to receive at least {@code rows} in {@link Batch}es
     * delivered to {@link Receiver#onBatch(Batch)} after this call.
     *
     * <p>This method is not additive: {@code request(64)} followed by a {@code request(32)} will
     * have the effect of a single {@code request(64)}, assuming there were no deliveries
     * between the two requests.</p>
     *
     * <p>The {@link Receiver} may receive less rows than requested if this emitter
     * completes or is cancelled before delivering all requested rows. {@link Receiver}s should
     * be prepared to receive an unbounded number of rows in excess of what is requested.</p>
     *
     * <p>If called after {@link #cancel()} or after a termination event has been delivered,
     * this call will have no effect. </p>
     *
     * @param rows the number of rows the receiver expects to receive after this call across
     *             batches delivered to {@link Receiver#onBatch(Batch)} after execution
     *             of this method <strong>started</strong>.
     * @throws NoReceiverException if this is called before {@link #subscribe(Receiver)}.
     */
    void request(long rows) throws NoReceiverException;

}
