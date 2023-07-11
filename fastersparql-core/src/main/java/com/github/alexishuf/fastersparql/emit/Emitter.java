package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.Vars;


public interface Emitter<B extends Batch<B>> {
    /** Set of vars naming the columns in batches delivered to {@link Receiver#onBatch(Batch)} */
    Vars vars();

    class RegisterAfterStartException extends IllegalStateException {
        public RegisterAfterStartException() {
            super("subscribe()/register() after request(long)");
        }
    }

    class MultipleRegistrationUnsupported extends IllegalStateException {
        public MultipleRegistrationUnsupported() {
            super("Multiple subscriptions are not supported");
        }
    }

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
            throws RegisterAfterStartException, MultipleRegistrationUnsupported;

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
     * Notifies the {@link Receiver} additionally expects {@code rows} rows distributed
     * across future {@link Batch}es delivered to {@link Receiver#onBatch(Batch)}.
     *
     * <p>If called after {@link #cancel()} or after a termination event has been delivered,
     * this call will have no effect. </p>
     *
     * @param rows the number of additionally expected rows across future deliveries of
     *             batches to {@link Receiver#onBatch(Batch)}. {@link Long#MAX_VALUE}
     *             removes any such bound allowing the underlying producer to produce without
     *             pause until completion, cancellation or failure.
     * @throws NoReceiverException if this is called before {@link #subscribe(Receiver)}.
     */
    void request(long rows) throws NoReceiverException;

}
