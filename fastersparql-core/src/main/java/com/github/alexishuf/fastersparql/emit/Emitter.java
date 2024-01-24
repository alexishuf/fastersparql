package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;


public interface Emitter<B extends Batch<B>> extends StreamNode, Rebindable, Requestable {
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
     * An adequate size to use with {@link #request(long)} by receivers that prefer to
     * issue multiple requests instead of a single large or unbounded {@link #request(long)}.
     *
     * @return a value for use with repeated {@link #request(long)} calls to this emitter.
     */
    default int preferredRequestChunk() {
        int cols = Math.max(1, vars().size());
        int b = FSProperties.emitReqChunkBatches();
        return Math.max(b, b*batchType().preferredTermsPerBatch()/cols);
    }

    /** Whether {@link Receiver#onComplete()} has been delivered downstream */
    boolean isComplete();

    /** Whether {@link Receiver#onCancelled()} has been delivered downstream */
    boolean isCancelled();

    /** Whether {@link Receiver#onError(Throwable)} has been delivered downstream */
    boolean isFailed();

    /** Whether {@link Receiver#onComplete()}, {@link Receiver#onCancelled()} or
     *  {@link Receiver#onError(Throwable)} has been delivered downstream*/
    boolean isTerminated();

    /**
     * Notifies that the {@link Receiver} wishes to stop receiving batches. Once the
     * emitter can ensure it will not deliver new batches, it must call
     * {@link Receiver#onCancelled()}
     *
     * @return {@code true} if this call had an effect and termination will be delivered
     *         <strong>after</strong> this call. {@code false} if termination has already been
     *         delivered. Note that even if this returns true, the delivery of a
     *         {@link Receiver#onError(Throwable)} instead of {@link Receiver#onCancelled()} is
     *         still possible.
     */
    boolean cancel();

    class NoReceiverException extends IllegalStateException {
        public NoReceiverException() {
            super("No Receiver attached");}
    }
}
