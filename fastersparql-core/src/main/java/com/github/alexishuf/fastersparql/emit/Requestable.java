package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;

public interface Requestable {
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
     * <p>If called after {@link Emitter#cancel()} or after a termination event has been delivered,
     * this call will have no effect. </p>
     *
     * @param rows the number of rows the receiver expects to receive after this call across
     *             batches delivered to {@link Receiver#onBatch(Batch)} after execution
     *             of this method <strong>started</strong>.
     * @throws Emitter.NoReceiverException if this is called before {@link Emitter#subscribe(Receiver)}.
     */
    void request(long rows) throws Emitter.NoReceiverException;
}
