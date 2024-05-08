package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

public interface Stage<I extends Batch<I>, O extends Batch<O>, S extends Stage<I, O, S>>
        extends Emitter<O, S>, Receiver<I> {

    /**
     * Idempotently call {@link Emitter#subscribe(Receiver)} on {@code emitter}, while enforcing
     * {@code this} {@link AbstractStage} has only one upstream {@link Emitter}.
     *
     * @param upstream the {@link Emitter} to subscribe if this stage has not yet done so.
     * @return {@code this}, for chaining
     * @throws MultipleRegistrationUnsupportedException if this stage was previously subscribed to an
     *                                         {@link Emitter} other than {@code emitter}.
     */
    @SuppressWarnings("unused") @This S subscribeTo(Orphan<? extends Emitter<I, ?>> upstream);

    /** Get the {@code upstream} given in {@link #subscribeTo(Orphan)}. */
    @MonotonicNonNull Emitter<I, ?> upstream();

    final class NoEmitterException extends IllegalStateException {
        public NoEmitterException() {
            super("Stage not yet subscribedTo() an upstream Emitter");
        }
        @Override public String toString() { return getClass().getSimpleName()+": "+getMessage(); }
    }

}
