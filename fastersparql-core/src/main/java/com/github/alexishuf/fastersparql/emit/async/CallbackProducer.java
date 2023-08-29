package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract class CallbackProducer<B extends Batch<B>>
        extends Stateful
        implements Producer<B>, CompletableBatchQueue<B> {
    private static final Logger log = LoggerFactory.getLogger(CallbackProducer.class);
    private static final boolean LOG_DEBUG = log.isDebugEnabled();
    private Throwable error = UNSET_ERROR;
    private AsyncEmitter<B> downstream;

    protected CallbackProducer(Flags flags) {
        super(Stateful.CREATED, flags);
    }

    @Override public final void registerOn(AsyncEmitter<B> emitter) throws IllegalStateException {
        if (downstream == null) {
            downstream = emitter;
            emitter.registerProducer(this);
        } else if (downstream != emitter) {
            throw new IllegalStateException("Already registered on another emitter");
        }
    }

    @Override
    public final void forceRegisterOn(AsyncEmitter<B> emitter) throws IllegalStateException {
        if (downstream == emitter) return;
        downstream = emitter;
        emitter.registerProducer(this);
    }

    @Override public void rebindAcquire() {delayRelease();}
    @Override public void rebindRelease() {allowRelease();}

    /**
     * Causes the producer to eventually pause production and delivery of batches to
     * {@link #offer(Batch)}, until {@link #resume()} is called.
     */
    protected abstract void pause();

    /**
     * Causes the producer to permanently stop producing and feeding batches to
     * {@link #offer(Batch)}. Any number of batches may still be delivered after this call returns.
     */
    protected abstract void doCancel();

    @Override public final void cancel() {
        if (moveStateRelease(statePlain(), CANCEL_REQUESTED)) {
            try {
                doCancel();
            } catch (Throwable t) { handleDoCancelFailure(t); }
        }
    }

    private void handleDoCancelFailure(Throwable t) {
        terminate(FAILED, t);
        String name = t.getClass().getSimpleName();
        if (LOG_DEBUG)
            log.info("Ignoring {} during {}.doCancel()", name, this, t);
        else
            log.info("Ignoring {} during {}.doCancel()", name, this);
    }

    @Override public BatchType<B> batchType() { return downstream.batchType; }
    @Override public Vars         vars()      { return downstream.vars; }

    @Override public final void complete(@Nullable Throwable cause) {
        int tgt = interceptComplete(cause);
        if (tgt < 0)
            return;
        if ((tgt & IS_TERM) == 0)
            throw new IllegalArgumentException("Illegal state returned by interceptComplete()");
        terminate(tgt, cause);
    }

    private void terminate(int termState, @Nullable Throwable cause) {
        if (this.error == null) this.error = cause;
        int current = statePlain();
        if (moveStateRelease(current, termState)) {
            downstream.producerTerminated();
            markDelivered(current, termState);
        }
    }

    /**
     * To which {@link #IS_TERM} state should the producer transition in
     * response to a {@link #complete(Throwable)} call with given {@code cause}.
     */
    protected int interceptComplete(@Nullable Throwable cause) {
        if (cause == null) {
            return (state()&STATE_MASK) == CANCEL_REQUESTED
                    ? CANCELLED : COMPLETED;
        } else if (cause == CancelledException.INSTANCE) {
            return CANCELLED;
        } else {
            if (this.error == UNSET_ERROR) this.error = cause;
            return FAILED;
        }
    }

    @Override public final @Nullable B offer(B b) throws TerminatedException {
        if (downstream == null)
            throw new IllegalStateException("Not registered in AsyncEmitter");
        boolean overflowing = downstream.requested() <= 0;
        if (overflowing)
            pause();
        try {
            b = downstream.offer(b);
        } catch (CancelledException e) {
            cancel();
        }
        if (overflowing && downstream.requested() > 0)
            resume();
        return b;
    }

    @Override public final boolean      isTerminated() { return (state()&IS_TERM) != 0; }
    @Override public final boolean        isComplete() { return isCompleted(state()); }
    @Override public final boolean       isCancelled() { return isCancelled(state()); }
    @Override public final @Nullable Throwable error() {
        return isFailed(stateAcquire()) ? error : null;
    }
}
