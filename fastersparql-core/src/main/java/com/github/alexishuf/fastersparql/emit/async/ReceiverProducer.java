package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A Producer that gets its batches from another {@link Emitter}.
 */
public class ReceiverProducer<B extends Batch<B>> implements Producer, Receiver<B> {
    private static final Logger log = LoggerFactory.getLogger(ReceiverProducer.class);
    private static final int STEP_REQUEST = 64;
    private static final VarHandle STATE, REQUESTED;
    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(ReceiverProducer.class, "plainState", State.class);
            REQUESTED = MethodHandles.lookup().findVarHandle(ReceiverProducer.class, "plainRequested", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings({"FieldMayBeFinal"}) private State plainState = State.ACTIVE;
    @SuppressWarnings("unused") private int plainRequested;
    private final AsyncEmitter<B> downstream;
    private final Emitter<B> upstream;
    private @MonotonicNonNull Throwable error;

    private enum State {
        ACTIVE,
        PAUSED,
        CANCEL_REQUESTED,
        COMPLETED,
        CANCELLED,
        FAILED
    }

    public ReceiverProducer(AsyncEmitter<B> downstream, Emitter<B> upstream) {
        this.downstream = downstream;
        this.upstream = upstream;
        downstream.registerProducer(this);
        try {
            upstream.subscribe(this);
        } catch (Emitter.RegisterAfterStartException e) { // revert downstream.registerProducer
            plainState = State.FAILED;
            error = e;
            downstream.producerTerminated();
            throw e;
        }
    }

    @Override public String toString() {
        return "ReceiverProducer("+upstream+")";
    }

    /* --- --- -- internals --- -- --- */

    private boolean moveState(State dest) {
        while (true) {
            State s = (State) STATE.getOpaque(this);
            boolean allow = switch (s) {
                case ACTIVE, PAUSED               -> true;
                case COMPLETED, CANCELLED, FAILED -> false;
                case CANCEL_REQUESTED -> switch (dest) {
                    case CANCEL_REQUESTED, ACTIVE, PAUSED -> false;
                    case COMPLETED, CANCELLED, FAILED     -> true;
                };
            };
            if (allow) {
                if ((State)STATE.compareAndExchangeRelease(this, s, dest) == s)
                    return true;
                Thread.onSpinWait();
            } else {
                return false;
            }
        }
    }

    /* --- --- -- producer --- -- --- */

    @Override public void resume() {
        if (moveState(State.ACTIVE)) {
            int add = STEP_REQUEST - Math.max(0, (int)REQUESTED.getOpaque(this));
            if (add > 0) {
                REQUESTED.getAndAddRelease(this, add);
                upstream.request(add);
            }
        }
    }
    @Override public void cancel() { upstream.cancel(); }

    @Override public boolean isTerminated() {
        return switch ((State)STATE.getAcquire(this)) {
            case COMPLETED,CANCELLED,FAILED -> true;
            case ACTIVE,PAUSED,CANCEL_REQUESTED -> false;
        };
    }

    @Override public boolean isComplete() {
        return (State)STATE.getAcquire(this) == State.COMPLETED;
    }

    @Override public boolean isCancelled() {
        return (State)STATE.getAcquire(this) == State.FAILED;
    }

    @Override public @Nullable Throwable error() {
        return (State)STATE.getAcquire(this) == State.FAILED ? error : null;
    }

    /* --- --- --- receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (batch == null) return null;
        if (plainState != State.CANCEL_REQUESTED) {
            int requested = (int)REQUESTED.getAndAddRelease(this, -batch.rows)-batch.rows;
            try {
                batch = downstream.offer(batch);
            } catch (AsyncEmitter.CancelledException e) {
                cancel();
            } catch (AsyncEmitter.TerminatedException e) {
                cancel();
                log.warn("{} terminated before {}", downstream, this);
                assert false : "downstream terminated before this ReceiverProducer";
            }
            if ((State)STATE.getOpaque(this) == State.ACTIVE) {
                if (downstream.requested() <= 0) {
                    moveState(State.PAUSED);
                } else {
                    int add = STEP_REQUEST - Math.max(0, requested);
                    if (add > 0) {
                        REQUESTED.getAndAddRelease(this, add);
                        upstream.request(add);
                    }
                }
            }
        }
        return batch;
    }

    @Override public void onComplete() {
        var s = (State)STATE.compareAndExchangeRelease(this, State.ACTIVE, State.COMPLETED);
        assert s == State.ACTIVE : "onComplete() after termination";
    }

    @Override public void onCancelled() {
        var s = (State)STATE.compareAndExchangeRelease(this, State.ACTIVE, State.CANCELLED);
        assert s == State.ACTIVE : "onCancelled() after termination";
    }

    @Override public void onError(Throwable cause) {
        this.error = cause;
        var s = (State)STATE.compareAndExchangeRelease(this, State.ACTIVE, State.FAILED);
        assert s == State.ACTIVE : "onError() after termination";
    }
}
