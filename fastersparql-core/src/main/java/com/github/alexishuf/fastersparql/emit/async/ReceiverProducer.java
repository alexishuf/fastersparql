package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BatchQueue;
import com.github.alexishuf.fastersparql.batch.BatchQueue.TerminatedException;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

/**
 * A Producer that gets its batches from another {@link Emitter}.
 */
public class ReceiverProducer<B extends Batch<B>> extends Stateful implements Producer<B>, Receiver<B> {
    private static final int MAX_REQUESTED = 64, MIN_REQUEST = MAX_REQUESTED>>3;
    private static final VarHandle REQUESTED;
    static {
        try {
            REQUESTED = MethodHandles.lookup().findVarHandle(ReceiverProducer.class, "plainRequested", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") private int plainRequested;
    private AsyncEmitter<B> downstream;
    private final Emitter<B> upstream;
    private Throwable error = UNSET_ERROR;


    public ReceiverProducer(Emitter<B> upstream, AsyncEmitter<B> downstream) {
        super(CREATED, Flags.DEFAULT);
        this.downstream = downstream;
        this.upstream = upstream;
        try {
            upstream.subscribe(this);
        } catch (RegisterAfterStartException e) { // revert downstream.registerProducer
            error = e;
            if (moveStateRelease(CREATED, FAILED)) {
                if (downstream != null)
                    downstream.producerTerminated();
                markDelivered(FAILED);
            }
            throw e;
        }
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.of(upstream);
    }

    @Override public void registerOn(AsyncEmitter<B> emitter) {
        if (downstream != emitter)
            throw new IllegalStateException("Already registered with another emitter");
    }

    @Override public void forceRegisterOn(AsyncEmitter<B> ae) throws IllegalStateException {
        if ((state()&STATE_MASK) != CREATED)
            throw new IllegalStateException("Already started");
        this.downstream = ae;
        ae.registerProducer(this);
    }

    @Override public String toString() {
        return "ReceiverProducer("+upstream+")";
    }

    /* --- --- -- producer --- -- --- */

    @Override public void resume() {
        if (downstream == null) throw new NoDownstreamException(this);
        if (moveStateRelease(statePlain(), ACTIVE)) {
            int add = MAX_REQUESTED - Math.max(0, (int)REQUESTED.getOpaque(this));
            if (add > MIN_REQUEST) {
                REQUESTED.getAndAddRelease(this, add);
                upstream.request(add);
            }
        }
    }
    @Override public void cancel() {
        if (upstream == null)
            throw new NoUpstreamException(this);
        moveStateRelease(statePlain(), CANCEL_REQUESTED);
        upstream.cancel();
    }

    @Override public void rebindAcquire() {
        if (upstream == null) throw new NoUpstreamException(this);
        upstream.rebindAcquire();
    }
    @Override public void rebindRelease() {
        if (upstream == null) throw new NoUpstreamException(this);
        upstream.rebindRelease();
    }
    @Override public void rebind(BatchBinding binding) {
        if (upstream == null) throw new NoUpstreamException(this);
        upstream.rebind(binding);
    }

    @Override public boolean      isTerminated() { return (state()&IS_TERM) != 0; }
    @Override public boolean        isComplete() { return isCompleted(state()); }
    @Override public boolean       isCancelled() { return isCancelled(state()); }
    @Override public @Nullable Throwable error() { return isFailed(stateAcquire()) ? error : null; }

    /* --- --- --- receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (batch == null) return null;
        int state = state();
        if ((state&STATE_MASK) != CANCEL_REQUESTED) {
            int requested = (int)REQUESTED.getAndAddRelease(this, -batch.rows)-batch.rows;
            try {
                batch = downstream.offer(batch);
            } catch (BatchQueue.QueueStateException e) {
                cancel();
                state = (state&FLAGS_MASK) | CANCEL_REQUESTED;
                assert !(e instanceof TerminatedException) : "downstream terminated before this";
            }
            if ((state&STATE_MASK) == ACTIVE) {
                if (downstream.requested() <= 0) {
                    moveStateRelease(state, PAUSED);
                } else {
                    int add = MAX_REQUESTED - Math.max(0, requested);
                    if (add > MIN_REQUEST) {
                        REQUESTED.getAndAddRelease(this, add);
                        upstream.request(add);
                    }
                }
            }
        }
        return batch;
    }

    @Override public void onComplete() {
        if  (moveStateRelease(statePlain(), COMPLETED)) {
            downstream.producerTerminated();
            markDelivered(COMPLETED);
        }
    }

    @Override public void onCancelled() {
        if  (moveStateRelease(statePlain(), CANCELLED)) {
            downstream.producerTerminated();
            markDelivered(CANCELLED);
        }
    }

    @Override public void onError(Throwable cause) {
        if (this.error == UNSET_ERROR)
            this.error = cause;
        if  (moveStateRelease(statePlain(), FAILED)) {
            downstream.producerTerminated();
            markDelivered(FAILED);
        }
    }
}
