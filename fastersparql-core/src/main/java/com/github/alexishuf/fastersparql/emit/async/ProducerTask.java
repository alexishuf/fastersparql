package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.async.AsyncEmitter.CancelledException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;

import static com.github.alexishuf.fastersparql.emit.async.RecurringTaskRunner.TaskResult.DONE;
import static com.github.alexishuf.fastersparql.emit.async.RecurringTaskRunner.TaskResult.RESCHEDULE;

public abstract class ProducerTask<B extends Batch<B>>
        extends RecurringTaskRunner.Task implements Producer {
    private static final Logger log = LoggerFactory.getLogger(ProducerTask.class);
    @SuppressWarnings("FieldMayBeFinal") private static int plainNextId = 1;
    private static final VarHandle S, NEXT_ID;
    static {
        try {
            S = MethodHandles.lookup().findVarHandle(ProducerTask.class, "plainState", State.class);
            NEXT_ID = MethodHandles.lookup().findStaticVarHandle(ProducerTask.class, "plainNextId", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final AsyncEmitter<B> emitter;
    private int id;
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) private State plainState = State.CREATED;
    private @Nullable Throwable error;
    private boolean warnedEmptyBatch;
    private @Nullable B tmp;

    private enum State {
        CREATED,
        LIVE,
        PAUSED,
        CANCEL_REQUESTED,
        CANCELLED,
        COMPLETED,
        FAILED;

        private static final long TRANSITIONS;
        static {
            State[] values = values();
            assert values.length <= 8;
            long bits = 0L;

            for (var src : List.of(CREATED, LIVE, PAUSED)) {
                for (State dst : List.of(LIVE, PAUSED, COMPLETED, FAILED, CANCELLED, CANCEL_REQUESTED)) {
                    if (dst != src)
                        bits |= 1L << (src.ordinal()*8 + dst.ordinal());
                }
            }
            for (State dst : List.of(COMPLETED, FAILED, CANCELLED))
                bits |= 1L << (CANCEL_REQUESTED.ordinal()*8 + dst.ordinal());
            TRANSITIONS = bits;
        }
    }

    public ProducerTask(AsyncEmitter<B> emitter, RecurringTaskRunner runner) {
        super(runner);
        this.emitter = emitter;
    }

    @Override public String toString() {
        if (id == 0)
            id = (int)NEXT_ID.getAndAddRelease(1);
        return "RecurringTaskProducer@"+id+"["+((State)S.getOpaque(this))+"]";
    }

    private boolean moveState(State succ) {
        var pred = (State)S.getAcquire(this);
        while (true) {
            if ((State.TRANSITIONS & (1L << ((pred.ordinal()<<3) + succ.ordinal()))) == 0)
                return false;
            var e = pred;
            if ((pred=(State) S.compareAndExchangeRelease(this, e, succ)) == e)
                return true;
        }
    }

    @Override public final void resume() { if (moveState(State.LIVE            )) awake(); }
    @Override public final void cancel() { if (moveState(State.CANCEL_REQUESTED)) awake(); }

    @Override public boolean isTerminated() {
        return switch ((State) S.getOpaque(this)) {
            case COMPLETED,CANCELLED,FAILED -> true;
            default -> false;
        };
    }

    @Override public final boolean  isComplete() {
        return (State) S.getOpaque(this)==State.COMPLETED;
    }

    @Override public final boolean isCancelled() {
        return (State) S.getOpaque(this)==State.CANCELLED;
    }

    @Override public @Nullable Throwable error() {
        return (State) S.getOpaque(this) == State.FAILED ? error : null;
    }

    /**
     * Produce one batch with at most {@code limit} rows.
     *
     * @param offer If non-null, a batch that can be {@link Batch#clear()}ed, filled with
     *              rows and returned by this method
     * @return a batch with at most {@code limit} rows or null if the producer reached its
     *         natural end and a completion should be propagated to the {@link AsyncEmitter}
     * @throws Throwable if something goes wrong. Once this method throws, it will not be called
     *                   again for this instance, the {@link ProducerTask} will move
     *                   into a failed state and  the {@link Throwable} will be propagated to
     *                   the {@link AsyncEmitter}, which will consolidate it and eventually
     *                   propagate the failure to the downstream {@link Receiver}s
     */
    protected abstract @Nullable B produce(long limit, @Nullable B offer) throws Throwable;

    /**
     * Notifies that {@link #produce(long, Batch)} will not be called anymore and that the
     * producer implementation should release any resources it holds.
     *
     * <p>Implementations can assume that:</p>
     * <ul>
     *     <li>It will never be called concurrently with {@link #produce(long, Batch)}</li>
     *     <li>It will be called at most once per {@link ProducerTask}</li>
     * </ul>
     *
     * @param reason the reason for this call: {@code null} in case of completion ({@code null}
     *               {@link #produce(long, Batch)}), {@link CancelledException} if
     *               due to cancellation, or a non-null {@link Throwable} if terminating due to
     *               an error raised by {@link #produce(long, Batch)}.
     */
    protected abstract void cleanup(@Nullable Throwable reason);

    @Override protected final RecurringTaskRunner.TaskResult task() {
        return switch ((State) S.getAcquire(this)) {
            case CREATED,PAUSED,CANCELLED,COMPLETED,FAILED -> DONE;
            case LIVE -> {
                RecurringTaskRunner.TaskResult result = DONE;
                long demand = emitter.requested();
                if (demand > 0) {
                    try {
                        B tmp = this.tmp;
                        this.tmp = null;
                        tmp = produce(demand, tmp);
                        if (tmp == null) {
                            transitionToTerminal(State.COMPLETED, null);
                        } else if (tmp.rows == 0) {
                            if (!warnedEmptyBatch) {
                                warnedEmptyBatch = true;
                                log.warn("{}: produce() returned empty batch, will not lock subsequent instances", this);
                            }
                            this.tmp = tmp;
                        } else  {
                            try {
                                this.tmp = emitter.offer(tmp);
                                result = RESCHEDULE;
                            } catch (AsyncEmitter.AsyncEmitterStateException e) {
                                this.tmp = tmp;
                                transitionToTerminal(State.CANCELLED, CancelledException.INSTANCE);
                            }
                        }
                    } catch (Throwable t) {
                        fail(t);
                    }
                } else {
                    moveState(State.PAUSED);
                }
                yield result;
            }
            case CANCEL_REQUESTED -> {
                transitionToTerminal(State.CANCELLED, CancelledException.INSTANCE);
                yield DONE;
            }
        };
    }

    private void transitionToTerminal(State dest, @Nullable Throwable reason) {
        if (!moveState(dest))
            return;
        try {
            if (tmp != null)
                tmp = tmp.recycle();
            unload();
            cleanup(reason);
        } catch (Throwable t) {
            log.warn("Ignoring {} during cleanup({}) for {}",
                    t.getClass().getSimpleName(), reason, this);
        }
        emitter.producerTerminated();
    }

    private void fail(Throwable t) {
        if (error == null) error = t;
        transitionToTerminal(State.FAILED, error);
    }
}
