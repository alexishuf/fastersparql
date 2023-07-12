package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.async.AsyncEmitter.CancelledException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;

import static com.github.alexishuf.fastersparql.batch.Timestamp.nextTick;
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

    private @MonotonicNonNull AsyncEmitter<B> emitter;
    private int id;
    @SuppressWarnings({"unused", "FieldMayBeFinal"}) private State plainState = State.CREATED;
    private @Nullable Throwable error;
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

    public ProducerTask(RecurringTaskRunner runner) {
        super(runner);
    }

    /**
     * Calls {@code emitter.registerProducer(this)} and stores the assigned index.
     *
     * @param emitter the {@link AsyncEmitter} to where this producer will send batches
     * @throws IllegalStateException if this method has been previously called with another
     *                               {@code emitter}
     */
    public void registerOn(AsyncEmitter<B> emitter) {
        if (this.emitter != null) {
            if (this.emitter == emitter) return;
            throw new IllegalStateException("Already registered to another emitter");
        }
        this.emitter = emitter;
        emitter.registerProducer(this);
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
        return (State) S.getAcquire(this) == State.FAILED ? error : null;
    }

    /**
     * Produce one batch with at most {@code limit} rows.
     *
     * <p>Implementations MUST NOT block. Tasks run on a small fixed-size thread pool and
     * blocking on results produced by another task of that pool might lead to a deadlock.
     * Work-stealing might very slowly recover from some deadlocks, but if all threads in the pool
     * block, it will be non-recoverable.</p>
     *
     * <p>There is no preemption and implementations should try to respect the provided
     * {@code deadline}, lest other tasks might observe problematic latencies. Likewise,
     * implementations should not try to return ASAP before the deadline, as doing so would break
     * CPU instruction and data locality and increase the overhead associated with task
     * scheduling.</p>
     *
     * @param limit The returned batch should contain no more than {@code limit} rows. This limit
     *              may be exceeded if not exceeding it would take more time than otherwise
     *              (i.e., returning a ready batch exceeding the limit instead of slicing it
     *              into two and returning the first half)
     * @param deadline This call should return once {@link Timestamp#nanoTime()} exceeds
     *                 {@code deadline}, even if zero rows have been obtained. An implementation
     *                 may exceed the deadline if tracking the deadline more precisely or
     *                 preempting computation in of a row would be inefficient. Nevertheless,
     *                 implementations should avoid unbounded computation as they could starve
     *                 other tasks, including tasks on which this producer might depend.
     * @param offer If non-null, a batch whose ownership is ceded by the caller. Implementations
     *              may fill {@code offer} and return it or swap it by another already filled
     *              batch.
     * @return a batch with at most {@code limit} rows. An empty batch does not imply
     *         {@link #exhausted()} and the producer may return a non-empty batch and still become
     *         {@link #exhausted()}
     *
     * @throws Throwable if something goes wrong. Once this method throws, it will not be called
     *                   again for this instance. The {@link ProducerTask} will move
     *                   into a failed state and  the {@link Throwable} will be propagated to
     *                   the {@link AsyncEmitter}, which will consolidate it and eventually
     *                   propagate the failure to the downstream {@link Receiver}s
     */
    protected abstract B produce(long limit, long deadline, @Nullable B offer)
            throws Throwable;

    protected enum ExhaustReason {
        CANCELLED,
        COMPLETED
    }

    /**
     * Checks, with minimal cost, whether {@link #produce(long, long, Batch)} should be called
     * to fetch more rows.
     *
     * <p>This should be implemented to avoid computation or blocking. Ideally, it should be
     * the {@link #produce(long, long, Batch)} implementation that detects exhaustion of the
     * source or cancellation, while this method simply checks a flag. This method exists simply
     * because simulating multi-value returns in Java is not efficient.</p>
     *
     * This method MAY detect a cancellation due to {@link #cleanup(Throwable)} but
     * implementations are NOT REQUIRED to do so and can return {@code null} after a
     * {@link #cleanup(Throwable)}. </p>
     *
     * @return {@code null} if this producer is not certainly exhausted (a future
     *         {@link #produce(long, long, Batch)} MAY yield a non-empty batches), else returns
     *         the reason for exhaustion: successful or out-of-band cancellation not caused by
     *         {@link #cleanup(Throwable)}.
     */
    protected abstract ExhaustReason exhausted();

    /**
     * Notifies that {@link #produce(long, long, Batch)} will not be called anymore and that the
     * producer implementation should release any resources it holds.
     *
     * <p>Implementations can assume that:</p>
     * <ul>
     *     <li>It will never be called concurrently with {@link #produce(long, long, Batch)}
     *         or {@link #exhausted()}</li>
 *         <li>{@link #produce(long, long, Batch)} and {@link #exhausted()} will not be called
     *         after this method</li>
     *     <li>It will be called at most once per {@link ProducerTask}</li>
     * </ul>
     *
     * @param reason the reason for this call: {@code null} in case of completion ({@code null}
     *               {@link #produce(long, long, Batch)}), {@link CancelledException} if
     *               due to cancellation, or a non-null {@link Throwable} if terminating due to
     *               an error raised by {@link #produce(long, long, Batch)}.
     */
    protected abstract void cleanup(@Nullable Throwable reason);

    @Override protected final RecurringTaskRunner.TaskResult task() {
        return switch ((State) S.getAcquire(this)) {
            case CREATED,PAUSED,CANCELLED,COMPLETED,FAILED -> DONE;
            case LIVE -> {
                RecurringTaskRunner.TaskResult result = DONE;
                long demand = emitter.requested();
                if (demand > 0) {
                    B tmp = this.tmp;
                    this.tmp = null;
                    try {
                        tmp = produce(demand, 1023|nextTick(), tmp);
                        try {
                            this.tmp = emitter.offer(tmp);
                            result = RESCHEDULE;
                        } catch (AsyncEmitter.AsyncEmitterStateException e) {
                            this.tmp = tmp;
                            transitionToTerminal(State.CANCELLED, CancelledException.INSTANCE);
                        }
                        State tgt = switch (exhausted()) {
                            case null -> null;
                            case CANCELLED -> State.CANCELLED;
                            case COMPLETED -> State.COMPLETED;
                        };
                        if (tgt != null) {
                            result = DONE;
                            transitionToTerminal(tgt, null);
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
