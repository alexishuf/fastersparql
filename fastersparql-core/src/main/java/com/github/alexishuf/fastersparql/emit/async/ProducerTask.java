package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BatchQueue.QueueStateException;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.Timestamp.nextTick;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract class ProducerTask<B extends Batch<B>>
        extends EmitterService.Task implements Producer<B> {

    private @MonotonicNonNull AsyncEmitter<B> downstream;
    private Throwable error = UNSET_ERROR;
    private @Nullable B tmp;

    protected ProducerTask(EmitterService runner) {
        super(runner, RR_WORKER, CREATED, TASK_FLAGS);
    }

    protected ProducerTask(EmitterService runner, int preferredWorker) {
        super(runner, preferredWorker, CREATED, TASK_FLAGS);
    }

    @Override public void registerOn(AsyncEmitter<B> emitter) {
        if (this.downstream != null) {
            if (this.downstream == emitter) return;
            throw new IllegalStateException("Already registered to another emitter");
        }
        this.downstream = emitter;
        emitter.registerProducer(this);
    }

    @Override public void forceRegisterOn(AsyncEmitter<B> emitter) throws IllegalStateException {
        int state = lock(statePlain());
        try {
            if ((state&STATE_MASK) != CREATED)
                throw new IllegalStateException("Already started");
            this.downstream = emitter;
            emitter.registerProducer(this);
        } finally { unlock(state); }
    }

    @Override public String toString() {
        return String.format("%s@%x", getClass().getSimpleName(), System.identityHashCode(this));
    }

    @Override public final void resume() {
        if (downstream == null) throw new NoDownstreamException(this);
        if (moveStateRelease(statePlain(), ACTIVE)) awake();
    }
    @Override public final void cancel() {
        if (downstream == null) throw new NoDownstreamException(this);
        if (moveStateRelease(statePlain(), CANCEL_REQUESTED)) awake();
    }

    @Override public void rebindAcquire() {delayRelease();}
    @Override public void rebindRelease() {allowRelease();}

    @Override public final boolean isTerminated() { return (state()&IS_TERM) != 0; }
    @Override public final boolean  isComplete()  { return isCompleted(state()); }
    @Override public final boolean isCancelled()  { return isCancelled(state()); }
    @Override public @Nullable Throwable error()  { return isFailed(stateAcquire()) ? error : null; }

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
     * @return {@code null} or a batch with at most {@code limit} rows. An empty batch or
     *          {@code null} doe not imply {@link #exhausted()}. Likewise, a non-empty return
     *          from this method may be followed by a non-null return from {@link #exhausted()}
     *
     * @throws Throwable if something goes wrong. Once this method throws, it will not be called
     *                   again for this instance. The {@link ProducerTask} will move
     *                   into a failed state and  the {@link Throwable} will be propagated to
     *                   the {@link AsyncEmitter}, which will consolidate it and eventually
     *                   propagate the failure to the downstream {@link Receiver}s
     */
    protected abstract @Nullable B produce(long limit, long deadline, @Nullable B offer)
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
     * <p>A previous {@link #cancel()} MAY cause this to return FAILED</p>
     *
     * @return {@code null} if this producer is not certainly exhausted (a future
     *         {@link #produce(long, long, Batch)} MAY yield a non-empty batches), else returns
     *         the reason for exhaustion.
     */
    protected abstract ExhaustReason exhausted();

    @Override protected final void task() {
        int state = stateAcquire(), termState = 0;
        if ((state& IS_CANCEL_REQ) != 0) {
            termState = CANCELLED;
        } else if ((state&STATE_MASK) == ACTIVE) {
            long demand = downstream.requested();
            if (demand > 0) {
                B tmp = this.tmp;
                assert tmp == null || tmp.assertUnpooled();
                this.tmp = null;
                try {
                    tmp = produce(demand, 1023|nextTick(), tmp);
                    termState = ACTIVE;
                    try {
                        this.tmp = downstream.offer(tmp);
                    } catch (QueueStateException e) {
                        this.tmp = tmp;
                        termState = CANCELLED;
                    }
                    termState = switch (exhausted()) {
                        case null -> termState;
                        case CANCELLED -> CANCELLED;
                        case COMPLETED -> COMPLETED;
                    };
                } catch (Throwable t) {
                    terminate(state, FAILED, t);
                }
            } else {
                moveStateRelease(state, PAUSED);
            }
        }

        if      ( termState          == ACTIVE) awake();
        else if ((termState&IS_TERM) !=      0) terminate(state, termState, null);
    }

    private void terminate(int current, int termState, Throwable cause) {
        if (cause != null && this.error == UNSET_ERROR) this.error = cause;
        if (moveStateRelease(current, termState&~IS_TERM_DELIVERED)) {
            downstream.producerTerminated();
            markDelivered(current, termState);
        }
    }
}
