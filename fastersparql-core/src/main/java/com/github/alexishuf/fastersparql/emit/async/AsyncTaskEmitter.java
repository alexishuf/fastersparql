package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.HasFillingBatch;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link TaskEmitter} that will emit batches queued via {@link #quickAppend(Orphan)} from its
 * {@link #task(EmitterService.Worker, int)}.
 *
 * <p>Subclasses must signal termination by moving the {@link #state()} to a
 * {@link #IS_PENDING_TERM} state via {@link #moveStateRelease(int, int)}. Subclasses must only
 * do such state transition if they can guarantee there will be no future
 * {@link #quickAppend(Orphan)} call until an eventual {@link #rebind(BatchBinding)}.</p>
 * @param <B>
 * @param <E>
 */
public abstract class AsyncTaskEmitter<B extends Batch<B>, E extends AsyncTaskEmitter<B, E>>
        extends TaskEmitter<B, E> implements HasFillingBatch<B> {
    private @Nullable B queue;

    protected AsyncTaskEmitter(BatchType<B> batchType, Vars vars, int initState, Flags flags) {
        super(batchType, vars, initState, flags);
    }

    @Override protected void doRelease() {
        queue = Batch.safeRecycle(queue, this);
        super.doRelease();
    }

    @Override protected int resetForRebind(int clearFlags, int setFlags) throws RebindException {
        int st = super.resetForRebind(clearFlags, setFlags);
        if ((st&LOCKED_MASK) == 0) lock();
        queue = Batch.safeRecycle(queue, this);
        if ((st&LOCKED_MASK) == 0) unlock();
        return st;
    }

    protected int dropAllQueuedAndGetState() {
        lock();
        queue = Batch.safeRecycle(queue, this);
        return unlock();
    }

    @Override public Orphan<B> pollFillingBatch() {
        B stale = queue; // returning null is cheaper than synchronization
        if (stale != null && stale.next != null && tryLock()) {
            var tail = queue == null ? null : queue.detachDistinctTail();
            unlock();
            return tail;
        }
        return null;
    }

    protected Orphan<B> takeFillingOrCreate() {
        var f = pollFillingBatch();
        return f != null ? f : bt.create(outCols);
    }

    /**
     * Enqueues {@code offer} for future delivery to the downstream {@link Receiver} in a
     * future {@link #task(EmitterService.Worker, int)} execution of this {@link AsyncTaskEmitter}.
     *
     * <p><strong>Important:</strong> this method must not be called after the {@link #state()}
     * has transitioned to {@link #IS_PENDING_TERM}. Subclasses must only move to such state
     * if they can ensure there will be no more batches to deliver.</p>
     *
     * @param offer a batch to enqueue.
     */
    protected void quickAppend(Orphan<B> offer) {
        if ((statePlain()&QUICK_APPEND_ALLOWED) == 0)
            throw new IllegalStateException("bad state for quickAppend()");
        lock();
        queue = Batch.quickAppend(queue, this, offer);
        unlock();
        awakeParallel();
    }
    private static final int QUICK_APPEND_ALLOWED = IS_INIT|IS_LIVE;

    @Override protected boolean mustAwake() {
        return (statePlain()&IS_PENDING_TERM) != 0 || queue != null;
    }

    @Override protected int doPendingTerm(int state) {
        // as per class and quickAppend() javadoc, subclasses must not call quickAppend()
        // after transitioning to an IS_PENDING_TERM state. Therefore, since state originated
        // from a stateAcquire() call, a plain read of this.queue is up-to-date
        if (queue != null)
            return state;
        // else: no queue, advance from IS_PENDING_TERM to IS_TERM/IS_TERM_DELIVERED
        return super.doPendingTerm(state);
    }

    @Override protected int produceAndDeliver(int state) {
        long deadline = Timestamp.nextTick(1);
        B queue;
        do {
            lock();
            queue      = this.queue;
            this.queue = null;
            unlock();
            if (queue == null)
                break; // no work, break here to avoid a contention on nanoTime()
            deliver(queue.releaseOwnership(this));
        } while (Timestamp.nanoTime() < deadline);
        return queue != null || (state&IS_PENDING_TERM) == 0 ? state
                : (state&~IS_PENDING_TERM) | IS_TERM;
    }

}
