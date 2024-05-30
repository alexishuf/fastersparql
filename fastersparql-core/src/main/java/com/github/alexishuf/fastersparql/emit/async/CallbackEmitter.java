package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.Batch.detachDistinctTail;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static com.github.alexishuf.fastersparql.util.concurrent.Timestamp.nanoTime;

/**
 * A {@link Emitter} that implements {@link CompletableBatchQueue}.
 *
 * <p>Events to {@link Receiver} are delivered from within a {@link EmitterService} thread. Events
 * from the producer can arrive concurrently, (including among themselves) from any thread.
 * However the implementation is optimized for better performance when {@link #offer(Orphan)}
 * arrives from a single thread.</p>
 *
 * <p>Events to the underlying producer are delivered via the
 * {@link #startProducer()}/{@link #resumeProducer(long)}/{@link #pauseProducer()}/
 * {@link #cancelProducer()}/{@link #earlyCancelProducer()}/{@link #releaseProducer()}
 * methods. All these methods are also delivered from within the same {@link EmitterService}
 * worker thread and thus are never called concurrently. Without a {@link #cancel()}, the
 * lifecycle of a {@link CallbackEmitter} is the following:</p>
 *
 * <ol>
 *     <li>An unknown thread calls {@link #request(long)} for the first time</li>
 *     <li>{@link #startProducer()} is called (from the emitter thread)</li>
 *     <li>Repeatedly:
 *         <ol>
 *             <li>{@link #resumeProducer(long)} is called from the emitter thread</li>
 *             <li>{@link #offer(Orphan)} gets called from an unknown thread</li>
 *             <li>One or both concurrently happen:
 *                 <ol>
 *                     <li>{@link #pauseProducer()} is called from the emitter thread because
 *                         the producer has delivered the number of rows requested in
 *                         {@link #resumeProducer(long)}.</li>
 *                     <li>{@link #complete(Throwable)} is called by the producer from
 *                         an unknown thread</li>
 *                 </ol>
 *             </li>
 *         </ol>
 *     </li>
 *     <li>Once the {@link CallbackEmitter} reaches a {@code _DELIVERED} state (termination has
 *         been signaled downstream) and the downstream {@link Receiver} has called
 *         {@link #recycle(Object)} on the {@link CallbackEmitter}, then {@link #releaseProducer()}
 *         will be called from the emitter thread.</li>
 * </ol>
 *
 *  <p>With the presence of {@link #cancel()}, which can arrive at any moment from any thread,
 *  there are two possibilities:</p>
 *  <ul>
 *      <li>If the emitter thread observes the cancel before {@link #startProducer()} gets called,
 *          {@link #earlyCancelProducer()} is called. {@link #startProducer()}/
 *          {@link #resumeProducer(long)}/{@link #pauseProducer()} will not be called</li>
 *      <li>If the emitter thread observes tha cancel after {@link #startProducer()} got called,
 *          {@link #cancelProducer()} will be called. There will be no
 *          {@link #resumeProducer(long)}/{@link #pauseProducer()} calls.</li>
 *  </ul>
 *
 *  <p>Although {@link #cancel(boolean)} does not transition the emitter state, if
 *  {@link #cancel()} <i>happens before</i> a {@link #offer(Orphan)} call, the offer will throw a
 *  {@link CancelledException}. Batches queued before the {@link #cancel()} will still be
 *  delivered before the termination of the emitter. To change this behavior, call
 *  {@link #dropAllQueued()} from an override of {@link #cancel()}</p>
 *
 *  Remember that {@link #rebind(BatchBinding)} always happens after entry into
 *  {@link Receiver#onComplete()}/{@link Receiver#onCancelled()}/{@link Receiver#onError(Throwable)}
 *  and thus after all {@code *Producer()} methods except for {@link #releaseProducer()}. Also
 *  note that {@link #rebind(BatchBinding)} resets the emitter state, allowing a new
 *  {@link #startProducer()} call which restarts the lifecycle described above.
 *
 * @param <B>
 */
public abstract class CallbackEmitter<B extends Batch<B>, E extends CallbackEmitter<B, E>>
        extends TaskEmitter<B, E>
        implements CompletableBatchQueue<B> {
    private   static final int PROD_LIVE      = 0x40000000;
    /** This flag is on if {@link #cancel()} was called and this emitter had no
     *  {@link #rebind(BatchBinding)} since then */
    protected static final int GOT_CANCEL_REQ = 0x20000000;
    private   static final int DONE_CANCEL    = 0x10000000;
    private   static final int DO_RESUME      = 0x08000000;
    private   static final int PAUSED         = 0x04000000;

    protected static int CLEAR_ON_REBIND = PROD_LIVE|GOT_CANCEL_REQ |DONE_CANCEL|DO_RESUME;

    protected static final Flags CB_FLAGS = TASK_FLAGS.toBuilder()
            .flag(PROD_LIVE,         "PROD_LIVE")
            .flag(GOT_CANCEL_REQ,    "GOT_CANCEL_REQ")
            .flag(DONE_CANCEL,       "DONE_CANCEL")
            .flag(DO_RESUME,         "DO_RESUME")
            .flag(PAUSED,            "PAUSED")
            .build();

    private @Nullable B queue;
    private int avgRows;

    public CallbackEmitter(BatchType<B> batchType, Vars vars, EmitterService runner,
                           int initState, Flags flags) {
        super(batchType, vars, runner, initState|PAUSED, flags);
        assert flags.contains(CB_FLAGS) : "missing CB_FLAGS";
    }

    @Override protected void doRelease() {
        try {
            assert (statePlain()&RELEASED_MASK) != 0 : "not released";
            queue = Owned.safeRecycle(queue, this);
        } finally {
            try {
                super.doRelease();
            } finally {
                releaseProducer();
            }
        }
    }

    /**
     * Start the producer so it can start calling {@link #offer(Orphan)} and eventually
     * {@link #complete(Throwable)}.
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread after the first
     * {@link #request(long)}</p>
     */
    protected abstract void startProducer();

    /**
     * Causes calls to {@link #offer(Orphan)} to stop sometime after entry into this
     * method. This method undoes and is undone by {@link #resumeProducer(long)}.
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread and always after a
     * {@link #resumeProducer(long)}.</p>
     */
    protected abstract void pauseProducer();

    /**
     * Causes calls to {@link #offer(Orphan)} and to resume at some point after the entry
     * into this method. This method undoes and is undone by {@link #pauseProducer()}
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread and always after a
     * {@link #startProducer()} or {@link #pauseProducer()}.</p>
     *
     * @param requested the producers should produce at least this many rows or complete
     *                  normally if it reached the end before achieving this number. If the
     *                  producer stops producing before reaching this count and does not terminate,
     *                  the downstream {@link Receiver} may starve. See {@link #requested()}.
     */
    protected abstract void resumeProducer(long requested);

    /**
     * Make the producer eventually stop and  call {@link #cancel(boolean)} with {@code ack=true}.
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread and always after a
     * {@link #startProducer()}</p>
     */
    protected abstract void cancelProducer();

    /**
     * Notify that a {@link #cancel()} or {@link #cancel(boolean)} with {@code ack=false}
     * <i>happened before</i> the first {@link #request(long)}. Therefore, there will be no
     * {@link #startProducer()}.
     *
     * <p>Doing nothing is a valid implementation of this method. The producer should not
     * invoke {@link #cancel(boolean)} or {@link #complete(Throwable)} during this call or
     * in response to it.</p>
     */
    protected abstract void earlyCancelProducer();

    /**
     * Called one time per {@link CallbackEmitter} to notify that the emitter has been released:
     * no future {@link #startProducer()} calls will occur since {@link #rebind(BatchBinding)}
     * cannot be called on the emitter. Implementations should release resources held by
     * the producer at this point.
     *
     * <p><strong>Attention:</strong> this method may be called from the {@link EmitterService}
     * worker thread that just delivered a termination to the downstream receiver or from an
     * unknown thread that called {@link #recycle(Object)}. Nevertheless:</p>
     *
     * <ul>
     *     <li>This method will be called only once per {@link CallbackEmitter}</li>
     *     <li>No {@code *Producer()} method will be called concurrently or after this call</li>
     * </ul>
     */
    protected abstract void releaseProducer();

    /**
     * Drop all batches queued in this {@link CallbackEmitter} by an {@link #offer(Orphan)} that
     * <i>happened before</i> this call.
     */
    @SuppressWarnings("unused") protected void dropAllQueued() {
        B q;
        lock();
        try {
            q     = queue;
            queue = null;
        } finally { unlock(); }
        Batch.recycle(q, this);
    }

    @Override public @Nullable Throwable error() {
        return (state()&~IS_TERM_DELIVERED) == FAILED ? error : null;
    }

    @Override public boolean complete(@Nullable Throwable cause) {
        int st = state(), tgt;
        if (cause == null) {
            tgt = (st&IS_CANCEL_REQ) == 0 ? PENDING_COMPLETED : PENDING_CANCELLED;
        } else {
            if (error == UNSET_ERROR) error = cause;
            tgt = PENDING_FAILED;
        }
        if (moveStateRelease(st, tgt)) {
            awakeParallel();
            return true;
        } else {
            journal("move to ", tgt, flags, "from", statePlain(), flags, "rejected for", this);
            return false;
        }
    }

    @Override protected final void resume() {
        setFlagsRelease(DO_RESUME);
        awake();
    }

    @Override public boolean cancel() {
        boolean first;
        B queue;
        int st = lock();
        first = (st&GOT_CANCEL_REQ) == 0;
        queue = this.queue;
        this.queue = null;
        st = unlock(0, GOT_CANCEL_REQ);
        if (first)
            awakeSameWorker();
        Batch.safeRecycle(queue, this);
        return (st&IS_TERM) == 0;
    }

    @Override public boolean cancel(boolean ack) {
        if (ack) {
            boolean done = moveStateRelease(statePlain(), CANCEL_REQUESTING);
            if (done)
                awakeParallel();
            return done;
        } else {
            return cancel();
        }
    }

    @Override public @Nullable Orphan<B> pollFillingBatch() {
        Orphan<B> tail;
        B stale = queue; // returning null is cheaper than synchronization
        if (stale == null || stale.next == null || !tryLock())
            return null; // no queue, no tail or contended lock
        try {
            if ((tail=detachDistinctTail(queue)) != null && EmitterStats.ENABLED && stats != null)
                stats.revertOnBatchReceived(tail);
        } finally { unlock(); }
        return tail;
    }

    @Override public Orphan<B> fillingBatch() {
        var orphan = pollFillingBatch();
        return orphan == null ? bt.createForThread(threadId, outCols) : orphan;
    }

    @Override public void offer(Orphan<B> offer) throws TerminatedException, CancelledException {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(offer);
        int rows = Batch.peekRows(offer);
        if (rows == 0) {
            Orphan.recycle(offer);
            return;
        }
        int st = lock();
        try {
            if ((st&(IS_TERM|IS_CANCEL_REQ|GOT_CANCEL_REQ)) == 0) {
                B tail = detachDistinctTail(queue, this);
                if (tail != null) {
                    unlock();
                    tail.append(offer);
                    offer = tail.releaseOwnership(this);
                    lock();
                }
                queue = Batch.quickAppend(queue, this, offer);
            } else {
                Orphan.recycle(offer); // recycle before throwing
                if (isCancelled(st) || (st&(IS_CANCEL_REQ))!=0)
                    throw CancelledException.INSTANCE;
                else if ((st&IS_TERM) != 0)
                    throw TerminatedException.INSTANCE;
                // else: cancel() was called, waiting for an ack via cancel(true)
                //       throwing could scare the producer away from delivering the ack
            }
        } finally { unlock(); }
        awakeParallel();
    }

    @Override protected void task(EmitterService.Worker worker, int threadId) {
        this.threadId = (short)threadId;
        int st = lock(), termState = 0;
        try {
            if ((st&(GOT_CANCEL_REQ|DONE_CANCEL)) == GOT_CANCEL_REQ)
                st = doCancel(st);
            if ((st&(IS_LIVE|PROD_LIVE|GOT_CANCEL_REQ)) == IS_LIVE)
                st = doStartProducer(st);
            if ((st&(IS_LIVE|DO_RESUME|GOT_CANCEL_REQ)) == (IS_LIVE|DO_RESUME))
                st = doResume();
            if ((st&(IS_TERM_DELIVERED|IS_INIT)) != 0)
                return; // no work to do
            long deadline = Timestamp.nextTick(1);
            for (boolean quick = true; quick; quick = nanoTime() <= deadline) {
                B b   = queue;
                queue = null;
                if (b == null)
                    break;
                st = unlock();
                if (b.rows == 0) {
                    b.recycle(this);
                } else {
                    avgRows = ((avgRows<<4) - avgRows + b.rows) >> 4;
                    deliver(b.releaseOwnership(this));
                }
                st = lock();
            }
            if (queue != null)
                awakeSameWorker(worker);
            if (requested() <= 0 && (st&PAUSE_MASK) == PAUSE_VALUE)
                st = doPause();
            if (queue == null) {
                termState =  (st&IS_CANCEL_REQ)   != 0 ? CANCELLED
                              : ((st&IS_PENDING_TERM) != 0 ? (st&~IS_PENDING_TERM)|IS_TERM : 0);
                if (termState != 0) {
                    if ((st&LOCKED_MASK) != 0)
                        st = unlock();
                    deliverTermination(st, termState);
                }
            }
        } catch (Throwable t) {
            if (termState != 0 || (st&IS_TERM) != 0)
                throw t;
            if ((st&LOCKED_MASK) != 0)
                st = unlock();
            if (error == UNSET_ERROR)
                error = t;
            deliverTermination(st, FAILED);
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock();
        }
    }

    private static final int PAUSE_MASK  = IS_LIVE|GOT_CANCEL_REQ|PAUSED;
    private static final int PAUSE_VALUE = IS_LIVE;

    protected int doCancel(int st) {
        if ((st&DONE_CANCEL) != 0)
            return st;
        var queue = this.queue;
        this.queue = null;
        st = unlock(0, DONE_CANCEL);
        try {
            Batch.safeRecycle(queue, this);
            if ((st&PROD_LIVE) == 0) {
                earlyCancelProducer();
                moveStateRelease(st, CANCEL_REQUESTED);
            } else {
                cancelProducer(); // producer will call cancel(true) or complete(cause)
            }
        } finally { st = lock(); }
        return st;
    }

    private int doStartProducer(int st) {
        assert (st&PROD_LIVE) == 0 : "producer already started";
        unlock(0, PROD_LIVE);
        try {
            startProducer();
        } finally { st = lock(); }
        return st;
    }

    private int doPause() {
        int st;
        unlock(0, PAUSED);
        try {
            pauseProducer();
        } finally { st = lock(); }
        return st;
    }

    private int doResume() {
        long n = requested();
        if (n <= 0)
            return statePlain();
        int st;
        unlock(DO_RESUME|PAUSED, 0);
        try {
            resumeProducer(n);
        } finally { st = lock(); }
        return st;
    }

    @Override protected int produceAndDeliver(int state) {
        throw new UnsupportedOperationException();
    }
}
