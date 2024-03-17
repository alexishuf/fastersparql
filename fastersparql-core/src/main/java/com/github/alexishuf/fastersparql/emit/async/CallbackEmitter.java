package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

/**
 * A {@link Emitter} that implements {@link CompletableBatchQueue}.
 *
 * <p>Events to {@link Receiver} are delivered from within a {@link EmitterService} thread. Events
 * from the producer can arrive concurrently, (including among themselves) from any thread.
 * However the implementation is optimized for better performance when {@link #offer(Batch)}
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
 *             <li>{@link #offer(Batch)} gets called from an unknown thread</li>
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
 *     <li>If there is no outstanding {@link #rebindAcquire()}, the emitter will be
 *         released and {@link #releaseProducer()} will be called from the emitter thread.</li>
 * </ol>
 *
 *  <p>With the presence of {@link #cancel()}, which can arrive at any moment from any thread,
 *  there are two possibilities:</p>
 *  <ul>
 *      <li>If the emitter thread observes the cancel before {@link #startProducer()} gets called,
 *          {@link #earlyCancelProducer()} is called. {@link #startProducer()}/
 *          {@link #resumeProducer(long)}/{@link #pauseProducer()} will not be called</li>
 *      <li>If the emiter thread observes tha cancel after {@link #startProducer()} got called,
 *          {@link #cancelProducer()} will be called. There will be no
 *          {@link #resumeProducer(long)}/{@link #pauseProducer()} calls.</li>
 *  </ul>
 *
 *  <p>Although {@link #cancel(boolean)} does not transition the emitter state, if
 *  {@link #cancel()} <i>happens before</i> a {@link #offer(Batch)} call, the offer will throw a
 *  {@link CancelledException}. Batches queued before the {@link #cancel()} will still be
 *  delivered before the termination of the emitter. To change this behavior, call
 *  {@link #dropAllQueued()} from an override of {@link #cancel()}</p>
 *
 *  Remember that {@link #rebind(BatchBinding)} always happens after entry into
 *  {@link Receiver#onComplete()}/{@link Receiver#onCancelled()}/{@link Receiver#onError(Throwable)}
 *  and thus after all {@code *Producer()} methods except for {@link #releaseProducer()}. Also
 *  note that {@link #rebind(BatchBinding)} resets the emitter state, allowin a new
 *  {@link #startProducer()} call which restarts the lifecycle described above.
 *
 * @param <B>
 */
public abstract class CallbackEmitter<B extends Batch<B>> extends TaskEmitter<B>
        implements CompletableBatchQueue<B> {
    private   static final int PROD_LIVE      = 0x40000000;
    /** This flag is on if {@link #cancel()} was called and this emitter had no
     *  {@link #rebind(BatchBinding)} since then */
    protected static final int GOT_CANCEL_REQ = 0x20000000;
    private   static final int DONE_CANCEL    = 0x10000000;
    private   static final int DO_RESUME      = 0x08000000;

    protected static int CLEAR_ON_REBIND = PROD_LIVE|GOT_CANCEL_REQ |DONE_CANCEL|DO_RESUME;

    protected static final Flags CB_FLAGS = TASK_FLAGS.toBuilder()
            .flag(PROD_LIVE,         "PROD_LIVE")
            .flag(GOT_CANCEL_REQ,    "GOT_CANCEL_REQ")
            .flag(DONE_CANCEL,       "DONE_CANCEL")
            .flag(DO_RESUME,         "DO_RESUME")
            .build();

    private @Nullable B ready, filling;
    private int avgRows;

    public CallbackEmitter(BatchType<B> batchType, Vars vars, EmitterService runner, int worker,
                           int initState, Flags flags) {
        super(batchType, vars, runner, worker, initState, flags);
        assert flags.contains(CB_FLAGS) : "missing CB_FLAGS";
    }

    @Override protected void doRelease() {
        try {
            assert (statePlain()&RELEASED_MASK) != 0 : "not released";
            ready   = bt.recycle(ready);
            filling = bt.recycle(filling);
        } finally {
            try {
                super.doRelease();
            } finally {
                releaseProducer();
            }
        }
    }

    /**
     * Start the producer so it can start calling {@link #offer(Batch)} and eventually
     * {@link #complete(Throwable)}.
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread after the first
     * {@link #request(long)}</p>
     */
    protected abstract void startProducer();

    /**
     * Causes calls to {@link #offer(Batch)} to stop sometime after entry into this
     * method. This method undoes and is undone by {@link #resumeProducer(long)}.
     *
     * <p>See the lifecycle described at {@link CallbackEmitter} documentation.
     * This method will always be called from the emitter thread and always after a
     * {@link #resumeProducer(long)}.</p>
     */
    protected abstract void pauseProducer();

    /**
     * Causes calls to {@link #offer(Batch)} and to resume at some point after the entry
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
     * unknown thread that called {@link #rebindRelease()}. Nevertheless:</p>
     *
     * <ul>
     *     <li>This method will be called only once per {@link CallbackEmitter}</li>
     *     <li>No {@code *Producer()} method will be called concurrently or after this call</li>
     * </ul>
     */
    protected abstract void releaseProducer();

    /**
     * Drop all batches queued in this {@link CallbackEmitter} by an {@link #offer(Batch)} that
     * <i>happened before</i> this call.
     */
    @SuppressWarnings("unused") protected void dropAllQueued() {
        B b0, b1;
        int st = lock(statePlain());
        try {
            b0      = ready;
            b1      = filling;
            ready   = null;
            filling = null;
        } finally { unlock(st); }
        bt.recycle(b0);
        bt.recycle(b1);
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
            awake();
            return true;
        } else {
            journal("move to ", tgt, flags, "from", statePlain(), flags, "rejected for", this);
            return false;
        }
    }

    @Override protected final void resume() {
        setFlagsRelease(statePlain(), DO_RESUME);
        awake();
    }

    @Override public boolean cancel() {
        boolean first;
        int st = lock(statePlain());
        first = (st&GOT_CANCEL_REQ) == 0;
        st = unlock(st, 0, GOT_CANCEL_REQ);
        if (first)
            awake();
        return (st&IS_TERM) == 0;
    }

    @Override public boolean cancel(boolean ack) {
        if (ack) {
            boolean done = moveStateRelease(statePlain(), CANCEL_REQUESTING);
            if (done)
                awake();
            return done;
        } else {
            return cancel();
        }
    }

    @Override public B fillingBatch() {
        int st = lock(statePlain());
        try {
            B f = filling;
            if (f == null) f = bt.createForThread(threadId, outCols);
            else           filling = null;
            return f;
        } finally { unlock(st); }
    }

    public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        if (b == null || b.rows == 0)
            return b;
        b.requireUnpooled();
        int st = lock(statePlain());
        try {
            if ((st&(IS_TERM|IS_CANCEL_REQ|GOT_CANCEL_REQ)) == 0) {
                if (ready == null) ready = b;
                else               filling = Batch.quickAppend(filling, b);
            } else {
                bt.recycle(b);
                if (isCancelled(st) || (st&(IS_CANCEL_REQ|GOT_CANCEL_REQ))!=0)
                    throw CancelledException.INSTANCE;
                else
                    throw TerminatedException.INSTANCE;
            }
        } finally {
            unlock(st);
        }
        awake();
        return null;
    }

    @Override protected void task(int threadId) {
        this.threadId = (short)threadId;
        int st = lock(state()), termState = 0;
        try {
            if ((st&(GOT_CANCEL_REQ|DONE_CANCEL)) == GOT_CANCEL_REQ)
                st = doCancel(st);
            if ((st&(IS_LIVE|PROD_LIVE|GOT_CANCEL_REQ)) == IS_LIVE)
                st = doStartProducer(st);
            if ((st&(IS_LIVE|DO_RESUME|GOT_CANCEL_REQ)) == (IS_LIVE|DO_RESUME))
                st = doResume(st);
            if ((st&(IS_TERM_DELIVERED|IS_INIT)) != 0)
                return; // no work to do
            long deadline = Timestamp.nextTick(1);
            while (ready != null) {
                B b     = ready;
                ready   = filling;
                filling = null;
                st = unlock(st);
                avgRows = ((avgRows<<4) - avgRows + b.rows) >> 4;
                if (b.rows > 0 && (b = deliver(b)) != null)
                    b.recycle();
                if (Timestamp.nanoTime() > deadline) {
                    awake();
                    break;
                }
                st = lock(st);
            }
            if (requested() <= 0 && (st&(GOT_CANCEL_REQ)) != 0)
                pauseProducer();
            if (ready == null) {
                assert filling == null : "ready == null but filling != null";
                termState =  (st&IS_CANCEL_REQ)   != 0 ? CANCELLED
                              : ((st&IS_PENDING_TERM) != 0 ? (st&~IS_PENDING_TERM)|IS_TERM : 0);
                if (termState != 0) {
                    if ((st&LOCKED_MASK) != 0)
                        st = unlock(st);
                    deliverTermination(st, termState);
                }
            }
        } catch (Throwable t) {
            if (termState != 0 || (st&IS_TERM) != 0)
                throw t;
            if ((st&LOCKED_MASK) != 0)
                st = unlock(st);
            if (error == UNSET_ERROR)
                error = t;
            deliverTermination(st, FAILED);
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock(st);
        }
    }

    private int doCancel(int st) {
        if ((st&DONE_CANCEL) != 0)
            return st;
        st = unlock(st, 0, DONE_CANCEL);
        try {
            if ((st&PROD_LIVE) == 0) {
                earlyCancelProducer();
                if (moveStateRelease(st, CANCEL_REQUESTED))
                    st = (st&FLAGS_MASK) | CANCEL_REQUESTED;
            } else {
                cancelProducer(); // producer will call cancel(true) or complete(cause)
            }
        } finally { st = lock(st); }
        return st;
    }

    private int doStartProducer(int st) {
        assert (st&PROD_LIVE) == 0 : "producer already started";
        st = unlock(st, 0, PROD_LIVE);
        try {
            startProducer();
        } finally { st = lock(st); }
        return st;
    }

    private int doResume(int st) {
        st = unlock(st, DO_RESUME, 0);
        try {
            long n = requested();
            if (n > 0)
                resumeProducer(n);
        } finally { st = lock(st); }
        return st;
    }

    @Override protected int produceAndDeliver(int state) {
        throw new UnsupportedOperationException();
    }
}
