package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.EmitterService.Task;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract class TaskEmitter<B extends Batch<B>, E extends TaskEmitter<B, E>>
        extends Task<E>
        implements Emitter<B, E> {
    private static final Logger log = LoggerFactory.getLogger(TaskEmitter.class);
    private static final VarHandle REQ;
    static {
        try {
            REQ = MethodHandles.lookup().findVarHandle(TaskEmitter.class, "plainRequested", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") protected long plainRequested;
    private @MonotonicNonNull Receiver<B> downstream;
    protected final BatchType<B> bt;
    protected short threadId, outCols;
    protected final Vars vars;
    private @Nullable HasFillingBatch<B> downstreamHFB;
    protected final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();
    protected Throwable error = UNSET_ERROR;

    protected TaskEmitter(BatchType<B> batchType, Vars vars, int initState, Flags flags) {
        super(initState, flags);
        this.vars = vars;
        this.bt = batchType;
        int ouCols = vars.size();
        if (ouCols > Short.MAX_VALUE)
            throw new IllegalArgumentException("too many columns");
        this.outCols = (short)ouCols;
    }

    @Override protected void doRelease() {
        if (EmitterStats.LOG_ENABLED && stats != null)
            stats.report(log, this);
        super.doRelease();
    }

    @Override protected void onPendingRelease() {
        cancel();
    }

    @Override public String toString() {
        return label(StreamNodeDOT.Label.SIMPLE).replace('\n', ' ');
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.empty(); }

    protected StringBuilder minimalLabel() {
        return StreamNodeDOT.minimalLabel(new StringBuilder(), this);
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = minimalLabel();
        if (type != StreamNodeDOT.Label.MINIMAL)
            appendToSimpleLabel(sb);
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state())).append(", requested=");
            StreamNodeDOT.appendRequested(sb, (long)REQ.getOpaque(this));
            appendToState(sb);
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    protected void appendToSimpleLabel(StringBuilder out) {}
    protected void appendToState(StringBuilder out) {}

    @Override public Vars              vars() { return vars; }
    @Override public BatchType<B> batchType() { return bt; }
    @Override public boolean     isComplete() { return Stateful.isCompleted(state()); }
    @Override public boolean    isCancelled() { return Stateful.isCancelled(state()); }
    @Override public boolean       isFailed() { return Stateful.isFailed(state()); }
    @Override public boolean   isTerminated() { return (state()&IS_TERM) != 0; }
    @Override public boolean cancel() {
        boolean done = false;
        int st = statePlain();
        if ((st&IS_CANCEL_REQ) != 0 || (done=moveStateRelease(st, CANCEL_REQUESTING)))
            awakeSameWorker();
        return done;
    }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        int st = lock();
        try {
            if ((st & IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (EmitterStats.ENABLED && stats != null)
                ++stats.receivers;
            if (downstream != null && downstream != receiver)
                throw new MultipleRegistrationUnsupportedException(this);
            downstream = receiver;
            //noinspection unchecked
            downstreamHFB = receiver instanceof HasFillingBatch<?> hfb
                          ? (HasFillingBatch<B>)hfb : null;
        } finally {
            unlock();
        }
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("subscribed", receiver, "to", this);
    }

    public long requested() { return (long)REQ.getOpaque(this); }

    @Override public void request(long rows) {
        if (rows <= 0)
            return;
        boolean grown = Async.maxRelease(REQ, this, rows);
        if ((statePlain()&IS_INIT) != 0)
            onFirstRequest(); // atomically transition from INIT to a LIVE state
        if (grown)
            resume(); // awake() the task or producer
    }

    protected void onFirstRequest() {
        moveStateRelease(statePlain(), ACTIVE);
    }

    protected void resume() {
        awake();
    }

    protected abstract int produceAndDeliver(int state);

    protected boolean mustAwake() { return (long)REQ.getOpaque(this) > 0; }

    @Override protected void task(EmitterService.Worker worker, int threadId) {
        this.threadId = (short)threadId;
        int st = stateAcquire();
        if ((st&IS_CANCEL_REQ) != 0)
            st = doCancel(st);
        if ((st&IS_PENDING_TERM) != 0)
            st = doPendingTerm(st);
        int termState = st;
        if ((st&(CAN_PRODUCE_AND_DELIVER)) != 0) {
            try {
                termState = produceAndDeliver(st);
            } catch (Throwable t) {
                if (error == UNSET_ERROR)
                    error = t;
                termState = FAILED;
            }
        }

        if ((termState& IS_TERM_OR_TERM_DELIVERED) == IS_TERM)
            deliverTermination(st, termState);
        else if ((st&IS_TERM_DELIVERED) == 0 && mustAwake())
            awakeSameWorker(worker);
    }
    private static final int CAN_PRODUCE_AND_DELIVER   = IS_LIVE|IS_PENDING_TERM;
    private static final int IS_TERM_OR_TERM_DELIVERED = IS_TERM|IS_TERM_DELIVERED;

    /**
     * Called from {@link #task(EmitterService.Worker, int)} when the state contains the
     * {@link #IS_CANCEL_REQ} bit.
     *
     * @param state the current {@link #state()}
     * @return the updated current {@link #state()}
     */
    protected int doCancel(int state) {
        moveStateRelease(state, CANCELLED);
        return statePlain();
    }

    /**
     * Called from {@link #task(EmitterService.Worker, int)} when the current state has
     * the {@link #IS_PENDING_TERM} bit set.
     *
     * @param state the current {@link #state()}
     * @return the updated current {@link #state()}
     */
    protected int doPendingTerm(int state) {
        moveStateRelease(state, (state&~IS_PENDING_TERM)|IS_TERM);
        return statePlain();
    }

    protected final Orphan<B> beforeDelivery(Orphan<B> orphan) {
        B b = orphan.takeOwnership(this);
        try {
            beforeDelivery(b);
        } catch (Throwable t) {
            b.recycle(this);
            throw t;
        }
        return b.releaseOwnership(this);
    }
    protected final void beforeDelivery(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        REQ.getAndAddRelease(this, (long)-b.totalRows());
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchDelivered(b);
    }

    protected void deliver(Orphan<B> orphan) {
        orphan = beforeDelivery(orphan);
        deliver0(orphan);
    }

    protected final void deliver0(Orphan<B> orphan) {
        try {
            downstream.onBatch(orphan);
        } catch (Throwable t) {
            Emitters.handleEmitError(downstream, this, t, null);
        }
    }

    protected void deliverByCopy(B b) {
        beforeDelivery(b);
        try {
            downstream.onBatchByCopy(b);
        } catch (Throwable t) {
            Emitters.handleEmitError(downstream, this, t, null);
        }
    }

    protected @Nullable Orphan<B> pollDownstreamFillingBatch() {
        return downstreamHFB == null ? null : downstreamHFB.pollFillingBatch();
    }

    @Override protected int resetForRebind(int clearFlags, int setFlags) throws RebindException {
        int state = super.resetForRebind(clearFlags, setFlags);
        error = UNSET_ERROR;
        REQ.setRelease(this, 0L);
        return state;
    }

    protected void deliverTermination(int current, int termState) {
        if (moveStateRelease(current, termState)) {
            try {
                switch ((termState&STATE_MASK)) {
                    case COMPLETED -> downstream.onComplete();
                    case CANCELLED -> downstream.onCancelled();
                    case FAILED    -> downstream.onError(error);
                }
            } catch (Throwable t) {
                Emitters.handleTerminationError(downstream, this, t);
            }
            markDelivered(current, termState);
        } else {
            assert (statePlain()&CANCELLED) != 0
                    : "deliverTermination() after non-cancel previous termination";
        }
    }

}
