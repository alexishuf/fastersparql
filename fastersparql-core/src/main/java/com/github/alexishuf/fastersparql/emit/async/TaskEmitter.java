package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public abstract class TaskEmitter<B extends Batch<B>> extends EmitterService.Task
                                                      implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(TaskEmitter.class);
    private static final VarHandle REQ;
    static {
        try {
            REQ = MethodHandles.lookup().findVarHandle(TaskEmitter.class, "plainRequested", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    protected static final int MUST_AWAKE =  0x80000000;
    protected static final Flags TASK_EMITTER_FLAGS = TASK_FLAGS.toBuilder()
            .flag(MUST_AWAKE, "MUST_AWAKE")
            .build();

    @SuppressWarnings("unused") protected long plainRequested;
    private @MonotonicNonNull Receiver<B> downstream;
    protected final BatchType<B> bt;
    protected short threadId, outCols;
    protected final Vars vars;
    protected final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();
    protected Throwable error = UNSET_ERROR;

    protected TaskEmitter(BatchType<B> batchType, Vars vars,
                          EmitterService runner, int worker,
                          int initState, Flags flags) {
        super(runner, worker, initState, flags);
        this.vars = vars;
        this.bt = batchType;
        int ouCols = vars.size();
        if (ouCols > Short.MAX_VALUE)
            throw new IllegalArgumentException("too many columns");
        this.outCols = (short)ouCols;
        assert flags.contains(TASK_EMITTER_FLAGS);
    }

    @Override protected void doRelease() {
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
        super.doRelease();
    }

    @Override public String toString() { return label(StreamNodeDOT.Label.MINIMAL); }

    @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.empty(); }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type != StreamNodeDOT.Label.MINIMAL)
            appendToSimpleLabel(sb);
        if (type.showState()) {
            sb.append("\nstate=").append(flags.render(state())).append(", requested=");
            StreamNodeDOT.appendRequested(sb, (long)REQ.getOpaque(this));
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    protected void appendToSimpleLabel(StringBuilder out) {}

    @Override public Vars         vars()       { return vars; }
    @Override public BatchType<B> batchType()  { return bt; }


    @Override public void cancel() {
        int st = statePlain();
        if ((st&IS_CANCEL_REQ) != 0 || moveStateRelease(statePlain(), CANCEL_REQUESTING))
            awake();
    }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        int st = lock(statePlain());
        try {
            if ((st & IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (EmitterStats.ENABLED && stats != null)
                ++stats.receivers;
            if (downstream != null && downstream != receiver)
                throw new MultipleRegistrationUnsupportedException(this);
            downstream = receiver;
        } finally {
            unlock(st);
        }
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("subscribed", receiver, "to", this);
    }

    public long requested() { return (long)REQ.getOpaque(this); }

    @Override public void request(long rows) throws NoReceiverException {
        if (rows <= 0)
            return;
        // on first request(), transition from CREATED to LIVE
        if ((statePlain()&IS_INIT) != 0) {
            onFirstRequest();
        }
        if (Async.maxRelease(REQ, this, rows))
            resume();
    }

    @Override public void rebindAcquire() { delayRelease(); }
    @Override public void rebindRelease() { allowRelease(); }

    protected void onFirstRequest() {
        moveStateRelease(statePlain(), ACTIVE);
    }

    protected void resume() { awake(); }

    protected abstract int produceAndDeliver(int state);

    @Override protected void task(int threadId) {
        this.threadId = (short)threadId;
        int st = state(), termState;
        if ((st&IS_PENDING_TERM) != 0) {
            termState = (st&~IS_PENDING_TERM)|IS_TERM;
        } else if ((st&IS_CANCEL_REQ) != 0) {
            termState = CANCELLED;
        } else if ((st&IS_LIVE) != 0) {
            try {
                termState = produceAndDeliver(st);
            } catch (Throwable t) {
                if (this.error == UNSET_ERROR) this.error = t;
                termState = FAILED;
            }
        } else {
            return; // already terminated
        }

        if ((termState&IS_TERM) != 0)
            deliverTermination(st, termState);
        else if ((termState&MUST_AWAKE) != 0)
            awake();
    }

    protected @Nullable B deliver(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        REQ.getAndAddRelease(this, (long)-b.totalRows());
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onBatchDelivered(b);
            return downstream.onBatch(b);
        } catch (Throwable t) {
            Emitters.handleEmitError(downstream, this,
                    (statePlain()&IS_TERM) != 0, t);
            return null;
        }
    }

    @Override protected int resetForRebind(int clearFlags, int setFlags) throws RebindException {
        int state = super.resetForRebind(clearFlags, setFlags);
        error = UNSET_ERROR;
        REQ.setRelease(this, 0L);
        return state;
    }

    protected void deliverTermination(int current, int termState) {
        assert (state()&IS_TERM) == 0 : "deliverTermination() while already terminated";
        if (moveStateRelease(current, termState)) {
            try {
                switch ((termState &STATE_MASK)) {
                    case COMPLETED -> downstream.onComplete();
                    case CANCELLED -> downstream.onCancelled();
                    case FAILED    -> downstream.onError(error);
                }
            } catch (Throwable t) {
                Emitters.handleTerminationError(downstream, this, t);
            }
            markDelivered(current, termState);
        }
    }

}
