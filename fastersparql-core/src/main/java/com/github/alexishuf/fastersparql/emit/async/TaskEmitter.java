package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.THREAD_JOURNAL;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public abstract class TaskEmitter<B extends Batch<B>> extends EmitterService.Task
                                                      implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(TaskEmitter.class);
    protected static final VarHandle REQUESTED;
    static {
        try {
            REQUESTED = MethodHandles.lookup().findVarHandle(TaskEmitter.class, "plainRequested", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused") protected long plainRequested;
    protected final Vars vars;
    protected final BatchType<B> batchType;
    protected Throwable error = UNSET_ERROR;
    protected final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();


    protected TaskEmitter(BatchType<B> batchType, Vars vars,
                          EmitterService runner, int worker,
                          int initState, Flags flags) {
        super(runner, worker, initState, flags);
        this.vars = vars;
        this.batchType = batchType;
    }

    @Override protected void doRelease() {
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
        super.doRelease();
    }

    @Override public String toString() {
        var sb = new StringBuilder(48);
        String name = getClass().getSimpleName();
        int nameEnd = name.lastIndexOf("Emitter");
        nameEnd = nameEnd == -1 ? name.length() : nameEnd+2;
        return sb.append(name, 0, nameEnd)
                .append('@')
                .append(Integer.toHexString(System.identityHashCode(this)))
                .toString();
    }

    @Override public String nodeLabel() {
        long req = (long)REQUESTED.getOpaque(this);
        return this+"{st="+flags.render(state())+", requested=" +
                (req > 999_999 ? Long.toHexString(req) : Long.toString(req))+'}';
    }

    @Override public Vars         vars()      { return vars; }
    @Override public BatchType<B> batchType() { return batchType; }

    @Override public void cancel() {
        if (moveStateRelease(statePlain(), CANCEL_REQUESTING))
            awake();
    }

    @Override public void request(long rows) throws NoReceiverException {
        if (THREAD_JOURNAL) journal("request", rows, this);
        if (rows <= 0)
            return;
        // on first request(), transition from CREATED to LIVE
        if ((statePlain() & IS_INIT) != 0) {
            if (THREAD_JOURNAL)  journal("onFirstRequest on", this);
            onFirstRequest();
        }

        // add rows to REQUESTED protecting against overflow
        long now = Async.safeAddAndGetRelease(REQUESTED, this, rows);
        if (now > 0 && now-rows <= 0) {
            if (THREAD_JOURNAL) journal("resume on", this);
            resume();
        }
    }

    @Override public void rebindAcquire() { delayRelease(); }
    @Override public void rebindRelease() { allowRelease(); }

    protected void onFirstRequest() {
        moveStateRelease(statePlain(), ACTIVE);
    }

    protected void resume() { awake(); }

    protected B deliver(Receiver<B> receiver, B b) {
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onBatchDelivered(b);
            return receiver.onBatch(b);
        } catch (Throwable t) {
            Emitters.handleEmitError(receiver, this,
                    (statePlain()&IS_TERM) != 0, t);
            return null;
        }
    }

    @Override protected int resetForRebind(int clearFlags, int setFlags) throws RebindException {
        int state = super.resetForRebind(clearFlags, setFlags);
        REQUESTED.setOpaque(this, 0L);
        error = UNSET_ERROR;
        return state;
    }

    protected void deliverTermination(Receiver<B> receiver, int termState) {
        try {
            switch ((termState & STATE_MASK)) {
                case COMPLETED -> receiver.onComplete();
                case CANCELLED -> receiver.onCancelled();
                case FAILED    -> receiver.onError(error);
            }
        } catch (Throwable t) {
            Emitters.handleTerminationError(receiver, this, t);
        }
    }

}
