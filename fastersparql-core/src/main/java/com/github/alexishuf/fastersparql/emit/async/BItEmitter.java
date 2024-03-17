package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
import com.github.alexishuf.fastersparql.batch.base.SPSCBIt;
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
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.concurrent.Unparker;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public final class BItEmitter<B extends Batch<B>> extends Stateful
        implements Emitter<B>, Runnable {
    private static final Logger log = LoggerFactory.getLogger(BItEmitter.class);
    private static final VarHandle REQ;
    static {
        try {
            REQ = MethodHandles.lookup().findVarHandle(BItEmitter.class, "plainRequested", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final BIt<B> it;
    private final Thread drainer;
    private @MonotonicNonNull Receiver<B> downstream;
    @SuppressWarnings("unused") private long plainRequested;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public BItEmitter(BIt<B> it) {
        super(CREATED, Flags.DEFAULT);
        this.it = it;
        (this.drainer = Thread.ofVirtual().unstarted(this)).start();
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, it.vars());
    }

    @Override protected void doRelease() {
        try {
            it.close();
        } finally {super.doRelease();}
    }

    /* --- --- --- StreamNode --- --- --- */

    @Override public String toString() { return label(StreamNodeDOT.Label.MINIMAL); }

    @Override public Stream<? extends StreamNode> upstreamNodes() {return Stream.of(it);}

    @Override public String label(StreamNodeDOT.Label type) {
        String itLabel = it.label(StreamNodeDOT.Label.MINIMAL);
        var sb = new StringBuilder(itLabel.length()+16).append("Em(");
        sb.append(itLabel).append(')');
        if (type.showState())
            sb.append("st=").append(flags.render(state()));
        if (EmitterStats.ENABLED && type.showStats() && stats != null)
            sb = stats.appendToLabel(sb);
        return sb.toString();
    }

    /* --- --- --- --- Emitter --- --- --- */

    @Override public Vars         vars()         {return it.vars();}
    @Override public BatchType<B> batchType()    {return it.batchType();}
    @Override public Vars         bindableVars() {return Vars.EMPTY;}
    @Override public boolean        isComplete() {return Stateful.isCompleted(state());}
    @Override public boolean       isCancelled() {return Stateful.isCancelled(state());}
    @Override public boolean          isFailed() {return Stateful.isFailed(state());}
    @Override public boolean      isTerminated() {return (state()&IS_TERM) != 0;}

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        int st = lock(statePlain());
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (EmitterStats.ENABLED && stats != null)
                ++stats.receivers;
            if (downstream != null && downstream != receiver)
                throw new MultipleRegistrationUnsupportedException(this);
            downstream = receiver;
        } finally { unlock(st); }
        journal("subscribed", receiver, "to", this);
    }

    @Override public boolean cancel() {
        if (moveStateRelease(statePlain(), CANCEL_REQUESTED)) {
            Unparker.unpark(drainer);
            it.tryCancel();
            return true;
        }
        return false;
    }

    @Override public void request(long rows) throws NoReceiverException {
        int st = statePlain();
        if ((st&IS_INIT) != 0 && moveStateRelease(st, ACTIVE))
            st = st&FLAGS_MASK | ACTIVE;
        if ((st&IS_LIVE) != 0 && Async.maxRelease(REQ, this, rows)) {
            journal("request ", rows, ", unpark drainer on", this);
            Unparker.unpark(drainer);
        }
    }

    @Override public void rebindAcquire() {delayRelease();}
    @Override public void rebindRelease() {allowRelease();}
    @Override public void rebind(BatchBinding binding) throws RebindException {
        throw new UnsupportedOperationException("Cannot rebind a BIt");
    }

    /* --- --- --- drainer --- --- --- */

    @Override public void run() {
        if (Thread.currentThread() != drainer)
            throw new IllegalStateException("Called from wrong thread");
        SPSCBIt<B> queue = it instanceof SPSCBIt<B> q ? q : null;
        int st = statePlain();
        Throwable error = null;
        long req = 0;
        B b = null;
        try {
            while ((st&(IS_CANCEL_REQ|IS_TERM)) == 0) {
                if ((req=(long)REQ.getAcquire(this)) <= 0) {
                    LockSupport.park();
                    st = stateAcquire();
                } else {
                    int iReq = (int) Math.min(Integer.MAX_VALUE, req);
                    if (queue != null)
                        queue.maxReadyItems(iReq);
                    it.maxBatch(iReq);
                    if ((b=it.nextBatch(b)) == null)
                        break;
                    REQ.getAndAddRelease(this, (long)-b.totalRows());
                    try {
                        if (ResultJournal.ENABLED)
                            ResultJournal.logBatch(this, b);
                        if (EmitterStats.ENABLED && stats != null)
                            stats.onBatchDelivered(b);
                        b = downstream.onBatch(b);
                    } catch (Throwable t) {
                        handleEmitError(downstream, this,
                                        (statePlain()&IS_TERM) != 0, t);
                    }
                }
            }
        } catch (Throwable t) {
            error = t;
        } finally {
            try {
                it.close();
            } catch (Throwable t) {
                log.error("{} closing {} after end of iteration with error={}",
                          t.getClass().getSimpleName(), it, error, t);
            }
        }
        st = stateAcquire();
        int termState;
        if      (error instanceof BItReadCancelledException) termState = CANCELLED;
        else if (error != null)                              termState = FAILED;
        else if ((st&IS_CANCEL_REQ) != 0)                    termState = CANCELLED;
        else                                                 termState = COMPLETED;
        if (moveStateRelease(st, termState)) {
            try {
                switch (termState) {
                    case COMPLETED -> downstream.onComplete();
                    case FAILED    ->  downstream.onError(error);
                    case CANCELLED -> downstream.onCancelled();
                }
            } catch (Throwable t) {
                Emitters.handleTerminationError(downstream, this, t);
            }
            markDelivered(st, termState);
        } else {
            log.error("{} terminated outside drainer thread", this);
            assert false : "another thread terminated BItEmitter state";
        }
    }
}
