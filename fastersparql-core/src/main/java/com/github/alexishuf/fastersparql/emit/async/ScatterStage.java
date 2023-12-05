package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>> extends Stateful implements Stage<B, B> {
    private static final VarHandle REQ;
    static {
        try {
            REQ = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainReq", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Emitter<B> upstream;
    private int receiversCount;
    @SuppressWarnings("unchecked") private Receiver<B>[] receivers = new Receiver[12];
    @SuppressWarnings("unused") private long plainReq;
    private final BatchType<B> batchType;
    private int lastRebindSeq = -1;
    private final Vars vars;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public ScatterStage(Emitter<B> upstream) {
        super(CREATED, Flags.DEFAULT);
        this.batchType = upstream.batchType();
        this.vars      = upstream.vars();
        this.upstream  = upstream;
        upstream.subscribe(this);
    }

    /* --- --- --- Stage --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> upstream) {
        if (upstream != this.upstream)
            throw new MultipleRegistrationUnsupportedException(this);
        return this;
    }

    @Override public @MonotonicNonNull Emitter<B> upstream() {
        return upstream;
    }

    /* --- --- --- Emitter --- --- --- */

    @Override public Vars vars() { return vars;}
    @Override public BatchType<B> batchType() { return batchType; }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        int st = lock(statePlain());
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            for (int i = 0; i < receiversCount; i++) {
                if (receivers[i] == receiver) return;
            }
            if (receiversCount == receivers.length)
                receivers = Arrays.copyOf(receivers, receivers.length<<1);
            receivers[receiversCount++] = receiver;
            if (ENABLED) journal("subscribed", receiver, "to", this);
        } finally {
            unlock(st);
        }
    }

    @Override public void cancel() {
        moveStateRelease(statePlain(), CANCEL_REQUESTED);
        upstream.cancel();
    }

    @Override public void request(long rows) throws NoReceiverException {
        if (rows <= 0) return;
        upstream.request(rows);
    }

    /* --- --- --- Rebindable --- --- --- */

    @Override public void rebindAcquire() {
        delayRelease();
        upstream.rebindAcquire();
    }

    @Override public void rebindRelease() {
        allowRelease();
        upstream.rebindRelease();
    }

    @Override public void rebindPrefetch(BatchBinding b) {upstream.rebindPrefetch(b);}
    @Override public void rebindPrefetchEnd() {upstream.rebindPrefetchEnd();}
    @Override public Vars bindableVars() {return upstream.bindableVars();}

    @Override public void rebind(BatchBinding binding) throws RebindException {
        int st = resetForRebind(0, LOCKED_MASK);
        try {
            if (binding.sequence == lastRebindSeq)
                return; // duplicate rebind() due to diamond in emitter graph
            lastRebindSeq = binding.sequence;
            if (ENABLED)
                journal("rebind", this);
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            if (upstream == null)
                throw new NoUpstreamException(this);
            upstream.rebind(binding);
        } finally {
            unlock(st);
        }

    }

    /* --- --- --- StreamNode --- --- --- */

    @Override public String toString() {
        return label(StreamNodeDOT.Label.MINIMAL)+"<-"+upstream;
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showState())
            sb.append("state=").append(flags.render(state()));
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.of(upstream); }

    /* --- --- --- Receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        int last = receiversCount-1, rows = batch.totalRows();
        REQ.getAndAddRelease(this, -(long)rows);
        B copy = null;
        for (int i = 0; i < last; i++)
            copy = receivers[i].onBatch(copy == null ? batch.dup() : copy);
        batchType.recycle(copy);
        return receivers[last].onBatch(batch);
    }

    private int beginTerminationDelivery(int termState) {
        if (receiversCount == 0)
            throw new Emitter.NoReceiverException();
        int st = state();
        if (!moveStateRelease(st, termState))
            throw new IllegalStateException("received duplicate termination");
        st = (st&FLAGS_MASK) | termState;
        return st;
    }

    @Override public void onComplete() {
        int st = beginTerminationDelivery(COMPLETED);
        for (int i = 0, n = receiversCount; i < n; i++)
            receivers[i].onComplete();
        markDelivered(st);
    }

    @Override public void onCancelled() {
        int st = beginTerminationDelivery(CANCELLED);
        for (int i = 0, n = receiversCount; i < n; i++)
            receivers[i].onCancelled();
        markDelivered(st);
    }

    @Override public void onError(Throwable cause) {
        int st = beginTerminationDelivery(FAILED);
        for (int i = 0, n = receiversCount; i < n; i++)
            receivers[i].onError(cause);
        markDelivered(st);
    }
}
