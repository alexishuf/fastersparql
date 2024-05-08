package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Optional;
import java.util.stream.Stream;

public abstract class AbstractStage<I extends Batch<I>, O extends Batch<O>,
                                    S extends AbstractStage<I, O, S>>
        extends Stateful<S>
        implements Stage<I, O, S> {
    protected @MonotonicNonNull Emitter<I, ?> upstream;
    protected @MonotonicNonNull Receiver<O> downstream;
    public final BatchType<O> batchType;
    public final Vars vars;
    protected final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public AbstractStage(BatchType<O> batchType, Vars vars) {
        super(CREATED, Flags.DEFAULT);
        this.batchType = batchType;
        this.vars = vars;
    }

    @Override protected void doRelease() {
        if (upstream != null) upstream.recycle(this);
        super.doRelease();
    }

    @Override protected void onPendingRelease() {
        cancel();
    }

    @Override public String toString() { return label(StreamNodeDOT.Label.MINIMAL)+"<-"+upstream; }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showState())
            sb.append('[').append(flags.render(statePlain())).append(']');
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    /* --- --- --- Emitter methods --- --- --- */

    @Override public Vars         vars()      { return vars;       }
    @Override public BatchType<O> batchType() { return batchType;  }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Optional.ofNullable(upstream).stream();
    }

    @Override public void subscribe(Receiver<O> receiver) {
        if (downstream != null && downstream != receiver)
            throw new MultipleRegistrationUnsupportedException(this);
        downstream = receiver;
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("subscribed", receiver, "to", this);
    }

    @Override public boolean   isComplete() { return isCompleted(statePlain()); }
    @Override public boolean  isCancelled() { return isCancelled(statePlain()); }
    @Override public boolean     isFailed() { return isFailed(statePlain()); }
    @Override public boolean isTerminated() { return (statePlain()&IS_TERM) != 0; }

    @Override public boolean cancel() {
        return upstream != null && upstream.cancel();
    }
    @Override public void request(long rows) {
        if (upstream == null) throw new NoEmitterException();
        upstream.request(rows);
    }

    @Override public void rebindPrefetch(BatchBinding binding) {
        if (upstream != null) upstream.rebindPrefetch(binding);
    }

    @Override public void rebindPrefetchEnd() {
        if (upstream != null) upstream.rebindPrefetchEnd();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        ThreadJournal.journal("rebind", this);
        resetForRebind(0, 0);
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (upstream == null)
            throw new NoEmitterException();
        upstream.rebind(binding);
    }

    @Override public Vars bindableVars() { return upstream.bindableVars(); }

    /* --- --- --- Receiver methods --- --- --- */

    @Override public @This S subscribeTo(Orphan<? extends Emitter<I, ?>> emitter) {
        if (emitter != upstream) {
            if (upstream != null) throw new MultipleRegistrationUnsupportedException(this);
            upstream = emitter.takeOwnership(this);
            upstream.subscribe(this);
        }
        //noinspection unchecked
        return (S)this;
    }
    @Override public @MonotonicNonNull Emitter<I, ?> upstream() { return upstream; }
    @Override public void onComplete() {
        if (downstream == null) throw new NoReceiverException();
        boolean ok = moveStateRelease(statePlain(), COMPLETED);
        assert ok : "unexpected onComplete";
        downstream.onComplete();
        markDelivered(COMPLETED);
    }
    @Override public void onCancelled() {
        if (downstream == null) throw new NoReceiverException();
        boolean ok = moveStateRelease(statePlain(), CANCELLED);
        assert ok : "unexpected onCancelled";
        downstream.onCancelled();
        markDelivered(CANCELLED);
    }
    @Override public void onError(Throwable cause) {
        if (downstream == null) throw new NoReceiverException();
        boolean ok = moveStateRelease(statePlain(), FAILED);
        assert ok : "unexpected onError";
        downstream.onError(cause);
        markDelivered(FAILED);
    }
}
