package com.github.alexishuf.fastersparql.emit;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.Optional;
import java.util.stream.Stream;

public abstract class AbstractStage<I extends Batch<I>, O extends Batch<O>>
        implements Stage<I, O> {
    protected @MonotonicNonNull Emitter<I> upstream;
    protected @MonotonicNonNull Receiver<O> downstream;
    public final BatchType<O> batchType;
    public final Vars vars;
    protected final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public AbstractStage(BatchType<O> batchType, Vars vars) {
        this.batchType = batchType;
        this.vars = vars;
    }

    @Override public String toString() { return label(StreamNodeDOT.Label.MINIMAL)+"<-"+upstream; }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showStats() && stats != null)
            return stats.appendToLabel(sb).toString();
        return sb.toString();
    }

    /* --- --- --- Emitter methods --- --- --- */

    @Override public Vars         vars()      { return vars;       }
    @Override public BatchType<O> batchType() { return batchType;  }

    @Override public Stream<? extends StreamNode> upstream() {
        return Optional.ofNullable(upstream).stream();
    }

    @Override public void subscribe(Receiver<O> receiver) {
        if (downstream != null && downstream != receiver)
            throw new MultipleRegistrationUnsupportedException(this);
        downstream = receiver;
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("subscribed", receiver, "to", this);
    }

    @Override public void cancel() {
        if (upstream == null) throw new NoEmitterException();
        upstream.cancel();
    }
    @Override public void request(long rows) {
        if (upstream == null) throw new NoEmitterException();
        upstream.request(rows);
    }

    @Override public void rebindAcquire() { upstream.rebindAcquire(); }
    @Override public void rebindRelease() { upstream.rebindRelease(); }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (ThreadJournal.ENABLED)
            ThreadJournal.journal("rebind", this);
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (upstream == null)
            throw new NoEmitterException();
        upstream.rebind(binding);
    }

    /* --- --- --- Receiver methods --- --- --- */

    @Override public @This AbstractStage<I, O> subscribeTo(Emitter<I> emitter) {
        if (emitter != upstream) {
            if (upstream != null) throw new MultipleRegistrationUnsupportedException(this);
            (upstream = emitter).subscribe(this);
        }
        return this;
    }
    @Override public void onComplete() {
        if (downstream == null) throw new NoReceiverException();
        downstream.onComplete();
    }
    @Override public void onCancelled() {
        if (downstream == null) throw new NoReceiverException();
        downstream.onCancelled();
    }
    @Override public void onError(Throwable cause) {
        if (downstream == null) throw new NoReceiverException();
        downstream.onError(cause);
    }
}
