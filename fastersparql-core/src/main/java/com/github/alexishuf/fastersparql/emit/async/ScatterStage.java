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

import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>> extends Stateful implements Stage<B, B> {
    private @MonotonicNonNull Emitter<B> upstream;
    private int downstreamCount;
    @SuppressWarnings("unchecked") private Receiver<B>[] downstream = new Receiver[12];
    private final BatchType<B> batchType;
    private int lastRebindSeq = -1;
    private final Vars vars;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public ScatterStage(BatchType<B> batchType, Vars vars) {
        super(CREATED, Flags.DEFAULT);
        this.batchType = batchType;
        this.vars = vars;
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

    /* --- --- --- Stage --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> emitter) {
        if (emitter != upstream) {
            if (upstream != null) throw new MultipleRegistrationUnsupportedException(this);
            (upstream = emitter).subscribe(this);
        }
        return this;
    }

    @Override public @MonotonicNonNull Emitter<B> upstream() { return upstream; }

    /* --- --- --- rebindable --- --- --- */

    @Override public void rebindAcquire() {
        delayRelease();
        upstream.rebindAcquire();
    }

    @Override public void rebindRelease() {
        allowRelease();
        upstream.rebindRelease();
    }

    @Override public Vars bindableVars() { return upstream.bindableVars(); }

    /* --- --- --- Emitter --- --- --- */

    @Override public Vars              vars() { return vars; }
    @Override public BatchType<B> batchType() { return batchType; }

    @Override public void subscribe(Receiver<B> receiver) {
        int st = lock(statePlain());
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            for (int i = 0, n = downstreamCount; i < n; i++) {
                if (downstream[i] == receiver) return;
            }
            if (downstreamCount == downstream.length)
                downstream = Arrays.copyOf(downstream, downstream.length*2);
            downstream[downstreamCount++] = receiver;
            if (ENABLED)
                journal("subscribed", receiver, "to", this);
        } finally { unlock(st); }
    }

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

    @Override public void cancel() {
        moveStateRelease(statePlain(), CANCEL_REQUESTED);
        upstream.cancel();
    }

   @Override public void request(long rows) {
       if (ENABLED)
           journal("request", rows, "on", this);
        int st = lock(statePlain()), nextState = st;
        try {
            if ((st&IS_INIT) != 0)
                nextState = ACTIVE;
        } finally {
            unlock(st, STATE_MASK, nextState&STATE_MASK);
        }
       upstream.request(rows);
    }

    /* --- --- --- Receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        int n = downstreamCount;
        B copy = null;
        for (int i = 1; i < n; i++)
            copy = downstream[i].onBatch(copy == null ? batch.dup() : copy);
        batchType.recycle(copy);
        return downstream[0].onBatch(batch);
    }

    private int beginTerminationDelivery(int termState) {
        if (downstreamCount == 0)
            throw new NoReceiverException();
        int st = state();
        if (!moveStateRelease(st, termState))
            throw new IllegalStateException("received duplicate termination");
        st = (st&FLAGS_MASK) | termState;
        return st;
    }

    @Override public void onComplete() {
        int st = beginTerminationDelivery(COMPLETED);
        for (int i = 0, n = downstreamCount; i < n; i++)
            downstream[i].onComplete();
        markDelivered(st);
    }

    @Override public void onCancelled() {
        int st = beginTerminationDelivery(CANCELLED);
        for (int i = 0, n = downstreamCount; i < n; i++)
            downstream[i].onCancelled();
        markDelivered(st);
    }

    @Override public void onError(Throwable cause) {
        int st = beginTerminationDelivery(FAILED);
        for (int i = 0, n = downstreamCount; i < n; i++)
            downstream[i].onError(cause);
        markDelivered(st);
    }
}
