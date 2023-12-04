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
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>> extends Stateful implements Receiver<B> {
    private static final VarHandle PENDING;
    static {
        try {
            PENDING = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainPending", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Emitter<B> upstream;
    private int connectorCount;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[12];
    @SuppressWarnings("unused") private long plainPending;
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

    public Connector<B> createConnector() {
        int st = lock(statePlain());
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (connectorCount == connectors.length)
                connectors = Arrays.copyOf(connectors, connectors.length*2);
            int i = connectorCount++;
            return connectors[i] = new Connector<>(this, i);
        } finally { unlock(st); }
    }

    /* --- --- --- helpers used by Connectors --- --- --- */

    private void doRebind(BatchBinding binding) throws RebindException {
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

    private void doRequest(long pendingAtConnector) {
        long ac = plainPending;
        while (pendingAtConnector > ac) {
            long ex = ac;
            if ((ac=(long)PENDING.compareAndExchangeRelease(this, ex, pendingAtConnector)) != ex)
                continue;
            long add = pendingAtConnector - Math.max(0, ac);
            if (add > 0) {
                if (ENABLED)
                    journal("bump upstream requested by", add, "on", this);
                int st = stateAcquire();
                if ((st&IS_TERM) != 0 || ((st&IS_INIT) != 0 && !moveStateRelease(st, ACTIVE)))
                    break; // concurrently terminated, cannot request
                upstream.request(add);
            }
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
        int last = connectorCount-1, rows = batch.totalRows();
        PENDING.getAndAddRelease(this, -(long)rows);
        for (int i = 0; i <= last; i++)
            connectors[i].onBatch(rows);
        B copy = null;
        for (int i = 0; i < last; i++)
            copy = connectors[i].downstream.onBatch(copy == null ? batch.dup() : copy);
        batchType.recycle(copy);
        return connectors[last].downstream.onBatch(batch);
    }

    private int beginTerminationDelivery(int termState) {
        if (connectorCount == 0)
            throw new Emitter.NoReceiverException();
        int st = state();
        if (!moveStateRelease(st, termState))
            throw new IllegalStateException("received duplicate termination");
        st = (st&FLAGS_MASK) | termState;
        return st;
    }

    @Override public void onComplete() {
        int st = beginTerminationDelivery(COMPLETED);
        for (int i = 0, n = connectorCount; i < n; i++)
            connectors[i].downstream.onComplete();
        markDelivered(st);
    }

    @Override public void onCancelled() {
        int st = beginTerminationDelivery(CANCELLED);
        for (int i = 0, n = connectorCount; i < n; i++)
            connectors[i].downstream.onCancelled();
        markDelivered(st);
    }

    @Override public void onError(Throwable cause) {
        int st = beginTerminationDelivery(FAILED);
        for (int i = 0, n = connectorCount; i < n; i++)
            connectors[i].downstream.onError(cause);
        markDelivered(st);
    }

    /* --- --- --- Connector --- --- --- */

    public static final class Connector<B extends Batch<B>> implements Stage<B, B> {
        private static final VarHandle CONN_REQ;
        static {
            try {
                CONN_REQ = MethodHandles.lookup().findVarHandle(Connector.class, "plainReq", long.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final ScatterStage<B> parent;
        private @MonotonicNonNull Receiver<B> downstream;
        private final int index;
        @SuppressWarnings("unused") private long plainReq;

        public Connector(ScatterStage<B> parent, int index) {
            this.parent = parent;
            this.index  = index;
        }

        private void onBatch(int rows) {
            CONN_REQ.getAndAddRelease(this, (long)-rows);
        }

        /* --- --- --- StreamNode --- --- --- */

        @Override public String toString() {
            return label(StreamNodeDOT.Label.MINIMAL)+"["+index+"<-"+parent.upstream;
        }

        @Override public String label(StreamNodeDOT.Label type) {
            var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), parent);
            sb.append('[').append(index).append(']');
            if (type.showState()) {
                sb.append(" state=").append(parent.flags.render(parent.state()));
                appendRequested(sb.append(" pending="), (long)CONN_REQ.getOpaque(this));
                appendRequested(sb.append(" parent.pending="), (long)PENDING.getOpaque(parent));
            }
            if (type.showStats() && parent.stats != null)
                parent.stats.appendToLabel(sb);
            return sb.toString();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.of(parent.upstream);
        }

        /* --- --- --- Stage --- --- --- */

        @Override public @This Stage<B, B> subscribeTo(Emitter<B> upstream) {
            if (upstream != parent.upstream)
                throw new MultipleRegistrationUnsupportedException(this);
            return this;
        }

        @Override public @MonotonicNonNull Emitter<B> upstream() { return parent.upstream; }

        /* --- --- --- Rebindable --- --- --- */

        @Override public void rebindAcquire() {
            parent.delayRelease();
            parent.upstream.rebindAcquire();
        }

        @Override public void rebindRelease() {
            parent.allowRelease();
            parent.upstream.rebindRelease();
        }

        @Override public void rebind(BatchBinding binding) { parent.doRebind(binding); }

        @Override public Vars bindableVars() { return parent.upstream.bindableVars(); }

        /* --- --- --- Emitter --- --- --- */

        @Override public Vars              vars() { return parent.vars; }
        @Override public BatchType<B> batchType() { return parent.batchType; }

        @Override
        public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
            if (receiver == downstream)
                return; // no-op
            if (downstream != null)
                throw new MultipleRegistrationUnsupportedException(this);
            if ((parent.state()&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            downstream = receiver;
            if (ENABLED)
                journal("subscribed", receiver, "to", this);
        }

        @Override public void cancel() {
            parent.moveStateRelease(parent.statePlain(), CANCEL_REQUESTED);
            parent.upstream.cancel();
        }

        @Override public void request(long rows) throws NoReceiverException {
            if (rows <= 0)
                return;
            parent.doRequest(Async.safeAddAndGetRelease(CONN_REQ, this, plainReq, rows));
        }

        @Override public @Nullable B onBatch(B batch)  {throw new UnsupportedOperationException();}
        @Override public void     onComplete()         {throw new UnsupportedOperationException();}
        @Override public void    onCancelled()         {throw new UnsupportedOperationException();}
        @Override public void onError(Throwable cause) {throw new UnsupportedOperationException();}
    }
}
