package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.Async.maxAcquire;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>> extends Stateful implements Receiver<B> {
    private static final VarHandle REQ, OLDEST;
    static {
        try {
            REQ    = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainReq", long.class);
            OLDEST = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainOldest", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final int CLOGGED       = 0x80000000;
    private static final Flags SCATTER_FLAGS = Flags.DEFAULT.toBuilder()
            .flag(CLOGGED, "CLOGGED")
            .build();
    private static final int CLOG_SHIFT = 1;

    private final Emitter<B> upstream;
    private int connectorsCount;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[12];
    private final BatchType<B> batchType;
    private long delivered;
    @SuppressWarnings("unused") private long plainOldest;
    @SuppressWarnings("unused") private long plainReq;
    private final int maxDelta;
    private long[] clocks = new long[12];
    private int lastRebindSeq = -1;
    private final Vars vars;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public ScatterStage(Emitter<B> upstream) {
        super(CREATED, SCATTER_FLAGS);
        this.batchType = upstream.batchType();
        this.vars      = upstream.vars();
        this.upstream  = upstream;
        int b = FSProperties.emitReqChunkBatches();
        int safeCols = Math.max(1, vars.size());
        this.maxDelta = Math.max(b, b*batchType.preferredTermsPerBatch()/safeCols)<<CLOG_SHIFT;
        upstream.subscribe(this);
    }

    public Connector<B> createConnector() {
        int st = lock(statePlain());
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (connectorsCount == connectors.length) {
                connectors = Arrays.copyOf(connectors, connectors.length<<1);
                clocks     = Arrays.copyOf(clocks,         clocks.length<<1);
            }
            var conn = new Connector<>(this, connectorsCount++);
            return connectors[conn.index] = conn;
        } finally {
            unlock(st);
        }
    }

    /* --- --- --- helpers --- --- --- */
    private int onFirstRequest(int current) {
        if (moveStateRelease(current, ACTIVE))
            current = (current&FLAGS_MASK) | ACTIVE;
        else
            current = statePlain();
        return current;
    }
    private int updateDelta() {
        long min = delivered, max = min, t;
        for (int i = 0, n = connectorsCount; i < n; i++) {
            t = clocks[i];
            if (t < min) min = t;
            if (t > max) max = t;
        }
        int state = statePlain();
        boolean clogged = max-min > maxDelta;
        if (clogged && (state&CLOGGED) == 0)
            state = setFlagsRelease(state, CLOGGED);
        else if (!clogged && (state&CLOGGED) != 0)
            state = clearFlagsRelease(state, CLOGGED);
        OLDEST.setRelease(this, min);
        return state;
    }

    private void doCancel() {
        moveStateRelease(statePlain(), CANCEL_REQUESTED);
        upstream.cancel();
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
        int last = connectorsCount-1, rows = batch.totalRows();
        delivered += rows; // Connector.request() requires this write before REQ.release
        REQ.getAndAddRelease(this, (long)-rows);
        B copy = null;
        for (int i = 0; i < last; i++)
            copy = connectors[i].downstream.onBatch(copy == null ? batch.dup() : copy);
        batchType.recycle(copy);
        return connectors[last].downstream.onBatch(batch);
    }

    private int beginTerminationDelivery(int termState) {
        if (connectorsCount == 0)
            throw new Emitter.NoReceiverException();
        int st = state();
        if (!moveStateRelease(st, termState))
            throw new IllegalStateException("received duplicate termination");
        st = (st&FLAGS_MASK) | termState;
        return st;
    }

    @Override public void onComplete() {
        int st = beginTerminationDelivery(COMPLETED);
        for (int i = 0, n = connectorsCount; i < n; i++)
            connectors[i].downstream.onComplete();
        markDelivered(st);
    }

    @Override public void onCancelled() {
        int st = beginTerminationDelivery(CANCELLED);
        for (int i = 0, n = connectorsCount; i < n; i++)
            connectors[i].downstream.onCancelled();
        markDelivered(st);
    }

    @Override public void onError(Throwable cause) {
        int st = beginTerminationDelivery(FAILED);
        for (int i = 0, n = connectorsCount; i < n; i++)
            connectors[i].downstream.onError(cause);
        markDelivered(st);
    }

    /* --- --- --- Connector --- --- --- */
    public static final class Connector<B extends Batch<B>> implements Stage<B, B> {
        private final ScatterStage<B> p;
        private Receiver<B> downstream;
        private final int index;

        public Connector(ScatterStage<B> parent, int index) {
            this.p = parent;
            this.index = index;
        }

        /* --- --- --- Stage --- --- --- */

        @Override public @This Stage<B, B> subscribeTo(Emitter<B> upstream) {
            if (upstream != p.upstream) throw new MultipleRegistrationUnsupportedException(this);
            return this;
        }

        @Override public Emitter<B> upstream() { return p.upstream; }

        /* --- --- --- StreamNode --- --- --- */

        @Override public String toString() {
            return label(StreamNodeDOT.Label.MINIMAL)+"<-"+ p.upstream;
        }

        @Override public String label(StreamNodeDOT.Label type) {
            var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), p);
            sb.append('[').append(index).append(']');
            if (type.showState()) {
                sb.append(" state=").append(p.flags.render(p.state()));
                StreamNodeDOT.appendRequested(sb.append(" requested="), p.plainReq);
                sb.append(" now=").append(p.delivered).append(" oldest=").append(p.plainOldest);
                sb.append(" clock=").append(p.clocks[index]);
            }
            if (type.showStats() && p.stats != null)
                p.stats.appendToLabel(sb);
            return sb.toString();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {return p.upstreamNodes();}

        /* --- --- --- Rebindable --- --- --- */

        @Override public void rebindAcquire() {
            p.delayRelease();
            p.upstream.rebindAcquire();
        }
        @Override public void rebindRelease() {
            p.allowRelease();
            p.upstream.rebindRelease();
        }
        @Override public void rebindPrefetch(BatchBinding b) { p.upstream.rebindPrefetch(b); }
        @Override public void rebindPrefetchEnd(boolean s)   { p.upstream.rebindPrefetchEnd(s); }
        @Override public Vars bindableVars()                 { return p.upstream.bindableVars(); }

        @Override public void rebind(BatchBinding binding)   {
            int st = p.resetForRebind(0, LOCKED_MASK);
            try {
                if (binding.sequence == p.lastRebindSeq)
                    return; // duplicate rebind() due to diamond in emitter graph
                p.lastRebindSeq = binding.sequence;
                if (ENABLED)
                    journal("rebind", p);
                if (EmitterStats.ENABLED && p.stats != null)
                    p.stats.onRebind(binding);
                p.delivered = 0;
                Arrays.fill(p.clocks, 0, p.connectorsCount, 0L);
                REQ.setRelease(p, 0L);
                OLDEST.setRelease(p, 0L);
                if (p.upstream == null)
                    throw new NoUpstreamException(p);
                p.upstream.rebind(binding);
            } finally {
                p.unlock(st);
            }
        }


        /* --- --- --- Receiver --- --- --- */

        @Override public @Nullable B onBatch(B batch)  {throw new UnsupportedOperationException();}
        @Override public void onComplete()             {throw new UnsupportedOperationException();}
        @Override public void onCancelled()            {throw new UnsupportedOperationException();}
        @Override public void onError(Throwable cause) {throw new UnsupportedOperationException();}

        /* --- --- --- Emitter --- --- --- */

        @Override public Vars              vars()          { return p.vars; }
        @Override public BatchType<B> batchType()          { return p.batchType; }
        @Override public void            cancel()          { p.doCancel(); }

        @Override
        public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
            if ((p.state()&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (downstream != null && downstream != receiver)
                throw new MultipleRegistrationUnsupportedException(this);
            downstream = receiver;
        }

        @Override public void request(long rows) {
            long[]  clocks    = p.clocks;
            int     state     = p.state();
            if ((state&IS_INIT) != 0)
                state = p.onFirstRequest(state);
            if ((state&IS_TERM) != 0)
                return;
            long now, oldest = (long)OLDEST.getAcquire(p);
            boolean wasOldest = clocks[index] <= oldest;
            boolean added     = maxAcquire(REQ, p, rows);
            clocks[index]     = now = p.delivered; // onBatch() wrote this before REQ.release
            if (wasOldest || now-oldest > p.maxDelta)
                state = p.updateDelta();

            // do not request upstream if the oldest request() was more than 2 chunks ago
            if ((wasOldest || added) && (state&CLOGGED) == 0) {
                if ((rows = Math.max(p.plainReq, rows)) > 0)
                    p.upstream.request(rows);
            }

        }
    }

}
