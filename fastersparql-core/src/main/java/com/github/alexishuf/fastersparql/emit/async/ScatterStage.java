package com.github.alexishuf.fastersparql.emit.async;

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
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.concurrent.Async.maxAcquire;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>>
        extends Stateful<ScatterStage<B>>
        implements Receiver<B> {
    private static final VarHandle REQ;
    static {
        try {
            REQ    = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainReq", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final int CLOGGED   = 0x80000000;
    private static final int RECYCLED  = 0x40000000;
    private static final Flags SCATTER_FLAGS = Flags.DEFAULT.toBuilder()
            .flag(CLOGGED, "CLOGGED")
            .flag(RECYCLED, "RECYCLED")
            .build();
    private static final int CLOG_SHIFT = 1;

    private final Emitter<B, ?> upstream;
    private int connectorsCount;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[12];
    private final BatchType<B> batchType;
    private long delivered;
    private long oldest;
    @SuppressWarnings("unused") private long plainReq;
    private final int maxDelta;
    private long[] clocks = new long[12];
    private int lastRebindSeq = -1;
    private final Vars vars;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public ScatterStage(Orphan<? extends Emitter<B, ?>> upstreamOrphan) {
        super(CREATED, SCATTER_FLAGS);
        this.upstream  = upstreamOrphan.takeOwnership(this);
        this.batchType = upstream.batchType();
        this.vars      = upstream.vars();
        this.maxDelta  = upstream.preferredRequestChunk()<<CLOG_SHIFT;
        upstream.subscribe(this);
        takeOwnership0(vars); // connectors collectively own the ScatterStage
    }

    @Override protected void doRelease() {
        super.doRelease();
        upstream.recycle(this);
    }

    private void onConnectorRecycled() {
        int st = lock();
        try {
            if ((st&RECYCLED) != 0)
                return; // already recycled
            for (int i = 0; i < connectorsCount; i++) {
                if (!connectors[i].recycled)
                    return; // alive connector, do not recycle ScatterStage
            }
            recycle(vars);
        } finally { unlock(); }
    }

    public Orphan<Connector<B>> createConnector() {
        requireAlive();
        int st = lock();
        try {
            if ((st&IS_INIT) == 0)
                throw new RegisterAfterStartException(this);
            if (connectorsCount == connectors.length) {
                connectors = Arrays.copyOf(connectors, connectors.length<<1);
                clocks     = Arrays.copyOf(clocks,         clocks.length<<1);
            }
            var orphan = new Connector.Concrete<>(this, connectorsCount++);
            connectors[((Connector<B>)orphan).index] = orphan;
            return orphan;
        } finally {
            unlock();
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
        lock();
        long min = delivered, max = min;
        for (int i = 0, n = connectorsCount; i < n; i++) {
            long t = clocks[i];
            if (t < min) min = t;
            if (t > max) max = t;
        }
        oldest = min;
        return unlock(CLOGGED, max-min > maxDelta ? CLOGGED : 0);
    }

    private boolean doCancel() {
        return moveStateRelease(statePlain(), CANCEL_REQUESTED) && upstream.cancel();
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

    @Override public void onBatch(Orphan<B> orphan) {
        B batch = orphan.takeOwnership(this);
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        int last = connectorsCount - 1, rows = batch.totalRows();
        delivered += rows; // Connector.request() requires this write before REQ.release
        REQ.getAndAddRelease(this, (long)-rows);
        for (int i = 0; i < last; i++)
            connectors[i].downstream.onBatchByCopy(batch);
        connectors[last].downstream.onBatch(batch.releaseOwnership(this));
    }

    @Override public void onBatchByCopy(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        int last = connectorsCount-1, rows = batch.totalRows();
        delivered += rows; // Connector.request() requires this write before REQ.release
        REQ.getAndAddRelease(this, (long)-rows);
        for (int i = 0; i < last; i++)
            connectors[i].downstream.onBatchByCopy(batch);
        connectors[last].downstream.onBatchByCopy(batch);
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
        for (int i = 0, n = connectorsCount; i < n; i++) {
            var d = connectors[i].downstream;
            if (d != null) d.onCancelled();
        }
        markDelivered(st);
    }

    @Override public void onError(Throwable cause) {
        int st = beginTerminationDelivery(FAILED);
        for (int i = 0, n = connectorsCount; i < n; i++) {
            var d = connectors[i].downstream;
            if (d != null) d.onError(cause);
        }
        markDelivered(st);
    }

    /* --- --- --- Connector --- --- --- */
    public static abstract sealed class Connector<B extends Batch<B>>
            extends AbstractOwned<Connector<B>>
            implements Stage<B, B, Connector<B>> {
        private final ScatterStage<B> p;
        private Receiver<B> downstream;
        private final int index;
        private boolean recycled;

        protected Connector(ScatterStage<B> parent, int index) {
            this.p = parent;
            this.index = index;
        }

        private static final  class Concrete<B extends Batch<B>>
                extends Connector<B>
                implements Orphan<Connector<B>> {
            public Concrete(ScatterStage<B> parent, int index) {super(parent, index);}
            @Override public Connector<B> takeOwnership(Object o) {return takeOwnership0(o);}
        }

        @Override public @Nullable Connector<B> recycle(Object currentOwner) {
            internalMarkGarbage(currentOwner);
            recycled = true;
            p.onConnectorRecycled();
            return null;
        }

        /* --- --- --- Stage --- --- --- */

        @Override public @This Connector<B> subscribeTo(Orphan<? extends Emitter<B, ?>> orphan) {
            if (orphan != p.upstream)
                throw new MultipleRegistrationUnsupportedException(this);
            return this;
        }

        @Override public Emitter<B, ?> upstream() { return p.upstream; }

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
                sb.append(" now=").append(p.delivered).append(" oldest=").append(p.oldest);
                sb.append(" clock=").append(p.clocks[index]);
            }
            if (type.showStats() && p.stats != null)
                p.stats.appendToLabel(sb);
            return sb.toString();
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {return p.upstreamNodes();}

        /* --- --- --- Rebindable --- --- --- */

        @Override public void rebindPrefetch(BatchBinding b) { p.upstream.rebindPrefetch(b); }
        @Override public void rebindPrefetchEnd()            { p.upstream.rebindPrefetchEnd(); }
        @Override public Vars bindableVars()                 { return p.upstream.bindableVars(); }

        @Override public void rebind(BatchBinding binding)   {
            p.resetForRebind(0, LOCKED_MASK);
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
                p.oldest = 0L;
                if (p.upstream == null)
                    throw new NoUpstreamException(p);
                p.upstream.rebind(binding);
            } finally {
                p.unlock();
            }
        }


        /* --- --- --- Receiver --- --- --- */

        @Override public void onBatch(Orphan<B> batch) {throw new UnsupportedOperationException();}
        @Override public void onBatchByCopy(B batch)   {throw new UnsupportedOperationException();}
        @Override public void onComplete()             {throw new UnsupportedOperationException();}
        @Override public void onCancelled()            {throw new UnsupportedOperationException();}
        @Override public void onError(Throwable cause) {throw new UnsupportedOperationException();}

        /* --- --- --- Emitter --- --- --- */

        @Override public Vars              vars()          { return p.vars; }
        @Override public BatchType<B> batchType()          { return p.batchType; }
        @Override public boolean         cancel()          { return p.doCancel(); }

        @Override public boolean   isComplete() { return Stateful.isCompleted(p.state()); }
        @Override public boolean  isCancelled() { return Stateful.isCancelled(p.state()); }
        @Override public boolean     isFailed() { return Stateful.   isFailed(p.state()); }
        @Override public boolean isTerminated() { return (p.state()&IS_TERM) != 0; }

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
            long now, oldest = p.oldest;
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
