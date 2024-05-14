package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindStateException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.AbstractOwned;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.SpecialOwner;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleTerminationError;
import static com.github.alexishuf.fastersparql.emit.async.Stateful.*;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.Async.maxRelease;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;

public abstract sealed class GatheringEmitter<B extends Batch<B>>
        extends AbstractOwned<GatheringEmitter<B>>
        implements Emitter<B, GatheringEmitter<B>> {
    private static final Logger log = LoggerFactory.getLogger(GatheringEmitter.class);
    private static final Thread NO_OWNER = null;
    private static final VarHandle OWNER, FILLING, REQ, STATS_RCV_LOCK;
    static {
        assert ((STATE_MASK|GRP_MASK) & ~0xff) == 0
                : "Stateful states do not fit in a byte";
        try {
            OWNER          = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainOwner", Thread.class);
            FILLING        = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainFilling", Batch.class);
            REQ            = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainReq", long.class);
            STATS_RCV_LOCK = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainStatsRcvLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") private Thread plainOwner;
    private int lockedLevel;
    @SuppressWarnings("unused") private @Nullable B plainFilling;
    private Receiver<B> downstream;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[10];
    private short connectorCount;
    private short requestChunk;
    @SuppressWarnings("unused") private long plainReq;
    private byte state = CREATED;
    private boolean downstreamRecycle;
    private short connectorTerminatedCount;
    private final BatchType<B> batchType;
    private int lastRebindSeq = -1;
    private final Vars vars;
    private Vars bindableVars = Vars.EMPTY;
    @SuppressWarnings("unused") private int plainStatsRcvLock;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public static <B extends Batch<B>> Orphan<GatheringEmitter<B>>
    create(BatchType<B> batchType, Vars vars) {return new Concrete<>(batchType, vars);}

    protected GatheringEmitter(BatchType<B> batchType, Vars vars) {
        this.vars         = vars;
        this.batchType    = batchType;
        this.requestChunk = (short)Math.min(Short.MAX_VALUE, preferredRequestChunk());
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }

    @Override public @Nullable GatheringEmitter<B> recycle(Object currentOwner) {
        internalMarkGarbage(currentOwner);
        if (currentOwner == SpecialOwner.GARBAGE)
            return null;
        lock();
        try {
            downstreamRecycle = true;
            if ((state&IS_TERM_DELIVERED) != 0)
                doRelease();
            // else: future onConnectorTerminated() -> markDelivered() -> doRelease()
        } finally {unlock();}
        return null;
    }

    private void doRelease() {
        journal("releasing", this);
        for (int i = 0; i < connectorCount; i++)
            connectors[i].release();
        if (EmitterStats.LOG_ENABLED && stats != null)
            stats.report(log, this);

    }

    private static final class Concrete<B extends Batch<B>> extends GatheringEmitter<B>
            implements Orphan<GatheringEmitter<B>> {
        public Concrete(BatchType<B> batchType, Vars vars) {super(batchType, vars);}
        @Override public GatheringEmitter<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }


    public void subscribeTo(Orphan<? extends Emitter<B, ?>> orphan) {
        beginDelivery();
        if ((state&STATE_MASK) != CREATED)
            throw new RegisterAfterStartException(this);
        try {
            var connector = new Connector<>(orphan, this);
            assert isNovelUpstream(connector.up) : "Already subscribed to upstream";
            if (connectorCount >= connectors.length)
                connectors = Arrays.copyOf(connectors, connectorCount*2);
            int upChunk = connector.up.preferredRequestChunk();
            if (upChunk > requestChunk)
                requestChunk = (short)Math.min(Short.MAX_VALUE, upChunk);
            connectors[connectorCount++] = connector;
            bindableVars = bindableVars.union(connector.up.bindableVars());
        } finally {
            endDelivery();
        }
    }

    private boolean isNovelUpstream(Emitter<B, ?> upstream) {
        for (int i = 0, n = connectorCount; i < n; i++) {
            if (connectors[i].up == upstream) return false;
        }
        return true;
    }

    /*  --- --- --- StreamNode implementation  --- --- --- */

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Arrays.stream(connectors, 0, connectorCount).map(c -> c.up);
    }

    @Override public String toString() {
        return StreamNodeDOT.minimalLabel(new StringBuilder(), this).toString();
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showState()) {
            int flaggedState = 0xff&state;
            if ((Thread)OWNER.getAcquire(this) != null) flaggedState |= LOCKED_MASK;
            if ((state&IS_TERM_DELIVERED) != 0 && downstreamRecycle)
                flaggedState |= RELEASED_MASK;
            sb.append("\nstate=").append(Flags.DEFAULT.render(flaggedState));
            StreamNodeDOT.appendRequested(sb.append(", requested="), plainReq);
        }
        if (EmitterStats.ENABLED && type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    /*  --- --- --- Emitter implementation  --- --- --- */

    @Override public Vars               vars() { return vars; }
    @Override public BatchType<B>  batchType() { return batchType; }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException {
        requireAlive();
        beginDelivery();
        try {
            if (downstream == receiver)
                return;
            if (state != CREATED)
                throw new RegisterAfterStartException(this);
            else if (downstream != null)
                throw new MultipleRegistrationUnsupportedException(this);
            downstream = receiver;
        } finally {
            endDelivery();
        }
    }

    @Override public boolean   isComplete() { return Stateful.isCompleted(state); }
    @Override public boolean  isCancelled() { return Stateful.isCancelled(state); }
    @Override public boolean     isFailed() { return Stateful.   isFailed(state); }
    @Override public boolean isTerminated() { return (state&IS_TERM) != 0; }

    @Override public boolean cancel() {
        boolean cancelled = false;
        if ((state&(IS_INIT|IS_LIVE)) != 0) {
            for (int i = 0, count = connectorCount; i < count; i++)
                cancelled |= connectors[i].up.cancel();
        }
        return cancelled;
    }

    @Override public void rebindPrefetch(BatchBinding binding) {
        for (int i = 0; i < connectorCount; i++)
            connectors[i].rebindPrefetch(binding);
    }

    @Override public void rebindPrefetchEnd() {
        for (int i = 0; i < connectorCount; i++)
            connectors[i].rebindPrefetchEnd();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (binding.sequence == lastRebindSeq)
            return; // duplicate rebind() due to diamond in emitter graph
        lastRebindSeq = binding.sequence;
        lock();
        try {
            if ((state&(IS_INIT|IS_TERM)) == 0)
                throw new RebindStateException(this);
            requireAlive();
            state = CREATED;
            connectorTerminatedCount = 0;
            REQ.setRelease(this, 0);
            for (int i = 0, count = connectorCount; i < count; i++)
                connectors[i].rebind(binding);
        } finally {
            unlock();
        }
    }

    @Override public Vars bindableVars() { return bindableVars; }

    @Override public void request(long rows) throws NoReceiverException {
        if (rows == 0 || (state&IS_TERM) != 0)
            return;
        if (state == CREATED)
            state = ACTIVE;
        boolean added = Async.maxRelease(REQ, this, rows);
        if (ENABLED)
            journal(added?"request, rows=":"nop-request, rows=", rows, "on", this);
        if (added) {
            for (int i = 0, count = connectorCount; i < count; i++)
                connectors[i].requestNextChunk(0);
        }
    }

    /*  --- --- --- Receiver implementation  --- --- --- */

    private void onConnectorTerminated() {
        beginDelivery();
        try {
            if (++connectorTerminatedCount < connectorCount)
                return; // not all connectors are terminated
            int termState = COMPLETED;
            Throwable firstError = UNSET_ERROR;
            for (int i = 0, n = connectorCount; i < n; i++) {
                var c = connectors[i];
                if (c.error != null) {
                    termState = FAILED;
                    if (firstError == UNSET_ERROR) firstError = c.error;
                } else if (c.cancelled)  {
                    if (termState != FAILED) termState = CANCELLED;
                } else if (!c.completed) {
                    return; // should never happen
                }
            }
            if (Stateful.isSuccessor(state, termState)) {
                state = (byte)termState;
                try {
                    deliverTermination(downstream, termState, firstError);
                } catch (Throwable t) {
                    Emitters.handleTerminationError(downstream, this, t);
                } finally {
                    if ((state&IS_TERM) != 0) // no rebind() from downstream
                        markDelivered();
                }
            }
        } finally {
            endDelivery();
        }
    }

    private void markDelivered() {
        state |= (byte)IS_TERM_DELIVERED;
        if (downstreamRecycle)
            doRelease();
    }

    private void onBatchReceived(B b) {
        if (stats == null) return;
        while ((int)STATS_RCV_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            stats.onBatchReceived(b);
        } finally { STATS_RCV_LOCK.setRelease(this, 0); }
    }

    private void onBatchReceived(Orphan<B> b) {
        if (stats == null) return;
        while ((int)STATS_RCV_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
            Thread.onSpinWait();
        try {
            stats.onBatchReceived(b);
        } finally { STATS_RCV_LOCK.setRelease(this, 0); }
    }

    /**
     * Acquires the {@code LOCK} mutex, waiting for its release if it is locked.
     */
    private void beginDelivery() {
        lock();
        deliverFilling();
    }

    private void lock() {
        var me = currentThread();
        var curr = (Thread)OWNER.compareAndExchangeAcquire(this, NO_OWNER, me);
        if (curr != NO_OWNER && curr != me)
            lockCold(me);
        ++lockedLevel;
    }

    private void unlock() {
        assert lockedLevel > 0 && plainOwner == currentThread()
                : "unlock() outside the owner thread";
        if (--lockedLevel == 0) OWNER.setRelease(this, NO_OWNER);
    }

    private void lockCold(Thread me) {
        for (int i = 0; OWNER.compareAndExchangeAcquire(this, NO_OWNER, me) != NO_OWNER; ++i) {
            if ((i&7) == 7) EmitterService.yieldWorker(me);
            else            Thread.onSpinWait();
        }
    }

    /**
     * Atomically acquires the {@code LOCK} mutex if it is not locked.
     *
     * @return {@code true} iff this thread is the only one delivering to downstream
     */
    private boolean tryBeginDelivery() {
        var me = currentThread();
        var current = (Thread)OWNER.compareAndExchangeAcquire(this, NO_OWNER, me);
        if (current != NO_OWNER && current != me)
            return false; // another thread is delivering
        ++lockedLevel;
        // got the mutex, we are obligated to deliver FILLING.
        deliverFilling();
        return true; // this thread is the only one delivering
    }

    /** If {@code FILLING != null}, delivers it. Assumes calling thread owns {@code LOCK}. */
    private void deliverFilling() {
        try {
            //noinspection unchecked
            B b = (B) FILLING.getAndSetAcquire(this, null);
            if (b != null)
                deliver(b);
        } catch (Throwable t) {
            unlock();
            throw t;
        }
    }

    /**
     * Safely release the {@code LOCK}, ensuring that one of the following holds:
     *
     * <ul>
     *     <li>the mutex is released and there is no {@code FILLING} batch remaining</li>
     *     <li>there is a {@code FILLING} batch, but another thread already holds the mutex and
     *         is thus obliged to deliver it.</li>
     * </ul>
     */
    private void endDelivery() {
        do {
            // release LOCK before checking FILLING again, this allows another thread to
            // acquire LOCK and thus become responsible for delivery of the FILLING batch
            unlock();
        } while (FILLING.getAcquire(this) != null && tryBeginDelivery());
        // (LOCK==0 && FILLING==null) || LOCK==1 (but not owned by this thread)
    }

    /**
     * Deliver {@code b} to {@code downstream} via {@link Receiver#onBatch(Orphan)}
     */
    private void deliver(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchDelivered(b);
        try {
            Orphan<B> orphan = b.releaseOwnership(this);
            b = null;
            downstream.onBatch(orphan);
        } catch (Throwable t) {
            handleEmitError(downstream, this, t, b);
        }
    }

    /**
     * Deliver {@code b} to {@code downstream} via {@link Receiver#onBatchByCopy(Batch)}
     */
    private void deliverCopy(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchDelivered(b);
        try {
            downstream.onBatchByCopy(b);
        } catch (Throwable t) {
            handleEmitError(downstream, this, t, null);
        }
    }

    /**
     * Safely call {@link Receiver#onComplete()}/{@link Receiver#onError(Throwable)}/{@link Receiver#onCancelled()} on {@code downstream}.
     *
     * @param downstream the {@link Receiver} to receive the event
     * @param termState {@link Stateful#COMPLETED}, {@link Stateful#FAILED} or
     *                  {@link Stateful#CANCELLED}
     * @param error the cause, if {@code termState == }{@link Stateful#FAILED}
     */
    private void deliverTermination(Receiver<B> downstream, int termState,
                                    @Nullable Throwable error) {
        try {
            switch (termState&STATE_MASK) {
                case FAILED    -> downstream.onError(error);
                case CANCELLED -> downstream.onCancelled();
                case COMPLETED -> downstream.onComplete();
                default        -> throw new IllegalArgumentException();
            }
        } catch (Throwable t) {
            handleTerminationError(downstream, this, t);
        }
    }


    /**
     * Receives events from one upstream and safely queue or deliver them via {@code down.deliver*()}
     */
    private static final class Connector<B extends Batch<B>>
            implements Receiver<B> {
        private static final VarHandle CONN_REQ;
        static {
            try {
                CONN_REQ = MethodHandles.lookup().findVarHandle(Connector.class, "plainConnReq", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        final Emitter<B, ?> up;
        final GatheringEmitter<B> down;
        final BatchType<B> bt;
        int plainConnReq;
        BatchMerger<B, ?> projector;
        private boolean completed, cancelled;
        private @Nullable Throwable error;

        public Connector(Orphan<? extends Emitter<B, ?>> orphan,
                         GatheringEmitter<B> downstream) {
            this.down = downstream;
            this.bt   = downstream.batchType;
            this.up   = orphan.takeOwnership(this);
            this.up.subscribe(this);
            var projector = bt.projector(downstream.vars, up.vars());
            this.projector = projector == null ? null : projector.takeOwnership(this);
        }

        void release() {
            projector = Owned.safeRecycle(projector, this);
            up.recycle(this);
        }

        void requestNextChunk(int received) {
            short chunk   = down.requestChunk;
            long  downReq = (long)REQ     .getAndAddAcquire(down, (long)-received)-received;
            int   ac      = (int )CONN_REQ.getAndAddRelease(this,       -received)-received;
            if (ac <= chunk>>1) {
                int req = (int)Math.min(downReq, chunk);
                if (maxRelease(CONN_REQ, this, req))
                    up.request(req);
            }
        }

        public void rebindPrefetch(BatchBinding binding) { up.rebindPrefetch(binding); }

        public void rebindPrefetchEnd() { up.rebindPrefetchEnd(); }

        public void rebind(BatchBinding binding) {
            CONN_REQ.setRelease(this, 0);
            completed = false;
            cancelled = false;
            error = null;
            up.rebind(binding);
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.of(up); }

        @Override public String toString() {
            int i = 0;
            while (i < down.connectorCount && down.connectors[i] != this) ++i;
            return down+"["+i+"]";
        }

        @Override public String label(StreamNodeDOT.Label type) {
            int i = 0;
            while (i < down.connectorCount && down.connectors[i] != this) ++i;
            var sb = new StringBuilder("Gathering@")
                    .append(Integer.toHexString(identityHashCode(down)))
                    .append("[").append(i).append("]@")
                    .append(Integer.toHexString(identityHashCode(this)));
            if (type.showState()) {
                int st = ACTIVE;
                if      (error != null) st = FAILED;
                else if (cancelled    ) st = CANCELLED;
                else if (completed    ) st = COMPLETED;
                sb.append("\nstate=").append(Flags.DEFAULT.render(st));
                sb.append(", requested=");
                StreamNodeDOT.appendRequested(sb, (int)CONN_REQ.getOpaque(this));
            }
            return sb.toString();
        }

        @Override public void onBatch(Orphan<B> orphan) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.onBatchReceived(orphan);
            requestNextChunk(Batch.peekTotalRows(orphan));
            if (projector != null)
                orphan = projector.projectInPlace(orphan);
            onBatchLoop(orphan);
        }

        @SuppressWarnings("unchecked") private void onBatchLoop(Orphan<B> orphan) {
            B f, b = orphan.takeOwnership(down);
            Thread self = null;
            while (true) {
                if (down.tryBeginDelivery()) {
                    try {
                        down.deliver(b);
                        break;
                    } finally { down.endDelivery(); }
                } else if (FILLING.compareAndExchangeRelease(down, null, b) == null) {
                    // deliver our write to FILLING in case LOCK was released
                    if (down.tryBeginDelivery())  // else: thread owning LOCK will deliver
                        down.endDelivery();
                    break;
                } else if ((f=(B)FILLING.getAndSetAcquire(down, null)) != null) {
                    f.requireOwner(down);
                    f.append(b.releaseOwnership(down));
                    f.requireOwner(down);
                    b = f; // continue, trying to deliver the combined batch
                } else if (self == null) {
                    self = Thread.currentThread();
                } else {
                    EmitterService.yieldWorker(self);
                }
            }
        }

        @SuppressWarnings("unchecked") @Override public void onBatchByCopy(B b) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.onBatchReceived(b);
            requestNextChunk(b.totalRows());
            if (projector != null) {
                onBatchLoop(projector.project(null, b));
            } else if (down.tryBeginDelivery()) {
                try {
                    down.deliverCopy(b);
                } finally { down.endDelivery(); }
            } else {
                B f = (B)FILLING.getAndSetRelease(down, null);
                if (f == null)
                    f = bt.create(b.cols).takeOwnership(down);
                f.copy(b);
                onBatchLoop(f.releaseOwnership(down));
            }
        }

        @Override public void onComplete() {
            completed = true;
            down.onConnectorTerminated();
        }

        @Override public void onCancelled() {
            cancelled = true;
            down.onConnectorTerminated();
        }

        @Override public void onError(Throwable cause) {
            error = cause == null ? UNSET_ERROR : cause;
            down.onConnectorTerminated();
        }
    }

}
