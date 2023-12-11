package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.FSProperties;
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
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;

public class GatheringEmitter<B extends Batch<B>> implements Emitter<B> {
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
    private final short requestChunk;
    @SuppressWarnings("unused") private long plainReq;
    private byte state = CREATED, delayRelease;
    private short connectorTerminatedCount;
    private final BatchType<B> batchType;
    private int lastRebindSeq = -1;
    private final Vars vars;
    private Vars bindableVars = Vars.EMPTY;
    @SuppressWarnings("unused") private int plainStatsRcvLock;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public GatheringEmitter(BatchType<B> batchType, Vars vars) {
        this.vars         = vars;
        this.batchType    = batchType;
        int cols = Math.max(1, vars.size());
        int b = FSProperties.emitReqChunkBatches();
        this.requestChunk = (short)Math.max(b, b*batchType.preferredTermsPerBatch()/cols);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, vars);
    }


    public void subscribeTo(Emitter<B> upstream) {
        if ((state&STATE_MASK) != CREATED)
            throw new RegisterAfterStartException(this);
        beginDelivery();
        try {
            assert isNovelUpstream(upstream) : "Already subscribed to upstream";
            if (connectorCount >= connectors.length)
                connectors = Arrays.copyOf(connectors, connectorCount*2);
            connectors[connectorCount++] = new Connector<>(upstream, this);
            bindableVars = bindableVars.union(upstream.bindableVars());
        } finally {
            endDelivery();
        }
    }

    private boolean isNovelUpstream(Emitter<B> upstream) {
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
        StringBuilder sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showState()) {
            int flaggedState = 0xff&state;
            if ((Thread)OWNER.getAcquire(this) != null) flaggedState |= LOCKED_MASK;
            if ((state&IS_TERM_DELIVERED) != 0 && delayRelease == 0)
                flaggedState |= RELEASED_MASK;
            int drBit = numberOfTrailingZeros(DELAY_RELEASE_MASK);
            int drMax = DELAY_RELEASE_MASK>> drBit;
            flaggedState |= Math.min(drMax, delayRelease) << drBit;
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

    @Override public void cancel() {
        if ((state&(IS_INIT|IS_LIVE)) != 0) {
            for (int i = 0, count = connectorCount; i < count; i++)
                connectors[i].up.cancel();
        }
    }

    @Override public void rebindAcquire() {
        delayRelease = (byte)Math.min(Byte.MAX_VALUE, delayRelease+1);
        for (int i = 0, count = connectorCount; i < count; i++)
            connectors[i].up.rebindAcquire();
    }

    @Override public void rebindRelease() {
        boolean release = delayRelease == 1;
        delayRelease = (byte)Math.max(0, delayRelease-1);
        for (int i = 0, count = connectorCount; i < count; i++)
            connectors[i].up.rebindRelease();
        if (release && EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
    }

    @Override public void rebindPrefetch(BatchBinding binding) {
        for (int i = 0; i < connectorCount; i++)
            connectors[i].rebindPrefetch(binding);
    }

    @Override public void rebindPrefetchEnd(boolean sync) {
        for (int i = 0; i < connectorCount; i++)
            connectors[i].rebindPrefetchEnd(sync);
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (binding.sequence == lastRebindSeq)
            return; // duplicate rebind() due to diamond in emitter graph
        lastRebindSeq = binding.sequence;
        lock();
        try {
            if ((state&(IS_INIT|IS_TERM)) == 0)
                throw new RebindStateException(this);
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
        if (delayRelease == 0) {
            if (ENABLED) journal("releasing", this);
            for (int i = 0; i < connectorCount; i++) {
                if (connectors[i].projector != null)
                    connectors[i].projector.release();
            }
            if (EmitterStats.ENABLED && stats != null)
                stats.report(log, this);
        }
    }

    private void onBatchReceived(B b) {
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
        EmitterService.beginSpin();
        for (int i = 0; OWNER.compareAndExchangeAcquire(this, NO_OWNER, me) != NO_OWNER; ++i) {
            if ((i&15) == 0) Thread.yield();
            else             Thread.onSpinWait();
        }
        EmitterService.endSpin();
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
            B b = (B) FILLING.getAndSetRelease(this, null);
            if (b != null)
                batchType.recycle(deliver(b));
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
     * Deliver {@code b} (or a copy of it) to all downstream {@link Receiver}s
     * @return the result of {@link Receiver#onBatch(Batch)} of some downstream {@link Receiver}.
     */
    private @Nullable B deliver(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchDelivered(b);
        try {
            return downstream.onBatch(b);
        } catch (Throwable t) {
            handleEmitError(downstream, this, (state&IS_TERM)!=0, t);
            return null;
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
    private static final class Connector<B extends Batch<B>> implements Receiver<B> {
        private static final VarHandle CONN_REQ;
        static {
            try {
                CONN_REQ = MethodHandles.lookup().findVarHandle(Connector.class, "plainConnReq", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        final Emitter<B> up;
        final GatheringEmitter<B> down;
        final BatchType<B> bt;
        int plainConnReq;
        final BatchMerger<B> projector;
        private boolean completed, cancelled;
        private @Nullable Throwable error;


        public Connector(Emitter<B> upstream, GatheringEmitter<B> downstream) {
            (this.up          = upstream).subscribe(this);
            this.down         = downstream;
            this.bt           = downstream.batchType;
            this.projector    = bt.projector(downstream.vars, upstream.vars());
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

        public void rebindPrefetchEnd(boolean sync) { up.rebindPrefetchEnd(sync); }

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

        @SuppressWarnings("unchecked") @Override public @Nullable B onBatch(B in) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.onBatchReceived(in);
            requestNextChunk(in.totalRows());
            if (projector != null)
                in = projector.projectInPlace(in);
            B f, b = in;
            boolean spinNotified = false;
            while (true) {
                if (down.tryBeginDelivery()) {
                    try {
                        if ((b = down.deliver(b)) != in)
                            b = bt.recycle(b);
                        break;
                    } finally { down.endDelivery(); }
                } else if (FILLING.compareAndExchangeRelease(down, null, b) == null) {
                    // deliver our write to FILLING in case LOCK was released
                    if (down.tryBeginDelivery())  // else: thread owning LOCK will deliver
                        down.endDelivery();
                    b = null;
                    break;
                } else if (!spinNotified) {
                    spinNotified = true;
                    EmitterService.beginSpin();
                } else if ((f=(B)FILLING.getAndSetRelease(down, null)) != null) {
                    f.append(b); // preserve in, try delivering combined batch
                    b = f;
                }
            }
            if (spinNotified)
                EmitterService.endSpin();
            return b;
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
