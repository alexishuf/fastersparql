package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Emitters;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindStateException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.Async;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
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
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.System.identityHashCode;
import static java.lang.Thread.currentThread;

public class GatheringEmitter<B extends Batch<B>> implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(GatheringEmitter.class);
    private static final Thread NO_OWNER = null;
    private static final VarHandle OWNER, FILLING, REQ;
    static {
        assert ((STATE_MASK|GRP_MASK) & ~0xff) == 0
                : "Stateful states do not fit in a byte";
        try {
            OWNER   = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainOwner", Thread.class);
            FILLING = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainFilling", Batch.class);
            REQ     = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainRequested", long.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    @SuppressWarnings("unused") private Thread plainOwner;
    private int lockedLevel;
    @SuppressWarnings("unused") private @Nullable B plainFilling;
    private Receiver<B> downstream;
    private short extraDownCount, connectorCount;
    private @MonotonicNonNull Receiver<B>[] extraDown;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[10];
    @SuppressWarnings("unused") private long plainRequested;
    private byte state = CREATED, delayRelease;
    private short connectorTerminatedCount;
    private final short requestChunk;
    private final BatchType<B> batchType;
    private int lastRebindSeq = -1;
    private final Vars vars;
    private Vars bindableVars = Vars.EMPTY;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public GatheringEmitter(BatchType<B> batchType, Vars vars) {
        this.vars         = vars;
        this.batchType    = batchType;
        int cols = Math.max(1, vars.size());
        this.requestChunk = (short)Math.max(8, batchType.preferredTermsPerBatch()/cols);
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
            StreamNodeDOT.appendRequested(sb.append(", requested="), plainRequested);
        }
        if (EmitterStats.ENABLED && type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    /*  --- --- --- Emitter implementation  --- --- --- */

    @Override public Vars               vars() { return vars; }
    @Override public BatchType<B>  batchType() { return batchType; }
    @Override public boolean      canScatter() { return true; }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException {
        beginDelivery();
        try {
            if (downstream == receiver)
                return;
            if (state != CREATED) {
                throw new RegisterAfterStartException(this);
            } else if (downstream == null) {
                downstream = receiver;
            } else {
                if (extraDown == null)//noinspection unchecked
                    extraDown = new Receiver[10];
                for (int i = 0; i < extraDownCount; i++) {
                    if (extraDown[i] == receiver) return;
                }
                if (extraDownCount == extraDown.length)
                    extraDown = Arrays.copyOf(extraDown, extraDownCount<<1);
                extraDown[extraDownCount++] = receiver;
            }
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
        if (ENABLED)
            journal("request", rows, "on", this);
        if (rows == 0 || (state&IS_TERM) != 0)
            return;
        if (state == CREATED)
            state = ACTIVE;
        Async.safeAddAndGetRelease(REQ, this, plainRequested, rows);
        for (int i = 0, count = connectorCount; i < count; i++)
            connectors[i].updateRequested(0);
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
                    for (int i = 0, n = extraDownCount; i < n; i++)
                        deliverTermination(extraDown[i], termState, firstError);
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
        REQ.getAndAddRelease(this, (long)-b.totalRows());
        B copy = null;
        for (int i = 0, n = extraDownCount; i < n; i++)
            copy = deliver(extraDown[i], copy == null ? b.dup() : copy);
        batchType.recycle(copy);
        return deliver(downstream, b);
    }

    /**
     * Calls {@link Receiver#onRow(Batch, int)} for all downstream receivers.
     */
    private void deliver(B b, int row) {
        if (ResultJournal.ENABLED)
            ResultJournal.logRow(this, b, row);
        REQ.getAndAddRelease(this, -1L);
        deliver(downstream, b, row);
        for (int i = 0, n = extraDownCount; i < n; i++)
            deliver(extraDown[i], b, row);
    }

    /**
     * Safely call {@link Receiver#onBatch(Batch)} for {@code receiver} and {@code b}
     * @return the batch returned be {@link Receiver#onBatch(Batch)}
     */
    private @Nullable B deliver(Receiver<B> downstream, B b) {
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onBatchDelivered(b);
            return downstream.onBatch(b);
        } catch (Throwable t) {
            handleEmitError(downstream, this, (state&IS_TERM)!=0, t);
            return null;
        }
    }

    /**
     * Safely call {@link Receiver#onRow(Batch, int)} for {@code downstream}, {@code b}
     * and {@code row}.
     */
    private void deliver(Receiver<B> downstream, B b, int row) {
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRowDelivered();
            downstream.onRow(b, row);
        } catch (Throwable t) {
            handleEmitError(downstream, this, (state&IS_TERM)!=0, t);
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
                CONN_REQ = MethodHandles.lookup().findVarHandle(Connector.class, "plainConnectorRequested", int.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        final Emitter<B> up;
        final GatheringEmitter<B> down;
        final BatchType<B> bt;
        int plainConnectorRequested;
        final BatchMerger<B> projector;
        private boolean completed, cancelled;
        private @Nullable Throwable error;


        public Connector(Emitter<B> upstream, GatheringEmitter<B> downstream) {
            (this.up          = upstream).subscribe(this);
            this.down         = downstream;
            this.bt           = downstream.batchType;
            this.projector    = bt.projector(downstream.vars, upstream.vars());
        }

        void updateRequested(int received) {
            int curr = (int)CONN_REQ.getAndAddAcquire(this, -received)-received;
            while (curr <= down.requestChunk>>1) {
                int req = (int)Math.min((long)REQ.getOpaque(down), down.requestChunk);
                if (req > 0) {
                    int ex = curr, n = Math.max(curr, 0) + req;
                    if ((curr = (int)CONN_REQ.compareAndExchangeRelease(this, ex, n)) == ex) {
                        up.request(req);
                        break;
                    }
                } else {
                    break;
                }
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

        @Override public @Nullable B onBatch(B in) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.stats.onBatchReceived(in);
            updateRequested(in.totalRows());
            if (projector != null) {
                in = projector.projectInPlace(in);
            }
            if (in.rows == 1 && in.next == null && onRow0(in, 0))
                return in;
            return onBatch0(in);
        }

        @SuppressWarnings("unchecked") private @Nullable B onBatch0(B in) {
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

        @SuppressWarnings("unchecked") @Override public void onRow(B batch, int row) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.stats.onRowReceived();
            updateRequested(1);
            B copy;
            if (projector != null) {
                copy = (B)FILLING.getAndSetRelease(down, null);
                projector.projectRow(copy, batch, row);
            } else if (onRow0(batch, row)) {
                return;
            } else {
                (copy = bt.create(batch.cols)).putRow(batch, row);
            }
            bt.recycle(onBatch0(copy));
        }

        @SuppressWarnings("unchecked") private boolean onRow0(B batch, int row) {
            B f;
            if (down.tryBeginDelivery()) {
                try {
                    down.deliver(batch, row);
                    return true;
                } finally { down.endDelivery(); }
            } else if ((f=(B)FILLING.getAndSetRelease(down, null)) != null) {
                f.putRow(batch, row);
                bt.recycle(onBatch0(f));
                return true;
            }
            return false;
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
