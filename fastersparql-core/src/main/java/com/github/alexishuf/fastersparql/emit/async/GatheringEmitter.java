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
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.asPooled;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleTerminationError;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.emit.async.Stateful.*;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static java.lang.Integer.numberOfTrailingZeros;

public class GatheringEmitter<B extends Batch<B>> implements Emitter<B> {
    private static final Logger log = LoggerFactory.getLogger(GatheringEmitter.class);
    private static final VarHandle LOCK, FILLING;
    static {
        assert ((STATE_MASK|GRP_MASK) & ~0xff) == 0
                : "Stateful states do not fit in a byte";
        try {
            LOCK    = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainLock", int.class);
            FILLING = MethodHandles.lookup().findVarHandle(GatheringEmitter.class, "plainFilling", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static int globalAvgFillingCap = 64;

    @SuppressWarnings("unused") private int plainLock;
    @SuppressWarnings("unused") private @Nullable B plainFilling;
    private Receiver<B> downstream;
    /** Moving average of FILLING.rows when delivered */
    private short avgFillingCap;
    private byte state = CREATED, delayRelease;
    private short connectorCount, connectorTerminatedCount;
    private @Nullable Receiver<B>[] extraDown;
    private int extraDownCount;
    private @Nullable B scatterTmp, recycled;
    @SuppressWarnings("unchecked") private Connector<B>[] connectors = new Connector[10];
    private final Vars vars;
    private final BatchType<B> batchType;
    private final EmitterStats stats = EmitterStats.createIfEnabled();

    public GatheringEmitter(BatchType<B> batchType, Vars vars) {
        this.vars = vars;
        this.batchType = batchType;
        this.avgFillingCap = (short)Math.min(Short.MAX_VALUE, globalAvgFillingCap);
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
        } finally {
            endDelivery(null);
        }
    }

    private boolean isNovelUpstream(Emitter<B> upstream) {
        for (int i = 0, n = connectorCount; i < n; i++) {
            if (connectors[i].up == upstream) return false;
        }
        return true;
    }

    /*  --- --- --- StreamNode implementation  --- --- --- */

    @Override public Stream<? extends StreamNode> upstream() {
        return Arrays.stream(connectors, 0, connectorCount).map(c -> c.up);
    }

    @Override public String toString() {
        return StreamNodeDOT.minimalLabel(new StringBuilder(), this).toString();
    }

    @Override public String label(StreamNodeDOT.Label type) {
        StringBuilder sb = StreamNodeDOT.minimalLabel(new StringBuilder(), this);
        if (type.showState()) {
            int flaggedState = 0xff&state;
            if ((int)LOCK.getOpaque(this) != 0) flaggedState |= LOCKED_MASK;
            if ((state&IS_TERM_DELIVERED) != 0 && delayRelease == 0)
                flaggedState |= RELEASED_MASK;
            int drBit = numberOfTrailingZeros(DELAY_RELEASE_MASK);
            int drMax = DELAY_RELEASE_MASK>> drBit;
            flaggedState |= Math.min(drMax, delayRelease) << drBit;
            sb.append("\nstate=").append(Flags.DEFAULT.render(flaggedState));
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
            endDelivery(null);
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

    @Override public void rebind(BatchBinding binding) throws RebindException {
        // if state is an undelivered termination, we have onConnectorTerminated() up in
        // the stack
        boolean lock = (state&GRP_MASK) != IS_TERM;
        if (lock)
            while ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
        try {
            if ((state & (IS_INIT|IS_TERM)) == 0)
                throw new RebindStateException(this);
            state = CREATED;
            for (int i = 0, count = connectorCount; i < count; i++)
                connectors[i].up.rebind(binding);
        } finally {
            if (lock)
                LOCK.setRelease(this, 0);
        }
    }

    @Override public void request(long rows) throws NoReceiverException {
        if ((state&IS_TERM) != 0)
            return; // do not propagate
        if (state == CREATED)
            state = ACTIVE;
        for (int i = 0, count = connectorCount; i < count; i++)
            connectors[i].up.request(rows);
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
                    if ((state&IS_TERM) != 0) {// no rebind() from downstream
                        state |= (byte)IS_TERM_DELIVERED;
                        if (delayRelease == 0) {
                            doRelease();
                        }
                    }
                }
            }
        } finally {
            endDelivery(null);
        }
    }

    private void doRelease() {
        recycled   = Batch.recyclePooled(recycled);
        scatterTmp = Batch.recyclePooled(scatterTmp);
        for (int i = 0; i < connectorCount; i++) {
            Connector<B> c = connectors[i];
            B b = c.plainRecycled;
            if (b != null) {
                b.untracedUnmarkPooled().recycle();
                c.plainRecycled = null;
            }
            if (c.projector != null)
                c.projector.release();
        }
        int global = globalAvgFillingCap;
        globalAvgFillingCap = ((global<<3) - global + avgFillingCap)>>3;
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
    }


    /**
     * Acquires the {@code LOCK} mutex, waiting for its release if it is locked.
     */
    private void beginDelivery() {
        for (int i = 0; (int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0; i++) {
            if ((i&31) == 31) Thread.yield();
            else              Thread.onSpinWait();
        }
        deliverFilling(null);
    }

    /**
     * Atomically acquires the {@code LOCK} mutex if it is not locked.
     *
     * @return {@code true} iff this thread is the only one delivering to downstream
     */
    private boolean tryBeginDelivery(Connector<B> connector) {
        if ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) {
            // CAE does not suffer from sporadic failures. Nevertheless, try at least twice
            Thread.onSpinWait();
            if ((int)LOCK.compareAndExchangeAcquire(this, 0, 1) != 0)
                return false; // another thread is delivering
        }

        // got the mutex, we are obligated to deliver FILLING.
        deliverFilling(connector);
        return true; // this thread is the only one delivering
    }

    /** If {@code FILLING != null}, delivers it. Assumes calling thread owns {@code LOCK}. */
    private void deliverFilling(@Nullable Connector<B> connector) {
        try {
            //noinspection unchecked
            B b = (B) FILLING.getAndSetRelease(this, null);
            if (b != null) {
                int avg = avgFillingCap;
                avgFillingCap = (short)Math.max(Short.MAX_VALUE, ((avg << 4) - avg + b.rows) >> 4);
                recycle(connector, deliver(b));
            }
        } catch (Throwable t) {
            LOCK.setRelease(this, 0);
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
    private void endDelivery(@Nullable Connector<B> conn) {
        do {
            // release LOCK before checking FILLING again, this allows another thread to
            // acquire LOCK and thus become responsible for delivery of the FILLING batch
            LOCK.setRelease(this, 0);
        } while (FILLING.getAcquire(this) != null && tryBeginDelivery(conn));
        // (LOCK==0 && FILLING==null) || LOCK==1 (but not owned by this thread)
    }

    /**
     * Recycles {@code b} into {@code this.recycled} or the global pool. Assumes caller
     * thread owns the delivery {@code LOCK}.
     */
    private void recycle(@Nullable Connector<B> conn, B b) {
        if (b != null) {
            b.markPooled();
            if (conn != null && conn.plainRecycled == null)
                conn.plainRecycled = b;
            else if (recycled == null)
                recycled = b;
            else
                b.untracedUnmarkPooled().recycle();
        }
    }

    /**
     * Deliver {@code b} (or a copy of it) to all downstream {@link Receiver}s
     * @return the result of {@link Receiver#onBatch(Batch)} of some downstream {@link Receiver}.
     */
    private @Nullable B deliver(B b) {
        if (ResultJournal.ENABLED)
            ResultJournal.logBatch(this, b);
        if (extraDown != null)  {
            B copy = b.copy(Batch.asUnpooled(scatterTmp));
            scatterTmp = null;
            for (int i = 0, last = extraDownCount -1; i <= last; i++) {
                B offer = deliver(extraDown[i], copy);
                copy = offer == copy || i == last ? offer : b.copy(offer);
            }
            scatterTmp = asPooled(copy);
        }
        return deliver(downstream, b);
    }

    /**
     * Calls {@link Receiver#onRow(Batch, int)} for all downstream receivers.
     */
    private void deliver(B b, int row) {
        if (ResultJournal.ENABLED)
            ResultJournal.logRow(this, b, row);
        if (extraDown != null) {
            for (int i = 0, n = extraDownCount; i < n; i++)
                deliver(extraDown[i], b, row);
        }
        deliver(downstream, b, row);
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
        private static final VarHandle RECYCLED;
        static {
            try {
                RECYCLED = MethodHandles.lookup().findVarHandle(Connector.class, "plainRecycled", Batch.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        final Emitter<B> up;
        final GatheringEmitter<B> down;
        final BatchMerger<B> projector;
        @Nullable B plainRecycled;
        private @Nullable Throwable error;
        private boolean completed, cancelled;

        public Connector(Emitter<B> upstream, GatheringEmitter<B> downstream) {
            (this.up = upstream).subscribe(this);
            this.down = downstream;
            projector = downstream.batchType.projector(downstream.vars, upstream.vars());
        }

        @Override public Stream<? extends StreamNode> upstream() { return Stream.of(up); }

        @Override public String toString() { return "Connector@"+down.toString(); }

        @Override public String label(StreamNodeDOT.Label type) { return down.label(type); }

        @Override public @Nullable B onBatch(B in) {
            if (EmitterStats.ENABLED && down.stats != null)
                down.stats.onBatchReceived(in);
            if (projector != null)
                in = projector.projectInPlace(in);
            if (in.rows == 1) {
                onRow0(in, 0); // will avoid setting FILLING to a singleton
                return in;
            }
            return onBatch0(in, false);
        }

        @SuppressWarnings("unchecked") private @Nullable B onBatch0(B in, boolean recycleOffer) {
            B f, b = in;
            while (true) {
                if (down.tryBeginDelivery(this)) {
                    try {
                        B offer = down.deliver(b);
                        if (recycleOffer || b != in) { // b != in implies previous f.put(in)
                            down.recycle(this, offer);
                            return recycleOffer ? null : in;
                        } else if (offer == null && (offer = plainRecycled) != null) {
                            plainRecycled = null;
                            offer.unmarkPooled();
                        }
                        return offer;
                    } finally {
                        down.endDelivery(this);
                    }
                } else if (FILLING.compareAndExchangeRelease(down, null, b) == null) {
                    // deliver our write to FILLING in case LOCK was released
                    if (down.tryBeginDelivery(this)) // else: thread owning LOCK will deliver
                        down.endDelivery(this);
                    return b == in ? null : in; // if original in got put into b, return in
                } else if (!EMITTER_SVC.yieldToTaskOnce() // retry locking if we could yield
                        && (f=(B)FILLING.getAndSetRelease(down, null)) != null) {
                    // had no task to yield to and stole a FILLING batch, merge b into it
                    f = f.put(b, RECYCLED, this);
                    if (b != in || recycleOffer) { // if b is irrevocably owned by this stack frame
                        if (plainRecycled == null) plainRecycled = asPooled(b);
                        else                       b.recycle();
                    }
                    b = f; // continue delivering the merged batch
                }
            }
        }

        @SuppressWarnings("unchecked") @Override public void onRow(B batch, int row) {
            if (projector == null) {
                onRow0(batch, row);
            } else { // projection requires writing batch[row] into a new batch
                //preferred projection destinations: FILLING, plainRecycled, down.recycle
                B merged = (B)FILLING.getAndSetRelease(down, null);
                if (merged == null && (merged=takeClearLocalRecycled()) == null
                                   && down.tryBeginDelivery(this)) {
                    try {
                        if ((merged=(B)FILLING.getAndSetRelease(down, null)) == null
                                && (merged=takeClearLocalRecycled()) == null
                                && (merged=down.recycled) != null) {
                            down.recycled = null;
                            merged.unmarkPooled();
                            merged.clear();
                        }
                    } finally {  down.endDelivery(this); }
                }
                onBatch0(projector.projectRow(merged, batch, row), true);
            }
        }

        private B takeClearLocalRecycled() {
            var b = plainRecycled;
            if (b != null) {
                b.unmarkPooled();
                b.clear();
            }
            return b;
        }

        @SuppressWarnings("unchecked") private void onRow0(B batch, int row) {
            B f;
            while (true) {
                if (down.tryBeginDelivery(this)) {
                    try {
                        down.deliver(batch, row);
                        return;
                    } finally { down.endDelivery(this); }
                } else if ((f=(B)FILLING.getAndSetRelease(down, null)) != null) {
                    f.putRow(batch, row);
                    onBatch0(f, true);
                    return;
                } else {
                    EMITTER_SVC.yieldToTaskOnce();
                }
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