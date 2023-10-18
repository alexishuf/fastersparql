package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.BindListener;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.asPooled;
import static com.github.alexishuf.fastersparql.batch.type.Batch.asUnpooled;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;

public abstract class BindingStage<B extends Batch<B>> extends Stateful implements Stage<B, B> {
    private static final Logger log = LoggerFactory.getLogger(BindingStage.class);
    private static final VarHandle RECYCLED;
    static {
        try {
            RECYCLED = MethodHandles.lookup().findVarHandle(BindingStage.class, "recycled", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final int LEFT_REQUESTED_CHUNK = 64;
    private static final int LEFT_REQUESTED_MAX   = LEFT_REQUESTED_CHUNK+16;

    /* flags embedded in plainState|S */

    private static final int LEFT_CANCELLED     = 0x01000000;
    private static final int LEFT_COMPLETED     = 0x02000000;
    private static final int LEFT_FAILED        = 0x04000000;
    private static final int LEFT_TERM          = LEFT_COMPLETED | LEFT_CANCELLED | LEFT_FAILED;
    private static final int RIGHT_CANCELLED    = 0x00100000;
    private static final int RIGHT_FAILED       = 0x00200000;
    private static final int RIGHT_TERM         = RIGHT_CANCELLED | RIGHT_FAILED;
    private static final int RIGHT_BINDING      = 0x00400000;
    private static final int RIGHT_STARVED      = 0x00800000;
    private static final int CAN_REQ_RIGHT_MASK = RIGHT_BINDING|RIGHT_STARVED;
    private static final int CAN_REQ_RIGHT      = 0;
    private static final int CAN_BIND_MASK      = RIGHT_STARVED|RIGHT_BINDING;
    private static final int CAN_BIND           = RIGHT_STARVED;
    private static final int FILTER_BIND        = 0x00010000;
    private static final Flags BINDING_STAGE_FLAGS =
            Flags.DEFAULT.toBuilder()
                    .flag(LEFT_CANCELLED, "LEFT_CANCELLED")
                    .flag(LEFT_COMPLETED, "LEFT_COMPLETED")
                    .flag(LEFT_FAILED, "LEFT_FAILED")
                    .flag(RIGHT_CANCELLED, "RIGHT_CANCELLED")
                    .flag(RIGHT_FAILED, "RIGHT_FAILED")
                    .flag(RIGHT_BINDING, "RIGHT_BINDING")
                    .flag(RIGHT_STARVED, "RIGHT_STARVED")
                    .flag(FILTER_BIND, "FILTER_BIND").build();

    private static final byte PASSTHROUGH_NOT   = 0;
    private static final byte PASSTHROUGH_LEFT  = 1;
    private static final byte PASSTHROUGH_RIGHT = 2;

    protected final BatchType<B> batchType;
    protected final Vars outVars;
    private final Emitter<B> leftUpstream;
    private final RightReceiver rightRecv;
    private @Nullable B fillingLB;
    private @Nullable B recycled;
    private int lr = -1;
    private @Nullable B lb;
    private int leftPending;
    private long requested;
    private final BatchBinding intBinding;
    private @Nullable BatchBinding extBinding;
    private Throwable error = UNSET_ERROR;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public static final class ForPlan<B extends Batch<B>> extends BindingStage<B> {
        public ForPlan(EmitBindQuery<B> bq, Vars rebindHint, boolean weakDedup,
                       @Nullable Vars projection) {
            super(bq.bindings, bq.type, bq, projection == null ? bq.resultVars() : projection,
                  bq.parsedQuery().emit(
                          bq.bindings.batchType(),
                          bq.bindings.vars().union(rebindHint),
                          weakDedup));
        }
    }

    public static final class ForSparql<B extends Batch<B>> extends BindingStage<B> {
        public ForSparql(EmitBindQuery<B> bq, Vars outerRebindHint, SparqlClient client) {
            super(bq.bindings, bq.type, bq, bq.resultVars(),
                  client.emit(bq.batchType(), bq.query, bq.bindings.vars().union(outerRebindHint)));
        }
    }

    /**
     * Create a new {@link BindingStage}.
     *
     * @param bindings {@link Emitter} for bindings rows
     * @param type type of join to be performed
     * @param listener object that will receive {@link EmitBindQuery#emptyBinding(long)} and
     *                 {@link EmitBindQuery#nonEmptyBinding(long)} calls
     * @param outVars vars in the batches output by this {@link BindingStage}
     * @param rightUpstream A template emitter for the right-side of the join: For each row in
     *                      {@code bindings} there will be a {@link Emitter#rebind(BatchBinding)}
     */
    public BindingStage(Emitter<B> bindings, BindType type, @Nullable BindListener listener,
                        Vars outVars, Emitter<B> rightUpstream) {
        super(CREATED | RIGHT_STARVED | (type.isJoin() ? 0 : FILTER_BIND),
              BINDING_STAGE_FLAGS);
        batchType = bindings.batchType();
        this.outVars = outVars;
        Vars lVars = bindings.vars(), rVars = rightUpstream.vars();
        rightUpstream.rebindAcquire();
        BatchMerger<B> merger;
        byte passthrough = PASSTHROUGH_NOT;
        if (type.isJoin()) {
            if (outVars.equals(lVars)) {
                passthrough = PASSTHROUGH_LEFT;
                merger = null;
            } else if (outVars.equals(rVars)) {
                passthrough = PASSTHROUGH_RIGHT;
                merger = null;
            } else {
                merger = batchType.merger(outVars, lVars, rVars);
                merger.assumeThreadSafe();
            }
        } else {
            merger = null;
        }
        this.rightRecv = new RightReceiver(merger, passthrough, type, rightUpstream, listener);
        this.leftUpstream = bindings;
        this.intBinding = new BatchBinding(lVars);
        bindings.subscribe(this);
        rightUpstream.subscribe(rightRecv);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    @Override public Vars         vars()      { return outVars;   }
    @Override public BatchType<B> batchType() { return batchType; }

    @Override public String toString() {
        return label(StreamNodeDOT.Label.WITH_STATE_AND_STATS).replace("\n", " ");
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(64), this);
        if (type != StreamNodeDOT.Label.MINIMAL)
            sb.append('[').append(rightRecv.type.name()).append("] vars=").append(outVars);
        if (type.showState()) {
            StreamNodeDOT.appendRequested(sb.append("\nrequested="), requested)
                    .append(" state=").append(flags.render(state()));
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.concat(Stream.of(leftUpstream), Stream.ofNullable(rightRecv.upstream));
    }

    @Override protected void doRelease() {
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
        rightRecv.rebindRelease();
        rightRecv.recycled = Batch.recyclePooled(rightRecv.recycled);
        recycled = Batch.recyclePooled(recycled);
        lb                 = batchType.recycle(lb);
        fillingLB          = batchType.recycle(fillingLB);
        if (intBinding != null) {
            intBinding.remainder = null;
            intBinding.attach(null, 0);
        }
        super.doRelease();
    }

    /* --- --- --- Receiver side (bindings) --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> upstream) {
        if (upstream != this.leftUpstream)
            throw new MultipleRegistrationUnsupportedException(this);
        return this;
    }

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        int rows = batch == null ? 0 : batch.rows;
        if (rows == 0)
            return batch;
        B offer = null;
        int state = lock(statePlain());
        try {
            if (ENABLED)
                journal("onBatch", rows, "st=", state, flags, "bStage=", this);
            if (lb == null) {
                lb = batch;
            } else if (fillingLB == null) {
                fillingLB = batch;
            } else {
                B tmp = fillingLB;
                fillingLB = null;
                state = unlock(state);
                tmp = tmp.put(batch, RECYCLED, this);
                offer = batch;
                state = lock(state);
                if   (lb == null) lb        = tmp;
                else              fillingLB = tmp;
            }
            leftPending -= rows;
            if (offer == null && (offer = recycled) != null) {
                recycled = null;
                offer.unmarkPooled();
            }
            if ((state&CAN_BIND_MASK) == CAN_BIND)
                state = startNextBinding(state);
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
        }
        return offer;
    }

    @Override public void onRow(B b, int row) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRowReceived();
        int state = lock(statePlain());
        try {
            if (ENABLED)
                journal("onRow", row, "st=", state, flags, "bStage=", this);
            B dst;
            if (fillingLB != null) {
                dst = fillingLB;
                fillingLB = null;
            } else {
                dst = batchType.empty(asUnpooled(recycled), 1, b.cols, b.localBytesUsed(row));
                recycled = null;
            }
            state = unlock(state);

            dst.putRow(b, row);

            state = lock(state);
            if (lb == null) lb        = dst;
            else            fillingLB = dst;
            --leftPending;
            if ((state&CAN_BIND_MASK) == CAN_BIND)
                state = startNextBinding(state);
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
        }
    }

    @Override public void onComplete()             { leftTerminated(LEFT_COMPLETED, null); }
    @Override public void onCancelled()            { leftTerminated(LEFT_CANCELLED, null); }
    @Override public void onError(Throwable cause) { leftTerminated(LEFT_FAILED,    cause); }

    /* --- --- --- Emitter side of the Stage --- --- --- */

    @Override public void rebindAcquire() {
        delayRelease();
        leftUpstream.rebindAcquire();
    }

    private static final int REBIND_REL_CAN_TERM_MASK = LEFT_TERM|RIGHT_STARVED|DELAY_RELEASE_MASK;
    private static final int REBIND_REL_CAN_TERM      = LEFT_TERM|RIGHT_STARVED;
    @Override public void rebindRelease() {
        leftUpstream.rebindRelease();
        int st = allowRelease();
        if ((st & REBIND_REL_CAN_TERM_MASK) == REBIND_REL_CAN_TERM) {
            st = lock(st);
            try {
                terminateStage(st);
            } finally {
                if ((st&LOCKED_MASK) != 0)
                    unlock(st);
            }
        }
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        int st = resetForRebind(LEFT_TERM|RIGHT_TERM, RIGHT_STARVED|LOCKED_MASK);
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            requested = 0;
            leftPending = 0;
            leftUpstream.rebind(binding);
            if (extBinding == null || !binding.vars.equals(extBinding.vars))
                updateExtRebindVars(binding);
            extBinding = binding;
            intBinding.remainder = binding;
            if (ResultJournal.ENABLED)
                ResultJournal.rebindEmitter(this, binding);
            unlock(st);
        } catch (Throwable t) {
            this.error = t;
            unlock(st);
            markDelivered(st, FAILED);
            throw t;
        }
    }

    private void updateExtRebindVars(BatchBinding binding) {
        intBinding.vars(leftUpstream.vars().union(binding.vars));
    }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        if (rightRecv.downstream != null && rightRecv.downstream != receiver)
            throw new MultipleRegistrationUnsupportedException(this);
        rightRecv.downstream = receiver;
        if (ENABLED)
            journal("subscribed", receiver, "to", this);
    }

    @Override public void cancel() {
        if (moveStateRelease(statePlain(), CANCEL_REQUESTED)) { // cancel both left and right
            leftUpstream.cancel();
            var rrUpstream = rightRecv.upstream;
            if (rrUpstream != null)
                rrUpstream.cancel();
        }
    }

    @Override public void request(long rows) {
        int state = statePlain();
        long rightRows = (state&FILTER_BIND) == 0 ? rows : 2;
        if ((state & IS_INIT) != 0 && moveStateRelease(state, ACTIVE))
            state = (state&FLAGS_MASK) | ACTIVE;
        state = lock(state);
        try {
            long maybeOverflow = requested + rows;
            requested = maybeOverflow < 0 ? Long.MAX_VALUE : maybeOverflow;
            maybeRequestLeft();
            if ((state&CAN_REQ_RIGHT_MASK) == CAN_REQ_RIGHT)
                rightRecv.upstream.request(rightRows);
        } finally {
            unlock(state);
        }
    }

    /* --- --- --- internal helpers --- --- --- */

    private void maybeRequestLeft() {
        B lb = this.lb, fillingLB = this.fillingLB;
        // count queued left rows into LEFT_REQUESTED_MAX limit
        int queued = (lb == null ? 0 : lb.rows-lr) + (fillingLB == null ? 0 : fillingLB.rows);
        // do not request more from left if downstream is not expecting more rows.
        // leftPending+queued must never exceed LEFT_REQUESTED_MAX.
        // always request LEFT_REQUESTED_CHUNK rows.
        if (requested > 0 && LEFT_REQUESTED_MAX - leftPending - queued >= LEFT_REQUESTED_CHUNK) {
            leftPending += LEFT_REQUESTED_CHUNK;
            leftUpstream.request(LEFT_REQUESTED_CHUNK);
        }
    }

    private void leftTerminated(int flag, @Nullable Throwable error) {
        int state = lock(statePlain());
        try {
            if (ENABLED)
                journal("leftTerminated st=", state, flags, "flag=", flag, flags, "bStage=", this);
            if (error != null && this.error == UNSET_ERROR)
                this.error = error;
            if ((state & LEFT_TERM) == 0) {
                state = setFlagsRelease(state, flag);
                if ((state & (RIGHT_TERM|RIGHT_STARVED)) != 0) {
                    state = terminateStage(state);
                } else if (lr == (lb == null ? -1 : lb.rows-1)
                           && (fillingLB == null || fillingLB.rows == 0)
                           && (state&DELAY_RELEASE_MASK) == 0) {
                    rightRecv.rebindRelease();
                }
            }
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
        }
    }

    private void rightTerminated(int flag, @Nullable Throwable error) {
        int state = lock(statePlain());
        try {
            if (ENABLED)
                journal("rightTerminated st=", state, flags, "flag=", flag, flags, "bStage=", this);
            if (error != null && this.error == UNSET_ERROR)
                this.error = error;
            if ((state & RIGHT_TERM) == 0) {
                state = setFlagsRelease(state, flag);
                if ((state & LEFT_TERM) != 0)
                    state = terminateStage(state);
            }
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
        }
    }

    private int terminateStage(int state) {
        int termState;
        if ((state & (LEFT_FAILED | RIGHT_FAILED)) != 0) {
            termState = FAILED;
        } else if ((state & (LEFT_CANCELLED | RIGHT_CANCELLED)) != 0) {
            termState = CANCELLED;
        } else {
            assert (state & (LEFT_TERM|RIGHT_TERM)) == LEFT_COMPLETED;
            termState = COMPLETED;
        }
        lr = -1;
        var downstream = rightRecv.downstream;
        state = unlock(state);
        if (moveStateRelease(state, termState)) {
            try {
                switch (termState) {
                    case FAILED    -> downstream.onError(error);
                    case CANCELLED -> downstream.onCancelled();
                    case COMPLETED -> downstream.onComplete();
                }
            } catch (Throwable t) {
                Emitters.handleTerminationError(downstream, this, t);
            }
            markDelivered(state, termState);
        }
        return (state&FLAGS_MASK)|termState;
    }

    @SuppressWarnings("SameReturnValue") private @Nullable B recycle(B b) {
        if (recycled != null)
            return b.recycle();
        b.markPooled();
        recycled = b;
        return null;
    }

    /* --- --- --- right-side processing --- --- --- */

    private int startNextBinding(int st) {
        B lb = this.lb;
        int lr = this.lr+1;
        try {
            if (lb == null) {
                lr = -1;
            } else if (lr >= lb.rows) {
                recycle(lb);
                this.lb = lb = fillingLB;
                fillingLB = null;
                if (lb != null && lb.rows == 0) this.lb = lb = recycle(lb);
                lr = lb == null ? -1 : 0;
            }// else: lb != null && lr < lb.rows
            this.lr = lr;
            maybeRequestLeft();
            if (lb == null) {
                if ((st & LEFT_TERM) == 0) st = unlock(st, 0, RIGHT_STARVED);
                else                       st = terminateStage(st);
            } else {
                // release lock but forbid onBatch() from calling startNextBinding()
                // and forbid request() from calling upstream.request()
                st = unlock(st, RIGHT_STARVED, RIGHT_BINDING);
                rebind(intBinding.attach(lb, lr), rightRecv.upstream);
                st = lock(st);
                ++rightRecv.bindingSeq;
                rightRecv.upstreamEmpty = true;
                rightRecv.listenerNotified = false;
                if (ENABLED)
                    journal("startNextBinding st=", st, flags, "seq=", rightRecv.bindingSeq, "stage=", this);
                rightRecv.upstream.request((st & FILTER_BIND) == 0 ? requested : 2);
                st = unlock(st, RIGHT_BINDING, 0); // rebind complete, allow upstream.request()
            }
        } catch (Throwable t) {
            log.error("{}.startNextBinding() failed after seq={}", this, rightRecv.bindingSeq, t);
            if ((st&LOCKED_MASK) != 0)
                st = unlock(st); // avoid self-deadlock on rightTerminated/lock()
            rightTerminated(RIGHT_FAILED, t);
            rightRecv.upstream.cancel();
        }
        return st;
    }

    /**
     * Called once {@code emitter}, which supplies the right-side solutions of the join, has
     * reached a {@link #IS_TERM} state. Implementations that can efficiently
     * perform lexical joins ({@code FILTER(str(?leftVar) = str(?rightVar))}) can
     * {@link Emitter#rebind(BatchBinding)} the right-side {@code emitter} with the next lexical
     * variant of the right-side vars participating in a lexical join.
     *
     * <p>If lexical joins are not implemented or if there are no more variants to explore,
     * implementations must return null. The returned {@link Emitter} SHOULD be {@code emitter}
     * itself for better efficiency, but can be another instance so long the new instance can
     * efficiently (and correctly) handle a {@link Emitter#rebind(BatchBinding)} when the next
     * left-side row starts evaluation.</p>
     *
     * @param binding the same {@link BatchBinding} given in the last
     *        {@link Emitter#rebind(BatchBinding)} of {@code emitter} by this {@link BindingStage},
     *        with the original values, unaffected by lexical binding.
     * @return {@code null} if lexical joins are not implemented or if there are no more lexical
     *         variants for the current binding ({@code lb, lr}), else an {@link Emitter} that
     *         may provide additional solutions for the right-side of the join.
     */
    protected boolean lexContinueRight(BatchBinding binding, Emitter<B> emitter) {
        return false;
    }

    /**
     * By default, equivalent to {@code rightEmitter.rebind(binding)}. This may be overridden
     * to implement non-standard joining, e.g., lexical joining.
     */
    protected void rebind(BatchBinding binding, Emitter<B> rightEmitter) {
        rightEmitter.rebind(binding);
    }

    private final class RightReceiver implements Receiver<B> {
        private @MonotonicNonNull Receiver<B> downstream;
        private @Nullable B recycled;
        private final Emitter<B> upstream;
        private final @Nullable BatchMerger<B> merger;
        private final BindType type;
        private final @Nullable BindListener bindingListener;
        private final byte passThrough;
        private boolean upstreamEmpty, listenerNotified, rebindReleased;
        private long bindingSeq = -1;

        public RightReceiver(@Nullable BatchMerger<B> merger, byte passThrough,
                             BindType type, Emitter<B> rightUpstream, @Nullable BindListener bindingListener) {
            this.upstream = rightUpstream;
            this.merger = merger;
            this.passThrough = passThrough;
            this.type = type;
            this.bindingListener = bindingListener;
        }

        @Override public Stream<? extends StreamNode> upstream() {
            return Stream.of(upstream);
        }

        @Override public String toString() {
            return format("%s.RightReceiver@%08x", BindingStage.this, identityHashCode(this));
        }

        public void rebindRelease() {
            if (rebindReleased) return;
            rebindReleased = true;
            upstream.rebindRelease();
        }

        @Override public void onRow(B batch, int row) {
            if (batch == null)
                return;
            if (passThrough == PASSTHROUGH_NOT) {
                B tmp;
                int st = lock(statePlain());
                try {
                    tmp = batchType.empty(asUnpooled(recycled), 1, batch.cols, batch.localBytesUsed(row));
                    recycled = null;
                } finally { st = unlock(st); }

                tmp.putRow(batch, row);
                tmp = onBatch(tmp);
                if (tmp != null) {
                    st = lock(st);
                    try {
                        if (recycled == null) recycled = asPooled(tmp);
                        else                  tmp.recycle();
                    } finally { unlock(st); }
                }
            } else {
                upstreamEmpty = false;
                switch (type) {
                    case NOT_EXISTS,MINUS -> {
                        if (!listenerNotified && bindingListener != null) {
                            bindingListener.emptyBinding(bindingSeq);
                            listenerNotified = true;
                        }
                        upstream.cancel();
                    }
                    default ->  {
                        B b;
                        int bRow;
                        if (passThrough == PASSTHROUGH_LEFT) {
                            b = lb;
                            bRow = lr;
                        } else {
                            b = batch;
                            bRow = row;
                        }
                        try {
                            if (!listenerNotified && bindingListener != null) {
                                bindingListener.nonEmptyBinding(bindingSeq);
                                listenerNotified = true;
                            }
                            if (ResultJournal.ENABLED)
                                ResultJournal.logRow(BindingStage.this, b, bRow);
                            downstream.onRow(b, bRow);
                            int st = lock(statePlain());
                            --requested;
                            unlock(st);
                        } catch (Throwable t) {
                            handleEmitError(downstream, BindingStage.this, false, t);
                        }
                    }
                }
            }
        }

        @Override public B onBatch(B rb) {
            if (rb == null || rb.rows == 0)
                return rb;
            upstreamEmpty = false;
            return switch (type) {
                case JOIN,LEFT_JOIN,EXISTS -> merge(rb);
                case NOT_EXISTS,MINUS      -> {
                    if (!listenerNotified && bindingListener != null) {
                        bindingListener.emptyBinding(bindingSeq);
                        listenerNotified = true;
                    }
                    upstream.cancel();
                    yield rb;
                }
            };
        }

        private @Nullable B merge(@Nullable B rb) {
            assert lb != null;
            B b = null;
            boolean rightPassThrough = merger == null && passThrough == PASSTHROUGH_RIGHT;
            if (rightPassThrough) {
                b = rb;
                assert rb != null;
            } else if (merger != null) {
                if (!merger.hasRecycled()) {
                    merger.recycle(asUnpooled(recycled));
                    recycled = null;
                }
                b = merger.merge(null, lb, lr, rb);
            }

            try {
                // notify binding is not empty
                if (!listenerNotified && bindingListener != null) {
                    bindingListener.nonEmptyBinding(bindingSeq);
                    listenerNotified = true;
                }

                // update stats and requested
                if (EmitterStats.ENABLED && stats != null) {
                    if (b == null) stats.onRowDelivered();
                    else           stats.onBatchDelivered(b);
                }
                int state = lock(statePlain());
                requested -= b == null ? 1 : b.rows;
                unlock(state);

                // deliver batch/row
                if (b == null) {
                    if (ResultJournal.ENABLED)
                        ResultJournal.logRow(BindingStage.this, lb, lr);
                    downstream.onRow(lb, lr);
                } else {
                    if (ResultJournal.ENABLED)
                        ResultJournal.logBatch(BindingStage.this, b);
                    b = downstream.onBatch(b);
                    if (!rightPassThrough && b != null) {
                        if      (recycled          == null) recycled = asPooled(b);
                        else if (merger.recycle(b) != null) b.recycle();
                    }
                }
            } catch (Throwable t) {
                handleEmitError(downstream, BindingStage.this, false, t);
            }
            return rightPassThrough ? b : rb;
        }

        @Override public void onComplete() {
            assert lb != null;
            try {
                if (lexContinueRight(intBinding, upstream)) {
                    if (ENABLED)
                        journal("lexContinueRight, bStage=", BindingStage.this);
                    upstream.request((statePlain()&FILTER_BIND) == 0 ? requested : 2);
                } else {
                    if (upstreamEmpty) {
                        switch (type) {
                            case JOIN,EXISTS ->  {
                                if (!listenerNotified && bindingListener != null) {
                                    bindingListener.emptyBinding(bindingSeq);
                                    listenerNotified = true;
                                }
                            }
                            case LEFT_JOIN,NOT_EXISTS,MINUS -> merge(null);
                        }
                    }
                    startNextBinding(lock(statePlain()));
                }
            } catch (Throwable t) {
                log.error("onComplete() failed", t);
                rightTerminated(RIGHT_FAILED, t);
            }
        }

        @Override public void onCancelled() {
            if (type.isJoin()) rightTerminated(RIGHT_CANCELLED, null);
            else               onComplete();
        }

        @Override public void onError(Throwable cause) {
            rightTerminated(RIGHT_FAILED, cause);
        }
    }
}