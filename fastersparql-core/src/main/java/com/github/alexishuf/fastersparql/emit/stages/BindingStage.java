package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.BindListener;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.JournalNamed;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.detachDistinctTail;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.MINIMAL;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.System.identityHashCode;

public abstract class BindingStage<B extends Batch<B>, S extends BindingStage<B, S>>
        extends Stateful<S>
        implements Stage<B, B, S>, HasFillingBatch<B> {
    private static final Logger log = LoggerFactory.getLogger(BindingStage.class);

    /* flags embedded in plainState|S */

    private static final int LEFT_CANCELLED     = 0x01000000;
    private static final int LEFT_COMPLETED     = 0x02000000;
    private static final int LEFT_FAILED        = 0x04000000;
    private static final int LEFT_TERM          = LEFT_COMPLETED|LEFT_CANCELLED|LEFT_FAILED;
    private static final int RIGHT_CANCELLED    = 0x00100000;
    private static final int RIGHT_FAILED       = 0x00200000;
    private static final int RIGHT_TERM         = RIGHT_CANCELLED|RIGHT_FAILED;
    private static final int RIGHT_BINDING      = 0x00400000;
    private static final int RIGHT_STARVED      = 0x00800000;
    private static final int CAN_REQ_RIGHT_MASK = RIGHT_BINDING|RIGHT_STARVED;
    private static final int CAN_REQ_RIGHT      = 0;
    private static final int LEFT_CAN_BIND_MASK = RIGHT_STARVED|RIGHT_BINDING|IS_TERM|IS_CANCEL_REQ;
    private static final int LEFT_CAN_BIND      = RIGHT_STARVED;
    private static final int RIGHT_CANNOT_BIND  = RIGHT_BINDING|IS_TERM|IS_CANCEL_REQ;
    private static final int FILTER_BIND        = 0x00010000;
    private static final int REBIND_FAILED      = 0x00020000;
    private static final Flags BINDING_STAGE_FLAGS =
            Flags.DEFAULT.toBuilder()
                    .flag(LEFT_CANCELLED,  "LEFT_CANCELLED" )
                    .flag(LEFT_COMPLETED,  "LEFT_COMPLETED" )
                    .flag(LEFT_FAILED,     "LEFT_FAILED"    )
                    .flag(RIGHT_CANCELLED, "RIGHT_CANCELLED")
                    .flag(RIGHT_FAILED,    "RIGHT_FAILED"   )
                    .flag(RIGHT_BINDING,   "RIGHT_BINDING"  )
                    .flag(RIGHT_STARVED,   "RIGHT_STARVED"  )
                    .flag(FILTER_BIND,     "FILTER_BIND"    )
                    .flag(REBIND_FAILED,   "REBIND_FAILED"  )
                    .build();

    private static final byte PASSTHROUGH_NOT   = 0;
    private static final byte PASSTHROUGH_LEFT  = 1;
    private static final byte PASSTHROUGH_RIGHT = 2;

    protected final BatchType<B> batchType;
    private final Emitter<B, ?> leftUpstream;
    private final RightReceiver rightRecv;
    private @Nullable B lb;
    private short lr = -1;
    private final short leftChunk;
    private @Nullable TermTask termTask;
    private int lbTotalRows;
    private int leftPending;
    private long requested;
    private BatchBinding intBinding, nextIntBinding;
    protected int lastRebindSeq = -1;
    private Vars extBindingVars = Vars.EMPTY;
    protected final Vars outVars;
    protected final Vars bindableVars;
    private Throwable error = UNSET_ERROR;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public static <B extends Batch<B>> Orphan<? extends BindingStage<B, ?>>
    create(EmitBindQuery<B> bq, Vars rebindHint, boolean weakDedup, @Nullable Vars projection) {
        return new Concrete<>(bq.bindings, bq.type, bq, projection == null ? bq.resultVars() : projection,
              bq.parsedQuery().emit(
                      bq.batchType(),
                      bq.bindingsVars().union(rebindHint),
                      weakDedup));
    }

    public static <B extends Batch<B>> Orphan<? extends BindingStage<B, ?>>
    create(EmitBindQuery<B> bq, Vars outerRebindHint, SparqlClient client) {
        return new Concrete<>(bq.bindings, bq.type, bq, bq.resultVars(),
                client.emit(bq.batchType(), bq.query, bq.bindingsVars().union(outerRebindHint)));
    }

    private static final class Concrete<B extends Batch<B>, S extends BindingStage<B, S>>
            extends BindingStage<B, S>
            implements Orphan<S> {
        public Concrete(Orphan<? extends Emitter<B, ?>> bindings, BindType type,
                        @Nullable BindListener listener, Vars outVars,
                        Orphan<? extends Emitter<B, ?>> rightUpstream) {
            super(bindings, type, listener, outVars, rightUpstream);
        }
        @Override public S takeOwnership(Object o) {return takeOwnership0(o);}
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
    protected BindingStage(Orphan<? extends Emitter<B, ?>> bindings, BindType type,
                           @Nullable BindListener listener,
                           Vars outVars, Orphan<? extends Emitter<B, ?>> rightUpstream) {
        super(CREATED|RIGHT_STARVED|(type.isJoin() ? 0 : FILTER_BIND),
              BINDING_STAGE_FLAGS);
        leftUpstream = bindings.takeOwnership(this);
        batchType = leftUpstream.batchType();
        bindableVars = leftUpstream.bindableVars().union(Emitter.bindableVars(rightUpstream));
        this.outVars = outVars;
        Vars lVars = leftUpstream.vars(), rVars = Emitter.peekVars(rightUpstream);
        Orphan<? extends BatchMerger<B, ?>> merger;
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
            }
        } else {
            merger = null;
        }
        this.rightRecv = new RightReceiver(merger, passthrough, outVars.size(), type, rightUpstream, listener);
        this.leftChunk = (short)Math.max(8, leftUpstream.preferredRequestChunk()/4);
        this.intBinding     = new BatchBinding(lVars);
        this.nextIntBinding = new BatchBinding(lVars);
        leftUpstream.subscribe(this);
        rightRecv.upstream.subscribe(rightRecv);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    @Override protected void doRelease() {
        if (EmitterStats.LOG_ENABLED && stats != null)
            stats.report(log, this);
        Owned.safeRecycle(leftUpstream, this);
        Owned.safeRecycle(rightRecv.merger, rightRecv);
        Owned.safeRecycle(termTask, this);
        rightRecv.upstream.recycle(rightRecv);
        // if this happens before the first startNextBinding(), right upstream will be
        // in CREATED state and the above recycle() will not cause an actual release
        rightRecv.upstream.cancel();
        lr = -1;
        lb = Batch.safeRecycle(lb, this);
        lbTotalRows = 0;
        if (intBinding != null) {
            intBinding.remainder = null;
            intBinding.attach(null, 0);
        }
        if (nextIntBinding != null) {
            nextIntBinding.remainder = null;
            nextIntBinding.attach(null, 0);
        }
        super.doRelease();
    }

    @Override protected void onPendingRelease() {
        super.onPendingRelease();
        cancel();
    }

    @Override public Vars         vars()      { return outVars;   }
    @Override public BatchType<B> batchType() { return batchType; }
    @Override public String       toString()  { return label(MINIMAL); }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(64), this);
        if (type != MINIMAL)
            sb.append('[').append(rightRecv.type.name()).append("] vars=").append(outVars);
        if (type.showState())
            appendState(sb);
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    private void appendState(StringBuilder sb) {
        appendRequested(sb.append("\nrequested=" ), requested);
        appendRequested(sb.append(" leftPending="), leftPending);
        B lb = this.lb;
        int lQueued = (lb == null ? 0 : lbTotalRows -lr);
        appendRequested(sb.append(" leftQueued="), lQueued);
        sb.append(" state=").append(flags.render(state()));
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.concat(Stream.of(leftUpstream), Stream.ofNullable(rightRecv.upstream));
    }

    /* --- --- --- Receiver side (bindings) --- --- --- */

    @SuppressWarnings("unchecked") @Override
    public @This S subscribeTo(Orphan<? extends Emitter<B, ?>> upstream) {
        if (upstream != this.leftUpstream)
            throw new MultipleRegistrationUnsupportedException(this);
        return (S)this;
    }

    @Override public @MonotonicNonNull Emitter<B, ?> upstream() { return leftUpstream; }

    @Override public @Nullable Orphan<B> pollFillingBatch() {
        Orphan<B> tail;
        B stale = lb; // returning null is cheaper than synchronization
        if (stale == null || stale.next == null || !tryLock())
            return null; //no queue, no tail or contended lock
        try {
            if ((tail=detachDistinctTail(lb)) != null) {
                lbTotalRows -= Batch.peekTotalRows(tail);
                if (EmitterStats.ENABLED && stats != null)
                    stats.revertOnBatchReceived(tail);
            }
        } finally { unlock(); }
        return tail;
    }

    @Override public void onBatch(Orphan<B> orphan) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(orphan);
        int rows = Batch.peekTotalRows(orphan);
        if (rows == 0)
            return;
        journal("onBatch, rows=", rows, "st=", statePlain(), flags, "on", this);

        int state = lock();
        try {
            if ((state&IS_CANCEL_REQ) != 0) {
                orphan.takeOwnership(this).recycle(this);
            } else {
                lb = Batch.quickAppendTrusted(lb, this, orphan);
                lbTotalRows += rows;
                leftPending -= rows;
                if ((state&LEFT_CAN_BIND_MASK) == LEFT_CAN_BIND)
                    state = startNextBinding(state);
            }
        } finally {
            if ((state&LOCKED_MASK) != 0) unlock();
        }
    }

    @Override public void onBatchByCopy(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        int rows = batch == null ? 0 : batch.totalRows();
        if (rows == 0)
            return;
        journal("onBatchByCopy, rows=", rows, "st=", statePlain(), flags, "on", this);

        int state = lock();
        try {
            if ((state&IS_CANCEL_REQ) == 0) {
                B head = lb, tail = detachDistinctTail(head, this);
                state = unlock(); // copy/append() while unlocked
                if (tail == null)
                    tail = batchType.create(batch.cols).takeOwnership(this);
                tail.copy(batch);
                Orphan<B> tailOrphan = tail.releaseOwnership(this);
                state = lock();
                lb = Batch.quickAppend(lb, this, tailOrphan);
                lbTotalRows += rows;
                leftPending -= rows;
                if ((state&LEFT_CAN_BIND_MASK) == LEFT_CAN_BIND)
                    state = startNextBinding(state);
            }
        } finally {
            if ((state&LOCKED_MASK) != 0) unlock();
        }
    }

    @Override public void onError(Throwable cause) { leftTerminated(LEFT_FAILED,    cause); }
    @Override public void onComplete()             { leftTerminated(LEFT_COMPLETED, null); }
    @Override public void onCancelled() {
        leftTerminated((statePlain()&REBIND_FAILED)==0 ? LEFT_CANCELLED : LEFT_FAILED, null);
    }

    /* --- --- --- Emitter side of the Stage --- --- --- */

    @Override public Vars bindableVars() { return bindableVars; }

    @Override public void rebindPrefetch(BatchBinding binding) {
        leftUpstream.rebindPrefetch(binding);
    }

    @Override public void rebindPrefetchEnd() {
        leftUpstream.rebindPrefetchEnd();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (lastRebindSeq == binding.sequence)
            return; // duplicate rebind() due to diamond in processing graph
        lastRebindSeq = binding.sequence;
        resetForRebind(LEFT_TERM|RIGHT_TERM, RIGHT_STARVED|LOCKED_MASK);
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            leftUpstream.rebind(binding);
            requested   = 0;
            leftPending = 0;
            dropLeftQueued();
            if (extBindingVars.equals(binding.vars)) {
                intBinding.remainder     = binding;
                nextIntBinding.remainder = binding;
            } else {
                updateExtRebindVars(binding);
            }

            if (ResultJournal.ENABLED)
                ResultJournal.rebindEmitter(this, binding);
            unlock();
        } catch (Throwable t) {
            this.error = t;
            journal("rebind failed for ", this, t.toString());
            moveStateRelease(unlock(), REBIND_FAILED);
        }
    }

    private void dropLeftQueued() {
        rightRecv.upstream.rebindPrefetchEnd();
        lr          = -1;
        lb          = Batch.recycle(lb, this);
        lbTotalRows = 0;
    }

    protected Vars          intBindingVars() { return intBinding.vars;    }
    protected Emitter<B, ?>  rightUpstream() { return rightRecv.upstream; }

    protected void updateExtRebindVars(BatchBinding binding) {
        var bindingVars = binding.vars;
        if (ENABLED)
            journal("updateExtRebindVars on", this, "to", bindingVars);
        extBindingVars = bindingVars;
        Vars union = leftUpstream.vars().union(bindingVars);
        intBinding    .vars(union);
        nextIntBinding.vars(union);
        intBinding    .remainder = binding;
        nextIntBinding.remainder = binding;

//        Vars bindingVars = binding.vars;
//        extBindingVars = bindingVars;
//        Vars leftVars = leftUpstream.vars();
//        Vars rightVars = rightRecv.upstream.bindableVars();
//        Vars mergedVars = null;
//        for (int i = 0, n = bindingVars.size(); i < n; i++) {
//            var v = bindingVars.get(i);
//            if (!leftVars.contains(v) && rightVars.contains(v)) {
//                if (mergedVars == null)
//                    mergedVars = Vars.fromSet(leftVars, leftVars.size() + bindingVars.size());
//                mergedVars.add(v);
//            }
//        }
//        if (mergedVars == null) {
//            intBinding.vars(leftVars);
//            leftMerger = null;
//            extBindingForLeftMerger = batchType.recycle(extBindingForLeftMerger);
//        } else {
//            leftMerger = batchType.merger(mergedVars, bindingVars, leftVars);
//            intBinding.vars(mergedVars);
//            extBindingForLeftMerger = batchType.empty(extBindingForLeftMerger, bindingVars.size());
//        }
    }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        if (rightRecv.downstream != null && rightRecv.downstream != receiver)
            throw new MultipleRegistrationUnsupportedException(this);
        rightRecv.downstream = receiver;
        //noinspection unchecked
        rightRecv.downstreamHFB = receiver instanceof HasFillingBatch<?> hfb
                                ? (HasFillingBatch<B>)hfb : null;
        if (ENABLED)
            journal("subscribed", receiver, "to", this);
    }

    @Override public boolean   isComplete() {return Stateful.isCompleted(state());}
    @Override public boolean  isCancelled() {return Stateful.isCancelled(state());}
    @Override public boolean     isFailed() {return Stateful   .isFailed(state());}
    @Override public boolean isTerminated() {return (state()&IS_TERM) != 0;}

    @Override public boolean cancel() {
        journal("cancel()", this);
        int st = lock();
        boolean cancelLeft = false, cancelRight = false, terminateStage = false, hasEffect;
        try {
            hasEffect = (st&(IS_CANCEL_REQ|IS_TERM)) == 0;
            if (hasEffect) {
                // change state NOW so clearFlagsRelease(RIGHT_BINDING) can observe IS_CANCEL_REQ
                st = changeFlagsRelease(STATE_MASK, CANCEL_REQUESTED);
                // cancel() on terminated is a waste of time. cancel() concurrent with
                // rebind() can corrupt right upstream state
                cancelLeft  = (st&LEFT_TERM) == 0;
                cancelRight = (st&(RIGHT_STARVED|RIGHT_BINDING)) == 0;
                if (lb != null && (st&(RIGHT_STARVED|RIGHT_BINDING)) == RIGHT_STARVED) {
                    dropLeftQueued();
                    if (!cancelLeft) {
                        // left and right are terminated or not started, thus cannot receive
                        // a cancel(). this state holds between the first this.onBatch() call and
                        // the RebindTask.task() scheduled at onBatch(). RebindTask will bail
                        // due to IS_CANCEL_REQ and there will be no termination from the right
                        // side, which is starved. Calling terminateStage() directly from this
                        // stack frame can lead to deadlocks, since cancel() callers are not
                        // expecting to receive calls from upstream during a cancel() call.
                        terminateStage = true;
                    }
                }
            }
        } finally {
            if ((st&LOCKED_MASK) != 0) // cancelFutureBindings()->terminateStage() may unlock()
                unlock();
        }
        if (terminateStage) {
            if (termTask == null)
                termTask = new TermTask.Concrete(this).takeOwnership(this);
            termTask.schedule();
        }
        if (cancelLeft)
            leftUpstream.cancel();
        if (cancelRight)
            rightRecv.upstream.cancel();
        return hasEffect;
    }

    @Override public void request(long rows) {
        if (ENABLED)
            journal("request ", rows, "on", this);
        int state = statePlain(), leftRequest = 0;
        long rightRows = (state&FILTER_BIND) == 0 ? rows : 2;
        if ((state&IS_INIT) != 0) {
            if ((onFirstRequest(state)&IS_LIVE) == 0)
                return;
        }

        state = lock();
        try {
            if (rows > requested) {
                requested = rows;
                leftRequest = leftRequest(lb);
                if ((state&CAN_REQ_RIGHT_MASK) == CAN_REQ_RIGHT)
                    rightRecv.upstream.request(rightRows);
            }
        } finally { state = unlock(); }
        if (leftRequest > 0 && (state&IS_TERM) == 0)
            leftUpstream.request(leftRequest);
    }

    /* --- --- --- internal helpers --- --- --- */

    private int onFirstRequest(int state) {
        if (!moveStateRelease(state, ACTIVE))
            return state;
        state = (state&FLAGS_MASK)|ACTIVE;
        if ((state&REBIND_FAILED) != 0) {
            state = lock();
            try {
                boolean terminate = true;
                if ((state&LEFT_TERM) == 0) {
                    terminate = false;
                    leftUpstream.cancel();
                }
                if ((state&(RIGHT_TERM|RIGHT_STARVED)) == 0) {
                    terminate = false;
                    rightRecv.upstream.cancel();
                }
                if (terminate) {
                    state = setFlagsRelease(LEFT_FAILED|RIGHT_FAILED);
                    state = terminateStage(state);
                }
            } finally {
                if ((state&LOCKED_MASK) != 0) state = unlock();
            }
        }
        return state;
    }

    private int leftRequest(B lb) {
        // count queued left rows into LEFT_REQUESTED_MAX limit
        int queued = (lb == null ? 0 : lbTotalRows-lr);
        if (requested > Math.max(0, leftPending)+queued && leftPending < leftChunk>>1) {
            int n = (int)Math.min(requested, leftChunk);
            leftPending = n;
            return n;
        }
        return 0;
    }

    private void leftTerminated(int flag, @Nullable Throwable error) {
        int st = lock();
        try {
            if (ENABLED)
                journal("leftTerminated st=", st, flags, "flag=", flag, flags, "bStage=", this);
            if (error != null && this.error == UNSET_ERROR)
                this.error = error;
            if ((st&LEFT_TERM) == 0) {
                st = setFlagsRelease(flag);
                boolean canTerminate = (st&RIGHT_TERM) != 0
                        || ((st&RIGHT_STARVED) != 0 && (lb == null || (st&IS_CANCEL_REQ) != 0));
                if (canTerminate)
                    st = terminateStage(st);
            }
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock();
        }
    }

    private void rightFailedOrCancelled(int flag, @Nullable Throwable error) {
        int state = lock();
        try {
            journal("rightFailedOrCancelled", state, flags, "flag=", flag, flags,
                    "on", this);
            if ((state&RELEASED_MASK) == 0) {
                if (error != null && this.error == UNSET_ERROR) {
                    if (ENABLED) journal("error=", error);
                    this.error = error;
                }
                if ((state&RIGHT_TERM) == 0) {
                    dropLeftQueued();
                    state = setFlagsRelease(flag);
                    if ((state&LEFT_TERM) == 0) leftUpstream.cancel(); // if right failed, cancel
                    else                        state = terminateStage(state);
                }
            } // likely caused by cancel() from within doRelease()
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock();
        }
    }

    private int terminateStage(int state) {
        int termState;
        if ((state & (LEFT_FAILED|RIGHT_FAILED)) != 0) {
            termState = FAILED;
        } else if ((state & (LEFT_CANCELLED|RIGHT_CANCELLED)) != 0) {
            termState = CANCELLED;
        } else {
            assert (state & (LEFT_TERM|RIGHT_TERM)) == LEFT_COMPLETED;
            termState = COMPLETED;
        }
        dropLeftQueued();
        var downstream = rightRecv.downstream;
        state = unlock();
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
        } else {
            assert (statePlain()&CANCELLED) != 0
                    : "attempt to terminate after non-cancel previous termination";
        }
        return statePlain()&~LOCKED_MASK;
    }

    /* --- --- --- right-side processing --- --- --- */

    private @Nullable B advanceLB(B lb) {
        rightRecv.upstream.rebindPrefetchEnd();
        nextIntBinding.sequence = intBinding.sequence; // sync bindings sequence number
        // swap intBinding and nextIntBinding
        var oldIntBinding   = this.intBinding;
        this.intBinding     = this.nextIntBinding;
        this.nextIntBinding = oldIntBinding;
        this.lbTotalRows   -= lb.rows;
        this.lb             = lb = lb.dropHead(this);
        return lb;
    }

    private int cancelFutureBindings() {
        if (ENABLED) journal("cancelFutureBindings(), bs=", this);
        dropLeftQueued();
        int st = setFlagsRelease(RIGHT_STARVED);
        if ((st&LEFT_TERM) == 0) leftUpstream.cancel();
        else                     st = terminateStage(st);
        return st;
    }

    private int startNextBinding(int st) {
        assert (st&(LOCKED_MASK|RIGHT_BINDING|IS_TERM|IS_CANCEL_REQ)) == LOCKED_MASK : "bad state";
        B lb = this.lb;
        short lr = (short)(this.lr+1);
        try {
            if (lb == null)
                lr = -1;
            else if (lr >= lb.rows || lr < 0)
                lr = (lb = advanceLB(lb)) == null ? (short)-1 : 0;
            // else: lb != null && lr < lb.rows
            this.lr = lr;
            if (ENABLED)
                journal("startNextBinding st=", st, flags, "lr=", lr, "on", this);
            int leftRequest = leftRequest(lb);
            if (lb == null) {
                if ((st&LEFT_TERM) == 0) {
                    st = unlock(0, RIGHT_STARVED);
                    if (leftRequest > 0) leftUpstream.request(leftRequest);
                } else {
                    st = terminateStage(st);
                }
            } else {
                // release lock but forbid onBatch() from calling startNextBinding()
                // and forbid request() from calling upstream.request()
                st = unlock(RIGHT_STARVED, RIGHT_BINDING);
                if (leftRequest > 0 && (st&IS_TERM) == 0)
                    leftUpstream.request(leftRequest);
                ++intBinding.sequence;
                rebind(intBinding.attach(lb, lr), rightRecv.upstream);
                ++rightRecv.bindingSeq;
                rightRecv.upstreamEmpty = true;
                rightRecv.listenerNotified = false;
                st = clearFlagsRelease(RIGHT_BINDING)&~LOCKED_MASK; // rebind complete, allow upstream.request()
                if ((st&IS_CANCEL_REQ) != 0)
                    rightRecv.upstream.cancel();
                else
                    rightRecv.upstream.request((st&FILTER_BIND) == 0 ? requested : 2);
            }
        } catch (Throwable t) {
            log.error("{}.startNextBinding() failed after seq={}", this, rightRecv.bindingSeq, t);
            if ((st&LOCKED_MASK) != 0)
                st = unlock(); // avoid self-deadlock on rightTerminated/lock()
            rightFailedOrCancelled(RIGHT_FAILED, t);
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
    protected boolean lexContinueRight(BatchBinding binding, Emitter<B, ?> emitter) {
        return false;
    }

    /**
     * By default, equivalent to {@code rightEmitter.rebind(binding)}. This may be overridden
     * to implement non-standard joining, e.g., lexical joining.
     *
     * @param binding a {@link BatchBinding} to be used with {@link Emitter#rebind(BatchBinding)}
     * @param rightEmitter the right-side emitter
     */
    protected void rebind(BatchBinding binding, Emitter<B, ?> rightEmitter) {
        if (binding.row == 0)
            rightEmitter.rebindPrefetch(binding);
        rightEmitter.rebind(binding);
    }

    /**
     * An async task that {@link BindingStage} schedules when something must be done but cannot
     * it cannot be done at the current call stack, either because it would cause a deadlock or
     * because it would introduce latency.
     */
    private static abstract sealed class TermTask extends EmitterService.Task<TermTask> {
        private final BindingStage<?, ?> bs;
        private TermTask(BindingStage<?, ?> bs) {
            super(CREATED, TASK_FLAGS);
            this.bs = bs;
        }
        private static final class Concrete extends TermTask implements Orphan<TermTask> {
            private Concrete(BindingStage<?, ?> bs) {super(bs);}
            @Override public TermTask takeOwnership(Object o) {return takeOwnership0(o);}
        }

        void schedule() { awakeSameWorker(); }

        @Override protected void task(EmitterService.Worker worker, int threadId) {
            int st = bs.lock();
            try {
                if ((st & (LEFT_TERM | RIGHT_BINDING)) == LEFT_TERM
                        && (st & (RIGHT_STARVED | RIGHT_TERM)) != 0
                        && (bs.lb == null || (st & IS_CANCEL_REQ) != 0))
                    st = bs.terminateStage(st);
            } finally {
                if ((st & LOCKED_MASK) != 0) unlock();
            }
        }
    }

    private final class RightReceiver implements Receiver<B>, JournalNamed {
        private @MonotonicNonNull Receiver<B> downstream;
        private @Nullable HasFillingBatch<B> downstreamHFB;
        private final Emitter<B, ?> upstream;
        private final @Nullable BatchMerger<B, ?> merger;
        private final BindType type;
        private final @Nullable BindListener bindingListener;
        private final byte passThrough;
        private boolean upstreamEmpty, listenerNotified;
        private final int outCols;
        private final BatchType<B> bt;
        private long bindingSeq = -1;

        public RightReceiver(@Nullable Orphan<? extends BatchMerger<B, ?>> merger,
                             byte passThrough, int outCols,
                             BindType type, Orphan<? extends Emitter<B, ?>> rightUpstream,
                             @Nullable BindListener bindingListener) {
            this.upstream        = rightUpstream.takeOwnership(this);
            this.bt              = upstream.batchType();
            this.merger          = merger == null ? null : merger.takeOwnership(this);
            this.passThrough     = passThrough;
            this.outCols         = outCols;
            this.type            = type;
            this.bindingListener = bindingListener;
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {return Stream.of(upstream);}

        @Override public String    toString() {return label(MINIMAL);}
        @Override public String journalName() {return BindingStage.this.journalName()+".R";}

        @Override public String label(StreamNodeDOT.Label type) {
            var sb = StreamNodeDOT.minimalLabel(new StringBuilder(), BindingStage.this);
            sb.append(".right@").append(Integer.toHexString(identityHashCode(this)));
            if (type.showState())
                appendState(sb);
            if (type.showStats() && stats != null)
                stats.appendToLabel(sb);
            return sb.toString();
        }


        public void onBatch0(B rb, boolean copy) {
            if (rb == null)
                return;
            if (rb.rows == 0 || (statePlain()&IS_CANCEL_REQ) != 0) {
                if (!copy) rb.recycle(this);
                return;
            }
            upstreamEmpty = false;
            switch (type) {
                case JOIN,LEFT_JOIN,EXISTS -> merge(rb, copy);
                case NOT_EXISTS,MINUS      -> {
                    if (!listenerNotified && bindingListener != null) {
                        bindingListener.emptyBinding(bindingSeq);
                        listenerNotified = true;
                    }
                    upstream.cancel();
                    if (!copy)
                        rb.recycle(this);
                }
            }
        }

        @Override public void onBatch(Orphan<B> batch) {
            onBatch0(batch.takeOwnership(this), false);
        }
        @Override public void onBatchByCopy(B batch) {
            onBatch0(batch,  true);
        }

        private void merge(@Nullable B rb, boolean copy) {
            assert lb != null;
            int rowsProduced;
            B ownedRB = copy ? null : rb, b = null;
            try {
                if (merger != null) {
                    b = merger.merge(null, lb, lr, rb).takeOwnership(this);
                    rowsProduced = b.totalRows();
                } else if (passThrough == PASSTHROUGH_RIGHT && rb != null) {
                    b = ownedRB;
                    ownedRB = null;
                    rowsProduced = rb.totalRows();
                } else {
                    var offer = downstreamHFB == null ? null : downstreamHFB.pollFillingBatch();
                    b = (offer == null ? bt.create(outCols) : offer).takeOwnership(this);
                    if (passThrough == PASSTHROUGH_RIGHT) {
                        b.beginPut();
                        b.commitPut();
                    } else {
                        b.putRow(lb, lr);
                    }
                    rowsProduced = 1;
                }

                // notify binding is not empty
                if (!listenerNotified && bindingListener != null) {
                    bindingListener.nonEmptyBinding(bindingSeq);
                    listenerNotified = true;
                }

                B statsB = EmitterStats.ENABLED || ResultJournal.ENABLED
                        ? Objects.requireNonNullElse(b, rb) : null;
                // update stats and requested
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchDelivered(statsB);
                lock();
                requested -= rowsProduced;
                unlock();

                // deliver batch/row
                if (ResultJournal.ENABLED)
                    ResultJournal.logBatch(BindingStage.this, statsB);
                Orphan<B> orphan = b.releaseOwnership(this);
                b = null;
                if (orphan != null)
                    downstream.onBatch(orphan);
                else
                    downstream.onBatchByCopy(rb);
            } catch (Throwable t) {
                handleEmitError(downstream, BindingStage.this, t, null);
            } finally {
                Batch.recycle(b, this);
                Batch.recycle(ownedRB, this);
            }
        }


        @Override public void onComplete() {
            if (ENABLED) journal("right.onComplete() on ", this);
            int lockedSt = 0;
            try {
                assert lb != null;
                if (lexContinueRight(intBinding, upstream)) {
                    if (ENABLED)
                        journal("lexContinueRight, bStage=", this);
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
                            case LEFT_JOIN,NOT_EXISTS,MINUS -> merge(null, false);
                        }
                    }
                    lockedSt = lock();
                    if ((lockedSt&RIGHT_CANNOT_BIND)==0) {
                        lockedSt = startNextBinding(lockedSt);
                    } else if ((lockedSt&IS_CANCEL_REQ) != 0) {
                        lockedSt = cancelFutureBindings();
                    } else if (ENABLED) {
                        journal("r completed, but cannot startNextBinding, st=", lockedSt, flags, "on", this);
                    }
                }
                if ((lockedSt&LOCKED_MASK) != 0)
                    unlock();
            } catch (Throwable t) {
                log.error("onComplete() failed", t);
                if ((lockedSt&LOCKED_MASK&state()) != 0)
                     unlock();
                rightFailedOrCancelled(RIGHT_FAILED, t);
            }
        }

        @Override public void onCancelled() {
            if ((stateAcquire()&RELEASED_MASK) == 0) {
                if (type.isJoin()) rightFailedOrCancelled(RIGHT_CANCELLED, null);
                else               onComplete();
            }
            // doRelease will call cancel() to cause the right upstream tree to release itself
            // in case no startNextBinding() happened before doRelease(). In such scenario the
            // right upstream will be in CREATED state and rebindRelease() will not trigger
            // a release, thus a cancel() is necessary to cause it to terminate, then release.
        }

        @Override public void onError(Throwable cause) {
            rightFailedOrCancelled(RIGHT_FAILED, cause);
        }
    }
}
