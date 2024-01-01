package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.FSProperties;
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
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;
import static com.github.alexishuf.fastersparql.emit.async.EmitterService.currentWorker;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.Label.MINIMAL;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;

public class BindingStage<B extends Batch<B>> extends Stateful implements Stage<B, B> {
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
    private static final int CAN_BIND_MASK      = RIGHT_STARVED|RIGHT_BINDING|IS_TERM|IS_CANCEL_REQ;
    private static final int LEFT_CAN_BIND      = RIGHT_STARVED;
    private static final int RIGHT_CAN_BIND     = 0;
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
    private final Emitter<B> leftUpstream;
    private final RightReceiver rightRecv;
    private final RebindTask rebindTask;
    private @Nullable B fillingLB;
    private @Nullable B lb;
    private short lr = -1;
    private final short leftChunk;
    private int leftPending;
    private long requested;
    private BatchBinding intBinding, nextIntBinding;
    protected int lastRebindSeq = -1;
    private Vars extBindingVars = Vars.EMPTY;
    protected final Vars outVars;
    protected final Vars bindableVars;
    private Throwable error = UNSET_ERROR;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public BindingStage(EmitBindQuery<B> bq, Vars rebindHint, boolean weakDedup,
                       @Nullable Vars projection) {
        this(bq.bindings, bq.type, bq, projection == null ? bq.resultVars() : projection,
              bq.parsedQuery().emit(
                      bq.bindings.batchType(),
                      bq.bindings.vars().union(rebindHint),
                      weakDedup));
    }

    public BindingStage(EmitBindQuery<B> bq, Vars outerRebindHint, SparqlClient client) {
        this(bq.bindings, bq.type, bq, bq.resultVars(),
                client.emit(bq.batchType(), bq.query, bq.bindings.vars().union(outerRebindHint)));
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
    protected BindingStage(Emitter<B> bindings, BindType type, @Nullable BindListener listener,
                        Vars outVars, Emitter<B> rightUpstream) {
        super(CREATED|RIGHT_STARVED|(type.isJoin() ? 0 : FILTER_BIND),
              BINDING_STAGE_FLAGS);
        rebindTask = new RebindTask(this);
        batchType = bindings.batchType();
        bindableVars = bindings.bindableVars().union(rightUpstream.bindableVars());
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
        this.rightRecv = new RightReceiver(merger, passthrough, outVars.size(), type, rightUpstream, listener);
        this.leftUpstream = bindings;
        int safeCols = Math.max(1, bindings.vars().size());
        int b = Math.max(2, FSProperties.emitReqChunkBatches()/4);
        this.leftChunk = (short)Math.max(8, b*batchType.preferredTermsPerBatch()/safeCols);
        this.intBinding     = new BatchBinding(lVars);
        this.nextIntBinding = new BatchBinding(lVars);
        bindings.subscribe(this);
        rightUpstream.subscribe(rightRecv);
        if (ResultJournal.ENABLED)
            ResultJournal.initEmitter(this, outVars);
    }

    @Override public Vars         vars()      { return outVars;   }
    @Override public BatchType<B> batchType() { return batchType; }

    @Override public String toString() {
        return label(MINIMAL).replace("\n", " ");
    }

    @Override public String label(StreamNodeDOT.Label type) {
        var sb = StreamNodeDOT.minimalLabel(new StringBuilder(64), this);
        if (type != MINIMAL)
            sb.append('[').append(rightRecv.type.name()).append("] vars=").append(outVars);
        if (type.showState()) {
            appendRequested(sb.append("\nrequested=" ), requested);
            appendRequested(sb.append(" leftPending="), leftPending);
            B lb = this.lb, fillingLB = this.fillingLB;
            int lQueued = (lb        == null ? 0 :        lb.totalRows() - lr)
                        + (fillingLB == null ? 0 : fillingLB.totalRows());
            appendRequested(sb.append(" leftQueued="), lQueued);
            sb.append(" state=").append(flags.render(state()));
        }
        if (type.showStats() && stats != null)
            stats.appendToLabel(sb);
        return sb.toString();
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        return Stream.concat(Stream.of(leftUpstream), Stream.ofNullable(rightRecv.upstream));
    }

    @Override protected void doRelease() {
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
        rightRecv.rebindRelease();
        lb                   = batchType.recycle(lb);
        fillingLB            = batchType.recycle(fillingLB);
        rightRecv.singleton  = batchType.recycle(rightRecv.singleton);
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

    /* --- --- --- Receiver side (bindings) --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> upstream) {
        if (upstream != this.leftUpstream)
            throw new MultipleRegistrationUnsupportedException(this);
        return this;
    }

    @Override public @MonotonicNonNull Emitter<B> upstream() { return leftUpstream; }

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        int rows = batch == null ? 0 : batch.totalRows();
        if (rows == 0)
            return batch;
        if (ENABLED)
            journal("onBatch, rows=", rows, "st=", statePlain(), flags, "on", this);

        B retained = null;
        int state = lock(statePlain());
        try {
            if (lb != null && fillingLB != null) {
                retained = batch;
                batch = fillingLB;
                fillingLB = null;
                state = unlock(state);
                batch.copy(retained);
                state = lock(state);
            }
            if      (       lb == null)        lb = batch;
            else if (fillingLB == null) fillingLB = batch;
            else                        throw new IllegalStateException("fillingLB != null");
            leftPending -= rows;
            if ((state &CAN_BIND_MASK) == LEFT_CAN_BIND)
                rebindTask.schedule();
        } finally {
            if ((state&LOCKED_MASK) != 0) unlock(state);
        }
        return retained;
    }

    @Override public void onError(Throwable cause) { leftTerminated(LEFT_FAILED,    cause); }
    @Override public void onComplete()             { leftTerminated(LEFT_COMPLETED, null); }
    @Override public void onCancelled() {
        leftTerminated((statePlain()&REBIND_FAILED)==0 ? LEFT_CANCELLED : LEFT_FAILED, null);
    }

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
        int st = resetForRebind(LEFT_TERM|RIGHT_TERM, RIGHT_STARVED|LOCKED_MASK);
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
            unlock(st);
        } catch (Throwable t) {
            this.error = t;
            journal("rebind failed for ", this, t.toString());
            moveStateRelease(unlock(st), REBIND_FAILED);
        }
    }

    private void dropLeftQueued() {
        rightRecv.upstream.rebindPrefetchEnd();
        lr        = -1;
        lb        = batchType.recycle(lb);
        fillingLB = batchType.recycle(fillingLB);
    }

    protected Vars       intBindingVars() { return intBinding.vars;    }
    protected Emitter<B>  rightUpstream() { return rightRecv.upstream; }

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
        if (ENABLED)
            journal("subscribed", receiver, "to", this);
    }

    @Override public void cancel() {
        if (ENABLED) journal("cancel()", this);
        int st = lock(statePlain()), next = st&STATE_MASK;
        try {
            if ((st&(IS_CANCEL_REQ|IS_TERM)) == 0) {
                next = CANCEL_REQUESTED;
                leftUpstream.cancel();
                rightRecv.upstream.cancel();
            }
        } finally {
            unlock(st, STATE_MASK, next);
        }
    }

    @Override public void request(long rows) {
        if (ENABLED)
            journal("request ", rows, "on", this);
        int state = statePlain();
        long rightRows = (state&FILTER_BIND) == 0 ? rows : 2;
        if ((state&IS_INIT) != 0) {
            if (((state = onFirstRequest(state))&IS_LIVE) == 0)
                return;
        }

        state = lock(state);
        try {
            if (rows > requested) {
                requested = rows;
                maybeRequestLeft();
                if ((state&CAN_REQ_RIGHT_MASK) == CAN_REQ_RIGHT)
                    rightRecv.upstream.request(rightRows);
            }
        } finally {
            unlock(state);
        }
    }

    /* --- --- --- internal helpers --- --- --- */

    private int onFirstRequest(int state) {
        if (!moveStateRelease(state, ACTIVE))
            return state;
        state = (state&FLAGS_MASK)|ACTIVE;
        if ((state&REBIND_FAILED) != 0) {
            state = lock(state);
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
                    state = setFlagsRelease(state, LEFT_FAILED|RIGHT_FAILED);
                    state = terminateStage(state);
                }
            } finally {
                if ((state&LOCKED_MASK) != 0) state = unlock(state);
            }
        }
        return state;
    }

    private void maybeRequestLeft() {
        B lb = this.lb, fillingLB = this.fillingLB;
        // count queued left rows into LEFT_REQUESTED_MAX limit
        int queued = (       lb == null ? 0 :        lb.totalRows()-lr)
                   + (fillingLB == null ? 0 : fillingLB.totalRows());
        if (requested > Math.max(0, leftPending)+queued && leftPending < leftChunk>>1) {
            int n = (int)Math.min(requested, leftChunk);
            leftPending = n;
            leftUpstream.request(n);
        }
    }

    private void leftTerminated(int flag, @Nullable Throwable error) {
        int st = lock(statePlain());
        try {
            if (ENABLED)
                journal("leftTerminated st=", st, flags, "flag=", flag, flags, "bStage=", this);
            if (error != null && this.error == UNSET_ERROR)
                this.error = error;
            if ((st&LEFT_TERM) == 0) {
                st = setFlagsRelease(st, flag);
                if ((st&(RIGHT_TERM|RIGHT_STARVED)) != 0 && (lb==null || (st&IS_CANCEL_REQ) != 0))
                    st = terminateStage(st);
            }
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock(st);
        }
    }

    private void rightFailedOrCancelled(int flag, @Nullable Throwable error) {
        int state = lock(statePlain());
        try {
            if (ENABLED)
                journal("rightFailedOrCancelled", state, flags, "flag=", flag, flags, "on", this);
            if (error != null && this.error == UNSET_ERROR) {
                if (ENABLED) journal("error=", error);
                this.error = error;
            }
            if ((state&RIGHT_TERM) == 0) {
                state = setFlagsRelease(state, flag);
                if ((state&LEFT_TERM) == 0) leftUpstream.cancel(); // if right failed, cancel
                else                        state = terminateStage(state);
            }
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
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

    /* --- --- --- right-side processing --- --- --- */

    private B advanceLB(B lb) {
        nextIntBinding.sequence = intBinding.sequence; // sync bindings sequence number
        // swap intBinding and nextIntBinding
        var intBinding  = this.intBinding;
        intBinding      = nextIntBinding;
        nextIntBinding  = this.intBinding;
        this.intBinding = intBinding;
        lb = lb.dropHead();
        if (lb  == null) { // no linked batch, take fillingLB
            lb        = fillingLB;
            fillingLB = null;
            if (lb != null && lb.rows == 0) // recycle if fillingLB was empty
                lb = batchType.recycle(lb);
        }
        this.lb = lb;
        return lb;
    }

    private int startNextBinding(int st) {
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
            if ((st&IS_CANCEL_REQ) == 0)
                maybeRequestLeft();
            if (lb == null) {
                rightRecv.upstream.rebindPrefetchEnd();
                if ((st & LEFT_TERM) == 0) st = unlock(st, 0, RIGHT_STARVED);
                else                       st = terminateStage(st);
            } else {
                // release lock but forbid onBatch() from calling startNextBinding()
                // and forbid request() from calling upstream.request()
                st = unlock(st, RIGHT_STARVED, RIGHT_BINDING);
                ++intBinding.sequence;
                rebind(intBinding.attach(lb, lr), rightRecv.upstream);
                st = lock(st);
                ++rightRecv.bindingSeq;
                rightRecv.upstreamEmpty = true;
                rightRecv.listenerNotified = false;
                rightRecv.upstream.request((st & FILTER_BIND) == 0 ? requested : 2);
                st = unlock(st, RIGHT_BINDING, 0); // rebind complete, allow upstream.request()
            }
        } catch (Throwable t) {
            log.error("{}.startNextBinding() failed after seq={}", this, rightRecv.bindingSeq, t);
            if ((st&LOCKED_MASK) != 0)
                st = unlock(st); // avoid self-deadlock on rightTerminated/lock()
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
    protected boolean lexContinueRight(BatchBinding binding, Emitter<B> emitter) {
        return false;
    }

    /**
     * By default, equivalent to {@code rightEmitter.rebind(binding)}. This may be overridden
     * to implement non-standard joining, e.g., lexical joining.
     *
     * @param binding a {@link BatchBinding} to be used with {@link Emitter#rebind(BatchBinding)}
     * @param rightEmitter the right-side emitter
     */
    protected void rebind(BatchBinding binding, Emitter<B> rightEmitter) {
        if (binding.row == 0)
            rightEmitter.rebindPrefetch(binding);
        rightEmitter.rebind(binding);
    }

    private static final class RebindTask extends EmitterService.Task {

        private final BindingStage<?> bs;
        private RebindTask(BindingStage<?> bs) {
            super(EMITTER_SVC, RR_WORKER, CREATED, TASK_FLAGS);
            this.bs = bs;
        }

        public void schedule() {
            preferredWorker = currentWorker();
            runner.requireStealer();
            awake();
        }

        @Override protected void task(int threadId) {
            int st = bs.lock(bs.statePlain());
            try {
                if ((st&CAN_BIND_MASK) == LEFT_CAN_BIND)
                    st = bs.startNextBinding(st);
            } finally {
                if ((st&LOCKED_MASK) != 0) bs.unlock(st);
            }
        }

    }

    private final class RightReceiver implements Receiver<B> {
        private @MonotonicNonNull Receiver<B> downstream;
        private final Emitter<B> upstream;
        private final @Nullable BatchMerger<B> merger;
        private final BindType type;
        private final @Nullable BindListener bindingListener;
        private final byte passThrough;
        private boolean upstreamEmpty, listenerNotified, rebindReleased;
        private @Nullable B singleton;
        private final int outCols;
        private final BatchType<B> bt;
        private long bindingSeq = -1;

        public RightReceiver(@Nullable BatchMerger<B> merger, byte passThrough, int outCols,
                             BindType type, Emitter<B> rightUpstream,
                             @Nullable BindListener bindingListener) {
            this.bt = rightUpstream.batchType();
            this.upstream = rightUpstream;
            this.merger = merger;
            this.passThrough = passThrough;
            this.outCols = outCols;
            this.type = type;
            this.bindingListener = bindingListener;
        }

        @Override public Stream<? extends StreamNode> upstreamNodes() {
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
            B b;
            boolean recycleToSingleton = false;
            if (merger != null) {
                b = merger.merge(null, lb, lr, rb);
            } else if (passThrough == PASSTHROUGH_RIGHT && rb != null) {
                b = rb;
            } else {
                recycleToSingleton = true;
                b = bt.empty(singleton, outCols);
                singleton = null;
                if (passThrough == PASSTHROUGH_RIGHT) {
                    b.beginPut();
                    b.commitPut();
                } else {
                    b.putRow(lb, lr);
                }
            }

            try {
                // notify binding is not empty
                if (!listenerNotified && bindingListener != null) {
                    bindingListener.nonEmptyBinding(bindingSeq);
                    listenerNotified = true;
                }

                // update stats and requested
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchDelivered(b);
                int state = lock(statePlain());
                requested -= b == null ? 0 : b.totalRows();
                unlock(state);

                // deliver batch/row
                if (ResultJournal.ENABLED)
                    ResultJournal.logBatch(BindingStage.this, b);
                b = downstream.onBatch(b);
                if      (recycleToSingleton)  singleton = b;
                else if (merger != null)      bt.recycle(b);
                else                          rb = b;
            } catch (Throwable t) {
                handleEmitError(downstream, BindingStage.this, false, t);
            }
            return rb;
        }

        private int cancelFutureBindings(int st) {
            if (ENABLED) journal("cancelFutureBindings(), bs=", BindingStage.this);
            dropLeftQueued();
            st = setFlagsRelease(st, RIGHT_STARVED);
            if ((st&LEFT_TERM) == 0) leftUpstream.cancel();
            else                     st = terminateStage(st);
            return st;
        }

        @Override public void onComplete() {
            if (ENABLED) journal("right.onComplete() on ", BindingStage.this);
            int lockedSt = 0;
            try {
                assert lb != null;
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
                    lockedSt = lock(statePlain());
                    if ((lockedSt&CAN_BIND_MASK)==RIGHT_CAN_BIND) {
                        lockedSt = startNextBinding(lockedSt);
                    } else if ((lockedSt&IS_CANCEL_REQ) != 0) {
                        lockedSt = cancelFutureBindings(lockedSt);
                    } else if (ENABLED) {
                        journal("r completed, but cannot startNextBinding, st=", lockedSt, flags, "on", BindingStage.this);
                    }
                }
            } catch (Throwable t) {
                log.error("onComplete() failed", t);
                rightFailedOrCancelled(RIGHT_FAILED, t);
            } finally {
                if ((lockedSt&LOCKED_MASK) != 0)
                    unlock(lockedSt);
            }
        }

        @Override public void onCancelled() {
            if (type.isJoin()) rightFailedOrCancelled(RIGHT_CANCELLED, null);
            else               onComplete();
        }

        @Override public void onError(Throwable cause) {
            rightFailedOrCancelled(RIGHT_FAILED, cause);
        }
    }
}
