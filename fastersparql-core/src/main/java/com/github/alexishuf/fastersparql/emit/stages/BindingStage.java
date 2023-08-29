package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.BindListener;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.client.SparqlClient;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.asPooled;
import static com.github.alexishuf.fastersparql.batch.type.Batch.asUnpooled;
import static com.github.alexishuf.fastersparql.emit.Emitters.handleEmitError;
import static com.github.alexishuf.fastersparql.model.BindType.EXISTS;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.THREAD_JOURNAL;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;

public abstract class BindingStage<B extends Batch<B>> extends Stateful implements Stage<B, B> {
    private static final Logger log = LoggerFactory.getLogger(BindingStage.class);
    private static final int LEFT_REQUESTED_CHUNK = 64;
    private static final int LEFT_REQUESTED_MAX   = LEFT_REQUESTED_CHUNK+16;

    /* flags embedded in plainState|S */

    private static final int LEFT_CANCELLED   = 0x01000000;
    private static final int LEFT_COMPLETED   = 0x02000000;
    private static final int LEFT_FAILED      = 0x04000000;
    private static final int LEFT_TERM        = LEFT_COMPLETED | LEFT_CANCELLED | LEFT_FAILED;
    private static final int RIGHT_CANCELLED  = 0x00100000;
    private static final int RIGHT_FAILED     = 0x00400000;
    private static final int RIGHT_TERM       = RIGHT_CANCELLED | RIGHT_FAILED;
    private static final int RIGHT_STARVED    = 0x00800000;
    private static final int FILTER_BIND      = 0x00010000;
    private static final Flags BINDING_STAGE_FLAGS =
            Flags.DEFAULT.toBuilder()
                    .flag(LEFT_CANCELLED, "LEFT_CANCELLED")
                    .flag(LEFT_COMPLETED, "LEFT_COMPLETED")
                    .flag(LEFT_FAILED, "LEFT_FAILED")
                    .flag(RIGHT_CANCELLED, "RIGHT_CANCELLED")
                    .flag(RIGHT_FAILED, "RIGHT_FAILED")
                    .flag(RIGHT_STARVED, "RIGHT_STARVED")
                    .flag(FILTER_BIND, "FILTER_BIND").build();

    protected final BatchType<B> batchType;
    protected final Vars outVars;
    private final Emitter<B> leftUpstream;
    private final RightReceiver rightRecv;
    private @Nullable B fillingLB;
    private @Nullable B recycled0, recycled1;
    private int lr = -1;
    private @Nullable B lb;
    private int leftPending;
    private long requested;
    private @Nullable BatchBinding<B> rebindBinding;
    private Vars externalRebindVars = null;
    private @Nullable B externalRebindRow, rebindRow;
    private @Nullable BatchMerger<B> rebindMerger;
    private Throwable error = UNSET_ERROR;
    private final @Nullable EmitterStats stats = EmitterStats.createIfEnabled();

    public static final class ForPlan<B extends Batch<B>> extends BindingStage<B> {
        private final Plan right;
        public ForPlan(EmitBindQuery<B> bq, boolean weakDedup, @Nullable Vars projection) {
            super(bq.bindings, bq.type, bq, projection == null ? bq.resultVars() : projection,
                  bq.parsedQuery().emit(bq.bindings.batchType(), weakDedup));
            this.right = bq.parsedQuery();
        }
        @Override protected Object rightDisplay() {
            return sparqlDisplay(right.toString());
        }
    }

    public static final class ForSparql<B extends Batch<B>> extends BindingStage<B> {
        private final SparqlEndpoint endpoint;
        private final SparqlQuery right;
        public ForSparql(EmitBindQuery<B> bq, SparqlClient client) {
            super(bq.bindings, bq.type, bq, bq.resultVars(),
                  client.emit(bq.batchType(), bq.query));
            this.right = bq.query;
            this.endpoint = client.endpoint();
        }
        @Override protected Object rightDisplay() {
            return endpoint+"("+sparqlDisplay(right.toString())+")";
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
        Vars leftVars = bindings.vars();
        rightUpstream.rebindAcquire();
        var merger = batchType.merger(outVars, leftVars, rightUpstream.vars());
        this.rightRecv = new RightReceiver(merger, type, rightUpstream, listener);
        this.leftUpstream = bindings;
        bindings.subscribe(this);
        rightUpstream.subscribe(rightRecv);
    }

    @Override public Vars         vars()      { return outVars;   }
    @Override public BatchType<B> batchType() { return batchType; }

    private static final Pattern PROLOG_RX = Pattern.compile("(?:PREFIX|BASE|#).*\n");
    private static final Pattern STAR_RX = Pattern.compile("SELECT\\s*\\*\\s*WHERE\\s*\\{");
    private static final Pattern WHERE_RX = Pattern.compile("SELECT\\s*([^{]+)\\s*WHERE\\s*\\{");
    private static final Pattern N_SPACE_RX = Pattern.compile("(\\s)\\s+");
    private static final Pattern S_SPACE_RX = Pattern.compile("\\s*([.<>(){}])\\s*");
    private static final Pattern LONG_IRI_RX = Pattern.compile("<[^>]+([^>]{6})>");
    private static final AtomicInteger sparqlDisplayInvocations=  new AtomicInteger(0);
    protected static String sparqlDisplay(String sparql) {
        if ((sparqlDisplayInvocations.incrementAndGet() & 0x3ff) == 0x3ff)
            log.warn("Too many invocations of sparqlDisplay(), this should only be used for debug or for error reporting");
        sparql = PROLOG_RX.matcher(sparql).replaceAll("");
        sparql = STAR_RX.matcher(sparql).replaceAll("{");
        sparql = WHERE_RX.matcher(sparql).replaceAll("SELECT$1{");
        sparql = N_SPACE_RX.matcher(sparql).replaceAll("$1");
        sparql = S_SPACE_RX.matcher(sparql).replaceAll("$1");
        return LONG_IRI_RX.matcher(sparql).replaceAll("<...$1>");
    }

    protected abstract Object rightDisplay();

    @Override public String toString() {
        return format("%s@%x[%s]{vars=%s, left=%s, right=%s}",
                getClass().getSimpleName(), identityHashCode(this),
                rightRecv.type.name(), outVars, leftUpstream, rightDisplay());
    }

    @Override public String nodeLabel() {
        return format("%s@%x[%s] vars=%s requested=%s state=%s\nright=%s",
                getClass().getSimpleName(), identityHashCode(this),
                rightRecv.type.name(), outVars,
                requested > 999_999 ? Long.toHexString(requested) : Long.toString(requested),
                flags.render(state()), rightDisplay());
    }

    @Override public Stream<? extends StreamNode> upstream() {
        return Stream.concat(Stream.of(leftUpstream), Stream.ofNullable(rightRecv.upstream));
    }

    @Override protected void doRelease() {
        if (EmitterStats.ENABLED && stats != null)
            stats.report(log, this);
        rightRecv.rebindRelease();
        rightRecv.recycled = Batch.recyclePooled(rightRecv.recycled);
        recycled0          = Batch.recyclePooled(recycled0);
        recycled1          = Batch.recyclePooled(recycled1);
        rebindRow          = batchType.recycle(rebindRow);
        externalRebindRow  = batchType.recycle(externalRebindRow);
        lb                 = batchType.recycle(lb);
        fillingLB          = batchType.recycle(fillingLB);
        if (rebindBinding != null)
            rebindBinding.setRow(null, 0);
        if (rebindMerger != null)
            rebindMerger.release();
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
            if (THREAD_JOURNAL)
                journal("onBatch", rows, "st=", state, flags, "bStage=", this);
            if (lb == null) {
                lb = batch;
            } else if (fillingLB == null) {
                fillingLB = batch;
            } else {
                B tmp = fillingLB;
                fillingLB = null;
                state = unlock(state);
                tmp.put(batch);
                offer = batch;
                state = lock(state);
                if   (lb == null) lb = tmp;
                else              fillingLB    = tmp;
            }
            leftPending -= rows;
            if (offer == null) {
                if      ((offer = recycled0) != null) recycled0 = null;
                else if ((offer = recycled1) != null) recycled1 = null;
                if (offer != null) offer.unmarkPooled();
            }
            if ((state&RIGHT_STARVED) != 0)
                state = startNextBinding(state);
        } finally {
            if ((state&LOCKED_MASK) != 0)
                unlock(state);
        }
        return offer;
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

    @Override public void rebind(BatchBinding<B> binding) throws RebindException {
        int st = resetForRebind(LEFT_TERM|RIGHT_TERM, RIGHT_STARVED|LOCKED_MASK);
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            requested = 0;
            leftPending = 0;
            leftUpstream.rebind(binding);
            if (!binding.vars.equals(externalRebindVars))
                updateExternalRebindVars(binding);
            updateExternalRebindRow(binding);
            unlock(st);
        } catch (Throwable t) {
            this.error = t;
            unlock(st);
            markDelivered(st, FAILED);
            throw t;
        }
    }

    private void updateExternalRebindRow(BatchBinding<B> binding) {
        assert externalRebindRow != null;
        B bb = binding.batch;
        externalRebindRow.clear();
        if (bb != null) externalRebindRow.putRow(bb, binding.row);
    }
    private void updateExternalRebindVars(BatchBinding<B> binding) {
        Vars externalRebindVars = binding.vars;
        this.externalRebindVars = externalRebindVars;
        B bb = binding.batch;
        int localBytes =  bb == null ? 0 : bb.localBytesUsed(binding.row);
        externalRebindRow = batchType.empty(externalRebindRow, 1, externalRebindVars.size(), localBytes);
        if (rebindMerger != null)
            rebindMerger.release();
        Vars l = leftUpstream.vars(), u = l.union(externalRebindVars);
        rebindMerger = u == l ? null : batchType.merger(u, l, externalRebindVars);
    }

    @Override
    public void subscribe(Receiver<B> receiver) throws RegisterAfterStartException, MultipleRegistrationUnsupportedException {
        if (rightRecv.downstream != null && rightRecv.downstream != receiver)
            throw new MultipleRegistrationUnsupportedException(this);
        rightRecv.downstream = receiver;
        if (THREAD_JOURNAL)
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
            if ((state&RIGHT_STARVED) == 0)
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
            if (THREAD_JOURNAL)
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
            if (THREAD_JOURNAL)
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

    /* --- --- --- right-side processing --- --- --- */

    @SuppressWarnings("SameReturnValue") private @Nullable B recycle(B b) {
        if      (recycled0 == null) { b.markPooled(); recycled0 = b; }
        else if (recycled1 == null) { b.markPooled(); recycled1 = b; }
        else                          b.recycle();
        return null;
    }

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
                if ((st & LEFT_TERM) == 0) st = setFlagsRelease(st, RIGHT_STARVED);
                else                       st = terminateStage(st);
            } else {
                st = unlock(st, RIGHT_STARVED, 0);
                if (rebindMerger == null) {
                    if (rebindBinding == null)
                        rebindBinding = new BatchBinding<>(leftUpstream.vars());
                    rebindBinding.setRow(lb, lr);
                } else {
                    if (rebindBinding == null)
                        rebindBinding = new BatchBinding<>(rebindMerger.vars);
                    if (rebindRow != null)
                        rebindRow.clear();
                    rebindRow = rebindMerger.merge(rebindRow, lb, lr, externalRebindRow);
                    rebindBinding.setRow(rebindRow, 0);
                }
                rebind(rebindBinding, rightRecv.upstream);
                st = lock(st);
                ++rightRecv.bindingSeq;
                rightRecv.upstreamEmpty = true;
                rightRecv.listenerNotified = false;
                if (THREAD_JOURNAL)
                    journal("startNextBinding st=", st, flags, "seq=", rightRecv.bindingSeq, "stage=", this);
                rightRecv.upstream.request((st & FILTER_BIND) == 0 ? requested : 2);
            }
        } catch (Throwable t) {
            log.error("{}.startNextBinding() failed after seq={}", this, rightRecv.bindingSeq, t);
            if ((st&LOCKED_MASK) != 0)
                st = unlock(st); // avoid self-deadlock on rightTerminated/lock()
            rightTerminated(RIGHT_FAILED, t);
            rightRecv.upstream.cancel();
        } finally {
            if ((st & LOCKED_MASK) != 0)
                st = unlock(st);
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
     * @param lb the current left-side batch with the original values obtained from evaluating the
     *           left-side operand of the join.
     * @param lr current row in {@code lb}, i.e., the current left-side values of the current
     *           binding.
     * @return {@code null} if lexical joins are not implemented or if there are no more lexical
     *         variants for the current binding ({@code lb, lr}), else an {@link Emitter} that
     *         may provide additional solutions for the right-side of the join.
     */
    protected boolean lexContinueRight(B lb, int lr, Emitter<B> emitter) {
        return false;
    }

    /**
     * By default, equivalent to {@code rightEmitter.rebind(binding)}. This may be overridden
     * to implement non-standard joining, e.g., lexical joining.
     */
    protected void rebind(BatchBinding<B> binding, Emitter<B> rightEmitter) {
        rightEmitter.rebind(binding);
    }

    private final class RightReceiver implements Receiver<B> {
        private @MonotonicNonNull Receiver<B> downstream;
        private @Nullable B recycled;
        private final Emitter<B> upstream;
        private final BatchMerger<B> merger;
        private final BindType type;
        private final int downstreamCols;
        private final @Nullable BindListener bindingListener;
        private boolean upstreamEmpty, listenerNotified, rebindReleased;
        private long bindingSeq = -1;

        public RightReceiver(BatchMerger<B> merger, BindType type, Emitter<B> rightUpstream,
                             @Nullable BindListener bindingListener) {
            this.upstream = rightUpstream;
            this.merger = merger;
            this.type = type;
            this.bindingListener = bindingListener;
            this.downstreamCols = merger.vars.size();
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
            switch (type) {
                case JOIN,LEFT_JOIN,EXISTS -> merge(type == EXISTS ? null : rb);
                case NOT_EXISTS,MINUS      -> {
                    if (!listenerNotified && bindingListener != null) {
                        bindingListener.emptyBinding(bindingSeq);
                        listenerNotified = true;
                    }
                    upstream.cancel();
                }
            }
            return rb;
        }

        private void merge(@Nullable B rb) {
            assert lb != null;
            int rowsCap, localCap;
            if (type.isJoin() || rb == null) {
                rowsCap = 1;
                localCap = lb.localBytesUsed(lr);
            } else {
                rowsCap = rb.rows;
                localCap = rb.localBytesUsed();
            }
            B b = batchType.empty(asUnpooled(recycled), rowsCap, downstreamCols, localCap);
            recycled = null;
            if (type == EXISTS) b.putRow(lb, lr);
            else                b = merger.merge(b, lb, lr, rb);
            if (!listenerNotified && bindingListener != null) {
                bindingListener.nonEmptyBinding(bindingSeq);
                listenerNotified = true;
            }
            try {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchDelivered(b);
                recycled = asPooled(downstream.onBatch(b));
            } catch (Throwable t) {
                handleEmitError(downstream, BindingStage.this, false, t);
            }
        }

        @Override public void onComplete() {
            assert lb != null;
            try {
                if (lexContinueRight(lb, lr, upstream)) {
                    if (THREAD_JOURNAL)
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
