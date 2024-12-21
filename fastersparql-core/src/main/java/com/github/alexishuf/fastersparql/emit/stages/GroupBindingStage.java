package com.github.alexishuf.fastersparql.emit.stages;

import com.github.alexishuf.fastersparql.batch.operators.GroupBind;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.client.EmitBindQuery;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.async.EmitterService;
import com.github.alexishuf.fastersparql.emit.async.TaskEmitter;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.StreamNodeDOT;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.batch.type.Batch.quickAppendTrusted;
import static com.github.alexishuf.fastersparql.util.StreamNodeDOT.appendRequested;
import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public abstract sealed class GroupBindingStage<B extends Batch<B>>
        extends TaskEmitter<B, GroupBindingStage<B>>
        implements Stage<B, B, GroupBindingStage<B>> {
    private static final VarHandle LB, RB;
    static {
        try {
            LB = MethodHandles.lookup().findVarHandle(GroupBindingStage.class, "plainLB", Batch.class);
            RB = MethodHandles.lookup().findVarHandle(GroupBindingStage.class, "plainRB", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final int LEFT_COMPLETED  = 0x40000000;
    private static final int LEFT_FAILED     = 0x20000000;
    private static final int LEFT_CANCELLED  = 0x10000000;
    private static final int RIGHT_COMPLETED = 0x08000000;
    private static final int RIGHT_STARVED   = 0x04000000;
    private static final int RIGHT_CANCELLED = 0x02000000;
    private static final int RIGHT_FAILED    = 0x01000000;
    private static final int DOWN_REQUESTED  = 0x00800000;
    private static final int LEFT_TERM              = LEFT_COMPLETED|LEFT_CANCELLED|LEFT_FAILED;
    private static final int RIGHT_TERM             = RIGHT_COMPLETED|RIGHT_CANCELLED|RIGHT_FAILED;
    private static final int LEFT_TERM_RIGHT_FAILED = LEFT_TERM|RIGHT_FAILED;
    private static final int RIGHT_TERM_OR_STARVED  = RIGHT_TERM|RIGHT_STARVED;
    private static final int ANY_FAILED             = LEFT_FAILED|RIGHT_FAILED;
    private static final int ANY_CANCELLING         = IS_CANCEL_REQ|LEFT_CANCELLED|RIGHT_CANCELLED;
    private static final Flags FLAGS = TASK_FLAGS.toBuilder()
            .flag(LEFT_COMPLETED,  "LEFT_COMPLETED")
            .flag(LEFT_CANCELLED,  "LEFT_CANCELLED")
            .flag(LEFT_FAILED,     "LEFT_FAILED")
            .flag(RIGHT_COMPLETED, "RIGHT_COMPLETED")
            .flag(RIGHT_STARVED,   "RIGHT_STARVED")
            .flag(RIGHT_CANCELLED, "RIGHT_CANCELLED")
            .flag(RIGHT_FAILED,    "RIGHT_FAILED")
            .flag(DOWN_REQUESTED,  "DOWN_REQUESTED")
            .build();
    private static final short MAX_LEFT_CHUNK = (short)Math.min(1<<12, GroupBind.GROUP_SIZE);


    @SuppressWarnings("unused") private @Nullable B plainRB;
    private Emitter<B, ?> rightUpstream;
    private final Emitter<B, ?> leftUpstream;
    private GroupBind<B> gbState;
    private final RightReceiver rightReceiver;
    @SuppressWarnings("unused") private @Nullable B plainLB;
    private final short leftChunk;
    private final boolean negation, exists;
    private short leftPending;
    private long lastRebindSeq;
    private @Nullable Vars lastRebindVars;
    private @Nullable BatchMerger<B, ?> extRebindMerger;
    private @Nullable B extRebindLeftRow;
    private final GroupBind<B> gbTemplate;
    private final Vars bindableVars;

    /* --- --- --- lifecycle --- --- --- */

    private GroupBindingStage(Orphan<GroupBind<B>> gb) {
        super(GroupBind.batchType(gb), GroupBind.resultVars(gb),
              CREATED|RIGHT_STARVED, FLAGS);
        this.gbTemplate    = gb.takeOwnership(this);
        this.gbState       = gbTemplate;
        this.rightReceiver = new RightReceiver();
        var bq             = (EmitBindQuery<B>)this.gbState.bindQuery;
        this.leftUpstream  = bq.bindings.takeOwnership(this);
        this.bindableVars  = this.leftUpstream.bindableVars().union(this.gbState.bindableVars());
        this.leftChunk     = (short)Math.min(MAX_LEFT_CHUNK,
                                             batchType().preferredRowsPerBatch(vars()));
        this.negation      = bq.type.isNegation();
        this.exists        = bq.type == BindType.EXISTS;
        this.leftUpstream.subscribe(this);
    }

    @Override protected void doRelease() {
        try {
            recycleLBAndGetNonEmpty();
            Owned.safeRecycle(leftUpstream,this);
            gbState          = Owned.safeRecycle(gbState,             this);
            rightUpstream    = Owned.safeRecycle(rightUpstream,       this);
            extRebindMerger  = Owned.safeRecycle(extRebindMerger,        this);
            extRebindLeftRow = Batch.safeRecycle(extRebindLeftRow, this);
        } finally {super.doRelease();}
    }

    public static <B extends Batch<B>>
    Orphan<GroupBindingStage<B>> create(Orphan<GroupBind<B>> gb) {return new Concrete<>(gb);}

    private static final class Concrete<B extends Batch<B>> extends GroupBindingStage<B>
            implements Orphan<GroupBindingStage<B>> {
        public Concrete(Orphan<GroupBind<B>> gb) {super(gb);}
        @Override public GroupBindingStage<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    /* --- --- --- Stage methods --- --- --- */

    @Override
    public @This GroupBindingStage<B> subscribeTo(Orphan<? extends Emitter<B, ?>> upstream) {
        if (upstream != leftUpstream)
            throw new MultipleRegistrationUnsupportedException(this);
        return this;
    }

    @Override public @MonotonicNonNull Emitter<B, ?> upstream() {return leftUpstream;}

    @Override protected void appendToSimpleLabel(StringBuilder sb) {
        sb.append('[').append(gbTemplate.bindQuery.type.name());
        sb.append("] vars=").append(gbTemplate.resultVars);
    }

    @Override protected void appendToState(StringBuilder sb) {
        appendRequested(sb.append(" leftPending="), leftPending);
        appendRequested(sb.append(" leftQueued="),
                gbState == null ? 0 : gbState.enqueuedLeftRows());
        B lbIn = plainLB;
        if (lbIn != null)
            appendRequested(sb.append("+"), lbIn.totalRows());
    }

    @Override public Stream<? extends StreamNode> upstreamNodes() {
        Emitter<B, ?> ru = rightUpstream;
        return ru == null ? Stream.of(leftUpstream) : Stream.of(leftUpstream, ru);
    }

    /* --- --- --- Receiver methods --- --- --- */

    private void enqueueAsync(VarHandle H, Orphan<B> b) {
        if (Batch.peekRows(b) > 0) {
            //noinspection unchecked
            B q = quickAppendTrusted((B)H.getAndSetAcquire(this, null), this, b);
            if ((Batch<?>)H.compareAndExchangeRelease(this, null, q) != null)
                throw new IllegalStateException("Concurrent enqueueAsync()");
        } else {
            Orphan.safeRecycle(b);
        }
        awakeSameWorker();
    }

    @Override public void onBatch(Orphan<B> b) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(b);
        enqueueAsync(LB, b);
    }

    private boolean recycleLBAndGetNonEmpty() {
        var b = (Batch<?>)LB.getAndSetAcquire(this, null);
        boolean nonEmpty = b != null && b.rows > 0;
        if (b != null) Batch.safeRecycle(b, this);
        return nonEmpty;
    }

    private void setFlagAndAwake(int flag) {
        setFlagsRelease(flag);
        awakeSameWorker();
    }
    private void setErrorFlagAndAwake(@Nullable Throwable error, int flag) {
        if (this.error == UNSET_ERROR && error != null)
            this.error = error;
        setFlagAndAwake(flag);
    }

    @Override public void onComplete()         { setFlagAndAwake(LEFT_COMPLETED); }
    @Override public void onCancelled()        { setFlagAndAwake(LEFT_CANCELLED); }
    @Override public void onError(Throwable e) { setErrorFlagAndAwake(e, LEFT_FAILED); }

    /* --- --- --- Emitter methods --- --- --- */

    @Override public void rebind(BatchBinding binding) throws RebindException {
        if (lastRebindSeq == binding.sequence)
            return;
        lastRebindSeq = binding.sequence;
        var oldGB     = gbState;
        resetForRebind(LEFT_TERM|RIGHT_TERM, LOCKED_MASK|RIGHT_STARVED);
        try {
            if (EmitterStats.ENABLED && stats != null)
                stats.onRebind(binding);
            if (ResultJournal.ENABLED)
                ResultJournal.rebindEmitter(this, binding);

            // rebind leftUpstream
            recycleLBAndGetNonEmpty();
            leftUpstream.rebind(binding);
            leftPending = 0;

            // rebind gb
            gbState = gbTemplate.bound(binding).takeOwnership(this);
            // gb.resultVars may have changed due to startGB.rebind(binding).
            // Variables replaced must be re-inserted when batches arrive on rightOnBatch()
            var bt = batchType();
            Vars bVars = binding.vars, rVars = gbTemplate.resultVars;
            if (!gbState.resultVars.equals(oldGB.resultVars) || !bVars.equals(lastRebindVars)) {
                lastRebindVars = bVars;
                Owned.safeRecycle(extRebindMerger, this);
                extRebindMerger = bt.merger(rVars, bVars, gbState.resultVars).takeOwnership(this);
            } else if (gbState.resultVars.equals(gbTemplate.resultVars) && extRebindMerger != null) {
                extRebindMerger = Owned.safeRecycle(extRebindMerger, this);
            }
            extRebindLeftRow = bt.empty(extRebindLeftRow, this, bVars.size());
            binding.putRow(extRebindLeftRow);
        } catch (Throwable t) {
            journal("rebind failed for ", this, t.toString());
            error = t;
            try {
                leftUpstream.cancel();
            } catch (Throwable ignored) {}
            awakeSameWorker();
        } finally {
            if (oldGB != gbTemplate && oldGB != gbState)
                Owned.safeRecycle(oldGB, this);
            unlock();
        }
    }

    @Override public Vars bindableVars() {return bindableVars;}

    /* --- --- --- TaskEmitter methods --- --- --- */

    @Override protected void resume() {setFlagAndAwake(DOWN_REQUESTED);}

    @Override protected int doCancel(int st) {
        if ((st&IS_CANCEL_REQ) == 0)
            moveStateRelease(st, CANCEL_REQUESTED); // cancelled by upstream
        if ((st&LEFT_TERM) == 0)
            leftUpstream.cancel();
        if ((st&RIGHT_TERM_OR_STARVED) == 0 && rightUpstream != null)
            rightUpstream.cancel();
        if ((st&LEFT_TERM) != 0 && (st&RIGHT_TERM_OR_STARVED) != 0) {
            boolean hadLeftQueued = recycleLBAndGetNonEmpty() || gbState.enqueuedLeftRows() != 0;
            boolean hasError = error != UNSET_ERROR && error != null;
            return hasError ? FAILED : hadLeftQueued ? CANCELLED : COMPLETED;
        }
        return 0;
    }

    private int taskEndGroupDeliverQueuedAndResetRightFlags(int st) {
        //noinspection unchecked
        Orphan<B> queue = processRight((B)RB.getAndSetAcquire(this, null));
        Orphan<B> term = gbState.endCurrentGroup();
        if (queue != null)
            term = term == null ? queue : Batch.quickAppendTrusted(queue, term);
        if (term != null)
            deliver(mergeWithExtRebind(term));
        if ((st&LEFT_TERM_RIGHT_FAILED) == RIGHT_FAILED)
            leftUpstream.cancel();
        if ((st&RIGHT_COMPLETED) != 0)
            st = changeFlagsRelease(RIGHT_TERM, RIGHT_STARVED);
        // else: keep RIGHT_FAILED, do not call startNextGroup()
        return st;
    }

    private void taskEnqueueLeftRows() {
        @SuppressWarnings("unchecked")
        B lbIn = (B)LB.getAndSetAcquire(this, null);
        if (lbIn != null) {
            leftPending = (short)Math.max(0, leftPending-lbIn.totalRows());
            gbState.enqueueLeftBatch(lbIn.releaseOwnership(this));
        }
    }

    private long rightRequestSize(long requested) {
        if (requested <= 0)
            return 0;
        return negation ? Long.MAX_VALUE : (exists ? 1 : requested);
    }

    private int taskStartGroup(int st) {
        Plan plan = gbState.startNextGroup();
        if (plan != null) {
            // start a new rightUpstream
            rightUpstream = Owned.safeRecycle(rightUpstream, this);
            Orphan<? extends Emitter<B, ?>> em = plan.emit(batchType(), Vars.EMPTY);
            var ru = em.takeOwnership(this);
            ru.subscribe(rightReceiver);
            rightUpstream = ru;
            st = clearFlagsAcquire(RIGHT_STARVED);
            long n = rightRequestSize(plainRequested);
            if (n > 0)
                ru.request(n);
        }
        return st;
    }

    private void taskRequestFromLeft() {
        int pendingOrQueued = leftPending+gbState.enqueuedLeftRows();
        if (pendingOrQueued <= leftChunk) {
            // do not request more than requested()
            // if pendingOrQueued==0, request 2*leftChunk
            short req = (short)Math.min(requested()-pendingOrQueued,
                                        (long)leftChunk<<((pendingOrQueued-1)>>>31));
            if (req > 0) {
                leftPending = req;
                leftUpstream.request(req);
            }
        }
    }

    private int taskNonCancelTermination(int st) {
        if ((st&RIGHT_STARVED) != 0) {
            assert plainLB == null && gbState.enqueuedLeftRows() == 0
                    : "expected no left rows enqueued with RIGHT_STARVED";
            return (st&ANY_FAILED) != 0 ? FAILED : COMPLETED;
        } else if ((st&RIGHT_FAILED) != 0) {
            recycleLBAndGetNonEmpty();
            return FAILED;
        } // RIGHT_CANCELLED|RIGHT_COMPLETED will be processed on next task()
        return 0;
    }

    private Orphan<B> processRight(@Nullable B b)  {
        return b == null ? null : gbState.processRightBatch(b.releaseOwnership(this));
    }
    private Orphan<B> mergeWithExtRebind(Orphan<B> b) {
        var m = extRebindMerger;
        return m == null ? b : mergeWithExtRebind0(m, b);
    }
    private Orphan<B> mergeWithExtRebind0(BatchMerger<B, ?> merger, Orphan<B> b) {
        B right = b.takeOwnership(this);
        B left  = Objects.requireNonNull(extRebindLeftRow);
        Orphan<B> merged = merger.merge(pollDownstreamFillingBatch(), left, 0, right);
        Owned.safeRecycle(right, this);
        return merged;
    }
    private int taskHandleRightBatches(int st) {
        var ru = rightUpstream;
        @SuppressWarnings("unchecked") B queue = (B)RB.getAndSetAcquire(this, null);
        Orphan<B> orphan = queue == null ? null : processRight(queue);
        // request() from right before delivering orphan to improve concurrency
        if (ru != null && (st&DOWN_REQUESTED) != 0) {
            st = clearFlagsAcquire(DOWN_REQUESTED);
            ru.request(rightRequestSize(plainRequested));
        }
        // deliver queue, processed and merged
        if (orphan != null)
            deliver(mergeWithExtRebind(orphan));
        return st;
    }

    private int taskException(int term, Throwable cause) {
        int st = state();
        if (term == 0 && (st&IS_TERM_OR_DELIVERED) == 0) {
            if (error == UNSET_ERROR || error == null)
                error = cause;
            if ((st&LEFT_TERM) == 0) {
                try {
                    leftUpstream.cancel();
                } catch (Throwable ignored) {}
            }
            if ((st&RIGHT_TERM_OR_STARVED) == 0) {
                try {
                    Emitter<B, ?> ru = rightUpstream;
                    if (ru != null)
                        ru.cancel();
                } catch (Throwable ignored) {}
            }
            return FAILED;
        }
        return term;
    }

    @Override protected void task(EmitterService.Worker worker, int threadId) {
        this.threadId = (short)threadId;
        int term = 0, st = state(); // lock() forbids concurrent rebind()
        try {
            if ((st&ANY_CANCELLING) != 0) {
                term = doCancel(st);
            } else if ((st&IS_LIVE) != 0) {
                if ((st&RIGHT_TERM) != 0 && (st&RIGHT_STARVED) == 0) // right terminated
                    st = taskEndGroupDeliverQueuedAndResetRightFlags(st);
                taskEnqueueLeftRows();
                if ((st&RIGHT_STARVED)          != 0) st = taskStartGroup(st);
                if ((st&LEFT_TERM)              != 0) term = taskNonCancelTermination(st);
                if ((st&LEFT_TERM_RIGHT_FAILED) == 0) taskRequestFromLeft();
                if ((st&IS_LIVE)                != 0) st = taskHandleRightBatches(st);
            }
        } catch (Throwable t) {
            term = taskException(term, t);
        } finally {
            if (term != 0) {
                assert (st&LEFT_TERM) != 0 && (st&RIGHT_TERM_OR_STARVED) != 0
                        : "term != 0, one of the sides has not terminated";
                deliverTermination(st, term);
            }
        }
    }

    @Override protected int produceAndDeliver(int state) {return 0;}

    /* --- --- --- RightReceiver --- --- --- */

    private final class RightReceiver implements Receiver<B>  {
        @Override public void     onBatch(Orphan<B> b) {enqueueAsync(RB, b);}
        @Override public void  onComplete()            {setFlagAndAwake(RIGHT_COMPLETED);}
        @Override public void onCancelled()            {setFlagAndAwake(RIGHT_CANCELLED);}
        @Override public void     onError(Throwable e) {setErrorFlagAndAwake(e, RIGHT_FAILED);}

        @Override public String label(StreamNodeDOT.Label type) {
            return "[RIGHT]"+GroupBindingStage.this.label(type);
        }
        @Override public Stream<? extends StreamNode> upstreamNodes() {
            return Stream.ofNullable(rightUpstream);
        }
    }
}
