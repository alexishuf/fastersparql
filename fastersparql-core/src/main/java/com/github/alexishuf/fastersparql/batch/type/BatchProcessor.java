package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.emit.*;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.HANGMAN;

public abstract class BatchProcessor<B extends Batch<B>, P extends BatchProcessor<B, P>>
        extends Stateful<P>
        implements Stage<B, B, P> {
    protected static final int EXPECT_CANCELLED  = 0x80000000;

    protected static final Flags PROC_FLAGS = Flags.DEFAULT.toBuilder()
            .flag(EXPECT_CANCELLED, "EXPECT_CANCELLED")
            .build();

    protected @Nullable Emitter<B, ?> upstream;
    protected @MonotonicNonNull Receiver<B> downstream;
    public final BatchType<B> batchType;
    public final Vars vars;
    public Vars bindableVars = Vars.EMPTY;
    private @Nullable HasFillingBatch<B> downstreamHFB;
    protected final EmitterStats stats = EmitterStats.createIfEnabled();

    /* --- --- --- lifecycle --- --- --- */

    public BatchProcessor(BatchType<B> batchType, Vars outVars, int initState, Flags flags) {
        super(initState, flags);
        assert flags.contains(PROC_FLAGS);
        this.batchType = batchType;
        this.vars      = outVars;
    }

    @Override protected void onPendingRelease() {
        cancel();
    }

    @Override protected void doRelease() {
        Owned.safeRecycle(upstream, this);
        super.doRelease();
    }


    /* --- --- --- processing --- --- --- */

    /**
     * Tries to process {@code b} in-place avoiding new allocations. If this cannot be done,
     * the result of processing {@code b} will be placed in a new batch which will then be
     * returned by this method call.
     *
     * <p>In both cases, the caller gives up ownership of {@code b}. If {@code b} cannot be
     * processed in-place it will be recycled.</p>
     *
     * @param b the batch to process
     * @return a batch with the result of the processing, which may be {@code b} itself.
     */
    public abstract Orphan<B> processInPlace(Orphan<B> b);

    /**
     * {@link Emitter#cancel()}s upstream, but treat {@link #onCancelled()} as
     * {@link #onComplete()}
     */
    protected void cancelUpstream() {
        setFlagsRelease(EXPECT_CANCELLED);
        if (upstream != null)
            upstream.cancel();
    }

    /** Whether {@link #cancelUpstream()} has been called. */
    protected boolean upstreamCancelled() { return (statePlain()&EXPECT_CANCELLED) != 0; }

    /* --- --- --- Stage --- --- --- */

    @SuppressWarnings("unchecked") @Override
    public @This P subscribeTo(Orphan<? extends Emitter<B, ?>> orphan) {
        if (this.upstream == null) {
            var upstream = orphan.takeOwnership(this);
            this.upstream = upstream;
            upstream.subscribe(this);
            bindableVars = bindableVars.union(upstream.bindableVars());
        } else if (this.upstream != orphan) {
            if (orphan != null)
                orphan.takeOwnership(HANGMAN).recycle(HANGMAN);
            throw new MultipleRegistrationUnsupportedException(this);
        }
        return (P)this;
    }

    @Override public Emitter<B, ?> upstream() { return upstream; }

    /* --- --- --- Emitter --- --- --- */

    @Override public final Vars vars() {return vars;}
    @Override public final BatchType<B> batchType() {return batchType;}

    @Override
    public final void subscribe(Receiver<B> r) throws MultipleRegistrationUnsupportedException {
        if (downstream == null) {
            downstream = r;
            //noinspection unchecked
            downstreamHFB = r instanceof HasFillingBatch<?> hfb
                          ? (HasFillingBatch<B>)hfb : null;
        } else if (downstream != r) {
            throw new MultipleRegistrationUnsupportedException(this);
        }
    }

    @Override public void rebindPrefetch(BatchBinding binding) {
        if (upstream != null) upstream.rebindPrefetch(binding);
    }

    @Override public void rebindPrefetchEnd() {
        if (upstream != null) upstream.rebindPrefetchEnd();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        resetForRebind(0, 0);
        requireAlive();
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (upstream != null)
            upstream.rebind(binding);
        if (ResultJournal.ENABLED)
            ResultJournal.rebindEmitter(this, binding);
    }

    @Override final public Vars bindableVars() { return bindableVars; }

    @Override public boolean   isComplete() { return isCompleted(state()); }
    @Override public boolean  isCancelled() { return isCancelled(state()); }
    @Override public boolean     isFailed() { return isFailed(state()); }
    @Override public boolean isTerminated() { return (state()&IS_TERM) != 0; }

    @Override public final boolean cancel() {
        if (upstream == null) {
            if (moveStateRelease(state(), CANCELLED))
                markDelivered(CANCELLED);
            return false;
        } else {
            return upstream.cancel();
        }
    }

    @Override public void request(long rows) throws NoReceiverException {
        if (rows <= 0)
            return;
        var upstream = this.upstream;
        if (upstream != null) {
            int st = statePlain();
            if ((st&IS_INIT) != 0)
                moveStateRelease(st, ACTIVE);
            upstream.request(rows);
        }
    }

    @Override public final Stream<? extends StreamNode> upstreamNodes() {
        return Stream.ofNullable(upstream);
    }
    /* --- --- --- Receiver --- --- --- */

    /**
     * Must be called at entry of every {@link #onBatch(Orphan)} implementation.
     * @param batch see {@link Receiver#onBatch(Orphan)}
     * @return Whether the batch should be processed/delivered downstream.
     */
    protected final boolean beforeOnBatch(B batch) {
        if ((stateAcquire()&IS_TERM) != 0) {
            assert (statePlain()&CANCELLED) != 0 : "onBatch() after non-cancel termination";
            return false; // already terminated, do not process batch
        }
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        return true;
    }

    /** Equivalent to {@link #beforeOnBatch(Batch)} */
    protected final boolean beforeOnBatch(Orphan<B> orphan) {
        B b = orphan.takeOwnership(this);
        boolean process = beforeOnBatch(b);
        b.releaseOwnership(this);
        return process;
    }

    protected @Nullable Orphan<B> fillingBatch() {
        return downstreamHFB == null ? null : downstreamHFB.pollFillingBatch();
    }

    /**
     * Must be called by every {@link #onBatch(Orphan)} implementation after
     * the batch has been processed and only if {@link #beforeOnBatch(Orphan)} returned true.
     * This method will deliver the processed batch downstream.
     * @param orphan see {@link Receiver#onBatch(Orphan)}
     * @param receivedRows {@link Batch#totalRows()} before the batch was processed.
     */
    protected void afterOnBatch(@Nullable Orphan<B> orphan, long receivedRows) {
        if (downstream == null) {
            throw new NoDownstreamException(this);
        } else if (upstream == null) {
            throw new NoUpstreamException(this);
        } else if (orphan == null) {
            cancelUpstream();
        } else {
            B batch = orphan.takeOwnership(this);
            if (batch.rows == 0) {
                batch.recycle(this);
            } else {
                if (EmitterStats.ENABLED && stats != null)
                    stats.onBatchDelivered(batch);
                if (ResultJournal.ENABLED)
                    ResultJournal.logBatch(this, batch);
                downstream.onBatch(batch.releaseOwnership(this));
            }
        }
    }

    @Override public final void onComplete() {
        try {
            if (moveStateRelease(statePlain(), COMPLETED)) {
                if (downstream == null) throw new NoDownstreamException(this);
                downstream.onComplete();
            }
        } finally {
            markDelivered(COMPLETED);
        }
    }

    @Override public final void onCancelled() {
        int st = state();
        try {
            if (downstream == null)
                throw new NoDownstreamException(this);
            if ((st&EXPECT_CANCELLED) != 0) {
                if (moveStateRelease(st, COMPLETED)) {
                    st = (st&FLAGS_MASK) | COMPLETED;
                    downstream.onComplete();
                }
            } else if (moveStateRelease(st, CANCELLED)) {
                st = (st&FLAGS_MASK) | CANCELLED;
                downstream.onCancelled();
            }
        } finally {
            markDelivered(st, st);
        }
    }

    @Override public final void onError(Throwable cause) {
        try {
            if (moveStateRelease(statePlain(), FAILED)) {
                if (downstream == null) throw new NoDownstreamException(this);
                downstream.onError(cause);
            }
        } finally {
            markDelivered(FAILED);
        }
    }
}
