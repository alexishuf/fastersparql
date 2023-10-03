package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.async.Stateful;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoDownstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.NoUpstreamException;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.ResultJournal;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

public abstract class BatchProcessor<B extends Batch<B>> extends Stateful implements Stage<B, B> {
    protected static final VarHandle RECYCLED;
    static {
        try {
            RECYCLED = MethodHandles.lookup().findVarHandle(BatchProcessor.class, "recycled", Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected static final int EXPECT_CANCELLED  = 0x80000000;
    protected static final int ASSUME_THREAD_SAFE = 0x40000000;

    protected static final Flags PROC_FLAGS = Flags.DEFAULT.toBuilder()
            .flag(EXPECT_CANCELLED, "EXPECT_CANCELLED")
            .flag(ASSUME_THREAD_SAFE, "ASSUME_THREAD_SAFE")
            .build();

    protected @MonotonicNonNull Emitter<B> upstream;
    protected @MonotonicNonNull Receiver<B> downstream;
    @SuppressWarnings("unused") protected @Nullable B recycled;
    public final BatchType<B> batchType;
    public final Vars vars;
    protected final EmitterStats stats = EmitterStats.createIfEnabled();


    /* --- --- --- lifecycle --- --- --- */

    public BatchProcessor(BatchType<B> batchType, Vars outVars, int initState, Flags flags) {
        super(initState, flags);
        assert flags.contains(PROC_FLAGS);
        this.batchType = batchType;
        this.vars = outVars;
    }

    @Override protected void doRelease() {
        //noinspection unchecked
        Batch.recyclePooled((B)RECYCLED.getAndSetRelease(this, null));
        super.doRelease();
    }

    /**
     * Notifies that this processor will not be used anymore and that it may release
     * resources it holds (e.g., pooled objects).
     */
    public final void release() {
        if (upstream != null || downstream != null)
            throw new IllegalStateException("release() called on a BatchProcessor being used as a Stage.");
        if (moveStateRelease(statePlain(), CANCELLED))
            markDelivered(CANCELLED);
    }

    /**
     * Once called, writes and reads to {@code recycled} will <strong>NOT</strong> be atomic. Thus,
     * the caller promises to only interact with the processor from a single thread.
     *
     * <p>Calling this method when this processor is used as an {@link Emitter}/{@link Receiver},
     * is not necessary.</p>
     */
    public final void assumeThreadSafe() {
        setFlagsRelease(statePlain(), ASSUME_THREAD_SAFE);
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
    public abstract B processInPlace(B b);

    /**
     * {@link Emitter#cancel()}s upstream, but treat {@link #onCancelled()} as
     * {@link #onComplete()}
     */
    protected void cancelUpstream() {
        setFlagsRelease(statePlain(), EXPECT_CANCELLED);
        if (upstream != null)
            upstream.cancel();
    }

    /** Whether {@link #cancelUpstream()} has been called. */
    protected boolean upstreamCancelled() { return (statePlain()&EXPECT_CANCELLED) != 0; }

    /* --- --- --- Stage --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> e) {
        if      (this.upstream ==  null) (this.upstream = e).subscribe(this);
        else if (this.upstream !=     e) throw new MultipleRegistrationUnsupportedException(this);
        return this;
    }

    /* --- --- --- Emitter --- --- --- */

    @Override public final Vars vars() {return vars;}
    @Override public final BatchType<B> batchType() {return batchType;}

    @Override
    public final void subscribe(Receiver<B> r) throws MultipleRegistrationUnsupportedException {
        if      (this.downstream == null) this.downstream = r;
        else if (this.downstream !=    r) throw new MultipleRegistrationUnsupportedException(this);
    }

    @Override public void rebindAcquire() {
        if (upstream != null) upstream.rebindAcquire();
        delayRelease();
    }

    @Override public void rebindRelease() {
        if (upstream != null) upstream.rebindRelease();
        allowRelease();
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        resetForRebind(0, 0);
        if (EmitterStats.ENABLED && stats != null)
            stats.onRebind(binding);
        if (upstream != null)
            upstream.rebind(binding);
        if (ResultJournal.ENABLED)
            ResultJournal.rebindEmitter(this, binding);
    }

    @Override public final void cancel() {
        if (upstream != null)
            upstream.cancel();
    }

    @Override public void request(long rows) throws NoReceiverException {
        if (upstream == null) throw new NoUpstreamException(this);
        int state = statePlain();
        if ((state&STATE_MASK) == CREATED)
            moveStateRelease(state, ACTIVE);
        upstream.request(rows);
    }

    @Override public final Stream<? extends StreamNode> upstream() {
        return Stream.ofNullable(upstream);
    }
    /* --- --- --- Receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        if (batch == null)
            return null;
        batch = processInPlace(batch);

        if (downstream == null) {
            throw new NoDownstreamException(this);
        } else if (upstream == null) {
            throw new NoUpstreamException(this);
        } else if (batch == null) {
            cancelUpstream();
        } else {
            if (EmitterStats.ENABLED && stats != null)
                stats.onBatchDelivered(batch);
            if (ResultJournal.ENABLED)
                ResultJournal.logBatch(this, batch);
            batch = downstream.onBatch(batch);
        }
        return batch;
    }

    @Override public void onRow(B batch, int row) {
        if (batch == null) return;
        B tmp = batch.type().empty(Batch.asUnpooled(recycled), 1, batch.cols, batch.localBytesUsed(row));
        recycled = null;
        tmp.putRow(batch, row);
        recycled = Batch.asPooled(onBatch(tmp));
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
        try {
            if (moveStateRelease(statePlain(), CANCELLED)) {
                if (downstream == null) throw new NoDownstreamException(this);
                if ((statePlain()&EXPECT_CANCELLED) != 0) downstream.onComplete();
                else downstream.onCancelled();
            }
        } finally {
            markDelivered(CANCELLED);
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

    /* --- --- --- recycling --- --- --- */

    /**
     * Offers a batch for recycling and reuse in {@link BatchProcessor#processInPlace(Batch)}.
     *
     * @param batch a batch to recycle
     * @return {@code null} if the batch is now owned by the {@link BatchProcessor},
     *         {@code batch} if ownership remains with caller
     */
    public final @Nullable B recycle(@Nullable B batch) {
        if (batch == null)
            return null;
        batch.markPooled();
        if ((statePlain()&ASSUME_THREAD_SAFE) != 0) {
            if (recycled != null)
                return batch;
            recycled = batch;
            return null;
        } else {
           if (RECYCLED.compareAndExchangeRelease(this, null, batch) == null)
               return null;
           return batch.untracedUnmarkPooled();
        }
    }

    /**
     * Takes ownership of a previously {@link BatchProcessor#recycle(Batch)}ed batch.
     *
     * <p>The use case is to move the batch somewhere else when the {@link BatchProcessor}
     * is about to become unreachable </p>
     *
     * @return a previously {@link BatchProcessor#recycle(Batch)}ed batch or {@code null}
     */
    public final @Nullable B stealRecycled() {
        //noinspection unchecked
        B b = (B) RECYCLED.getAndSetAcquire(this, null);
        if (b != null) b.unmarkPooled();
        return b;
    }

    /**
     * Cheap test if this processor holds a recycled batch. This method is prone to both false
     * positives and false negatives.
     */
    public final boolean hasRecycled() {
        return recycled != null;
    }

    protected final B getBatch(int rows, int cols, int localBytes) {
        if ((statePlain()&ASSUME_THREAD_SAFE) != 0) {
            var b = batchType.empty(Batch.asUnpooled(recycled), rows, cols, localBytes);
            recycled = null;
            return b;
        } else {
            //noinspection unchecked
            B b = (B) RECYCLED.getAndSetAcquire(this, null);
            if (b != null)
                b.unmarkPooled();
            return batchType.empty(b, rows, cols, localBytes);
        }
    }
}
