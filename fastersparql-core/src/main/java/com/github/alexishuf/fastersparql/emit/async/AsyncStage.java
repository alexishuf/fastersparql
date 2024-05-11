package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import com.github.alexishuf.fastersparql.util.concurrent.Timestamp;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import com.github.alexishuf.fastersparql.util.owned.StaticMethodOwner;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public abstract sealed class AsyncStage<B extends Batch<B>>
        extends TaskEmitter<B, AsyncStage<B>>
        implements Stage<B, B, AsyncStage<B>> {
    private static final VarHandle TERMINATION, QUEUE;
    static {
        try {
            TERMINATION = MethodHandles.lookup().findVarHandle(AsyncStage.class, "plainTermination", int.class);
            QUEUE       = MethodHandles.lookup().findVarHandle(AsyncStage.class, "plainQueue",       Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final StaticMethodOwner CONSTRUCTOR = new StaticMethodOwner("AsyncStage.init()");
    private static final int IS_ASYNC = 0x40000000;
    private static final Flags ASYNC_FLAGS = TASK_FLAGS.toBuilder()
            .flag(IS_ASYNC, "ASYNC").build();

    private final Emitter<B, ?> up;
    @SuppressWarnings("unused") private @Nullable B plainQueue;
    @SuppressWarnings("unused") private int plainTermination;

    public static <B extends Batch<B>> Orphan<AsyncStage<B>>
    create(Orphan<? extends Emitter<B, ?>> upstream) {
        return new Concrete<>(upstream.takeOwnership(CONSTRUCTOR), EMITTER_SVC, RR_WORKER);
    }
    public static <B extends Batch<B>> Orphan<AsyncStage<B>>
    create(Orphan<? extends Emitter<B, ?>> upstream, EmitterService runner, int worker) {
        return new Concrete<>(upstream.takeOwnership(CONSTRUCTOR), runner, worker);
    }

    private AsyncStage(Emitter<B, ?> upstream, EmitterService runner, int worker) {
        super(upstream.batchType(), upstream.vars(), runner, worker, CREATED, ASYNC_FLAGS);
        this.up = upstream.transferOwnership(CONSTRUCTOR, this);
        upstream.subscribe(this);
    }

    @Override protected void doRelease() {
        super.doRelease();
        Batch.safeRecycle((Batch<?>)QUEUE.getAndSetAcquire(this, null), this);
        Owned.safeRecycle(up, this);
    }

    private static final class Concrete<B extends Batch<B>>
            extends AsyncStage<B> implements Orphan<AsyncStage<B>> {
        public Concrete(Emitter<B, ?> upstream, EmitterService runner, int worker) {
            super(upstream, runner, worker);
        }
        @Override public AsyncStage<B> takeOwnership(Object o) {return takeOwnership0(o);}
    }

    /* --- --- --- Stage --- --- --- */

    @Override public @This AsyncStage<B> subscribeTo(Orphan<? extends Emitter<B, ?>> emitter) {
        if (emitter != null && emitter != up)
            throw new MultipleRegistrationUnsupportedException(this);
        return null;
    }

    @Override public @MonotonicNonNull Emitter<B, ?> upstream()      { return up; }
    @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.of(up); }

    /* --- --- --- Rebindable --- --- --- */

    @Override public void rebind(BatchBinding b) {
        resetForRebind(0, 0);
        Batch.safeRecycle((Batch<?>)QUEUE.getAndSetAcquire(this, (Batch<?>)null), this);
        TERMINATION.setRelease(this, 0);
        up.rebind(b);
    }

    @Override public void rebindPrefetch(BatchBinding b) { up.rebindPrefetch(b); }
    @Override public void rebindPrefetchEnd()            { up.rebindPrefetchEnd(); }
    @Override public Vars bindableVars()                 { return up.bindableVars(); }

    /* --- --- --- Receiver --- --- --- */

    @SuppressWarnings("unchecked") @Override public void onBatch(Orphan<B> orphan) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(orphan);
        int state = statePlain();
        if ((state&IS_ASYNC) == 0) {
            long deadline = Timestamp.nextTick(2);
            deliver(orphan);
            if (Timestamp.nanoTime() > deadline) {
                lock();
                unlock(0, IS_ASYNC);
            }
        } else if ((state&IS_TERM) == 0) {
            B queue = (B)QUEUE.getAndSetAcquire(this, null);
            B tail = Batch.detachDistinctTail(queue, this);
            if (tail != null) {
                QUEUE.setRelease(this, queue); // allow head node to be delivered downstream
                tail.append(orphan);
                orphan = tail.releaseOwnership(this);
                queue = (B)QUEUE.getAndSetAcquire(this, null);
            }
            QUEUE.setRelease(this, Batch.quickAppend(queue, this, orphan));
            awake();
        } else {
            Orphan.recycle(orphan);
        }
    }

    @Override public void onBatchByCopy(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        int state = statePlain();
        if ((state&IS_ASYNC) == 0) {
            long deadline = Timestamp.nextTick(2);
            deliverByCopy(batch);
            if (Timestamp.nanoTime() > deadline) {
                setFlagsRelease(IS_ASYNC);
                unlock();
            }
        } else {
            if ((state&IS_TERM) == 0) {//noinspection unchecked
                B queue = (B)QUEUE.getAndSetAcquire(this, null);
                if (queue == null)
                    queue = bt.create(batch.cols).takeOwnership(this);
                queue.copy(batch);
                QUEUE.setRelease(this, queue);
                awake();
            }
        }
    }

    @Override public void onComplete() {
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, COMPLETED);
        } else {
            TERMINATION.setRelease(this, COMPLETED);
            awake();
        }
    }

    @Override public void onCancelled() {
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, CANCELLED);
        } else {
            TERMINATION.setRelease(this, CANCELLED);
            awake();
        }
    }

    @Override public void onError(Throwable cause) {
        error = cause;
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, FAILED);
        } else {
            TERMINATION.setRelease(this, FAILED);
            awake();
        }
    }

    /* --- --- --- TaskEmitter --- --- --- */


    @Override public boolean cancel() {
        return up.cancel();
    }

    @Override protected void resume() { up.request(requested()); }

    @Override protected boolean mustAwake() {
        return plainTermination != 0 || plainQueue != null;
    }

    @Override protected int produceAndDeliver(int state) {
        //noinspection unchecked
        B queue = (B)QUEUE.getAndSetAcquire(this, null);
        if (queue != null)
            deliver(queue.releaseOwnership(this));
        int termState = (int)TERMINATION.getAcquire(this);
        return plainQueue != null || termState == 0 ? state : termState;
    }
}
