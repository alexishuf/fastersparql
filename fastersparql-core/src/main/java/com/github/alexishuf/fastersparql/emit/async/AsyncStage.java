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
import org.checkerframework.common.returnsreceiver.qual.This;

import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public abstract sealed class AsyncStage<B extends Batch<B>>
        extends AsyncTaskEmitter<B, AsyncStage<B>>
        implements Stage<B, B, AsyncStage<B>> {
    private static final StaticMethodOwner CONSTRUCTOR = new StaticMethodOwner("AsyncStage.init()");
    private static final int IS_ASYNC = 0x40000000;
    private static final Flags ASYNC_FLAGS = TASK_FLAGS.toBuilder()
            .flag(IS_ASYNC, "ASYNC").build();

    private final Emitter<B, ?> up;

    public static <B extends Batch<B>> Orphan<AsyncStage<B>>
    create(Orphan<? extends Emitter<B, ?>> upstream) {
        return new Concrete<>(upstream.takeOwnership(CONSTRUCTOR), EMITTER_SVC);
    }
    public static <B extends Batch<B>> Orphan<AsyncStage<B>>
    create(Orphan<? extends Emitter<B, ?>> upstream, EmitterService runner) {
        return new Concrete<>(upstream.takeOwnership(CONSTRUCTOR), runner);
    }

    private AsyncStage(Emitter<B, ?> upstream, EmitterService runner) {
        super(upstream.batchType(), upstream.vars(), runner, CREATED, ASYNC_FLAGS);
        this.up = upstream.transferOwnership(CONSTRUCTOR, this);
        upstream.subscribe(this);
    }

    @Override protected void doRelease() {
        Owned.safeRecycle(up, this);
        super.doRelease();
    }

    private static final class Concrete<B extends Batch<B>>
            extends AsyncStage<B> implements Orphan<AsyncStage<B>> {
        public Concrete(Emitter<B, ?> upstream, EmitterService runner) {
            super(upstream, runner);
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
        up.rebind(b);
    }

    @Override public void rebindPrefetch(BatchBinding b) { up.rebindPrefetch(b); }
    @Override public void rebindPrefetchEnd()            { up.rebindPrefetchEnd(); }
    @Override public Vars bindableVars()                 { return up.bindableVars(); }

    /* --- --- --- Receiver --- --- --- */

    @Override public void onBatch(Orphan<B> orphan) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(orphan);
        int state = statePlain();
        if ((state&IS_ASYNC) == 0) {
            long deadline = Timestamp.nextTick(2);
            deliver(orphan);
            if (Timestamp.nanoTime() > deadline)
                setFlagsRelease(IS_ASYNC);
        } else if ((state&IS_TERM) == 0) {
            var tail = pollFillingBatch();
            if (tail != null) {
                B owned = tail.takeOwnership(this);
                owned.append(orphan);
                orphan = owned.releaseOwnership(this);
            }
            quickAppend(orphan);
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
            if (Timestamp.nanoTime() > deadline)
                setFlagsRelease(IS_ASYNC);
        } else {
            if ((state&IS_TERM) == 0) {
                B dst = takeFillingOrCreate().takeOwnership(this);
                dst.copy(batch);
                quickAppend(dst.releaseOwnership(this));
            }
        }
    }

    @Override public void onComplete() {
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, COMPLETED);
        } else {
            moveStateRelease(st, PENDING_COMPLETED);
            awake(true);
        }
    }

    @Override public void onCancelled() {
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, CANCELLED);
        } else {
            moveStateRelease(st, PENDING_CANCELLED);
            awake(true);
        }
    }

    @Override public void onError(Throwable cause) {
        error = cause;
        int st = statePlain();
        if ((st&IS_ASYNC) == 0) {
            deliverTermination(st, FAILED);
        } else {
            moveStateRelease(st, PENDING_FAILED);
            awake(true);
        }
    }

    /* --- --- --- AsyncTaskEmitter --- --- --- */

    @Override public boolean cancel() {
        if ((dropAllQueuedAndGetState()&IS_TERM) != 0)
            return false;
        return up.cancel();
    }

    @Override protected void resume() { up.request(requested()); }
}
