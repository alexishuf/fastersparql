package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Stage;
import com.github.alexishuf.fastersparql.emit.exceptions.MultipleRegistrationUnsupportedException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.util.StreamNode;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.stream.Stream;

import static com.github.alexishuf.fastersparql.emit.async.EmitterService.EMITTER_SVC;

public class AsyncStage<B extends Batch<B>> extends TaskEmitter<B> implements Stage<B, B> {
    private static final VarHandle TERMINATION, QUEUE;
    static {
        try {
            TERMINATION = MethodHandles.lookup().findVarHandle(AsyncStage.class, "plainTermination", int.class);
            QUEUE       = MethodHandles.lookup().findVarHandle(AsyncStage.class, "plainQueue",       Batch.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private static final int IS_ASYNC = 0x40000000;
    private static final Flags ASYNC_FLAGS = TASK_FLAGS.toBuilder()
            .flag(IS_ASYNC, "ASYNC").build();

    private final Emitter<B> up;
    @SuppressWarnings("unused") private @Nullable B plainQueue;
    @SuppressWarnings("unused") private int plainTermination;

    public AsyncStage(Emitter<B> upstream) {
        this(upstream, EMITTER_SVC, RR_WORKER);
    }
    public AsyncStage(Emitter<B> upstream, EmitterService runner, int worker) {
        super(upstream.batchType(), upstream.vars(), runner, worker, CREATED, ASYNC_FLAGS);
        this.up = upstream;
        upstream.subscribe(this);
    }

    /* --- --- --- Stage --- --- --- */

    @Override public @This Stage<B, B> subscribeTo(Emitter<B> emitter) {
        if (emitter != null && emitter != up)
            throw new MultipleRegistrationUnsupportedException(this);
        return null;
    }

    @Override public @MonotonicNonNull Emitter<B> upstream()      { return up; }
    @Override public Stream<? extends StreamNode> upstreamNodes() { return Stream.of(up); }

    /* --- --- --- Rebindable --- --- --- */

    @Override public void rebind(BatchBinding b) {
        resetForRebind(0, 0);
        //noinspection unchecked
        bt.recycle((B)QUEUE.getAndSetAcquire(this, (B)null));
        TERMINATION.setRelease(this, 0);
        up.rebind(b);
    }

    @Override public void rebindPrefetch(BatchBinding b) { up.rebindPrefetch(b); }
    @Override public void rebindPrefetchEnd()            { up.rebindPrefetchEnd(); }
    @Override public Vars bindableVars()                 { return up.bindableVars(); }

    @Override public void rebindAcquire() {
        super.rebindAcquire();
        up.rebindAcquire();
    }

    @Override public void rebindRelease() {
        super.rebindRelease();
        up.rebindRelease();
    }
    /* --- --- --- Receiver --- --- --- */

    @Override public @Nullable B onBatch(B batch) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchReceived(batch);
        int state = statePlain();
        if ((state&IS_ASYNC) == 0) {
            long deadline = Timestamp.nextTick(2);
            batch = deliver(batch);
            if (Timestamp.nanoTime() > deadline)
                unlock(setFlagsRelease(lock(state), IS_ASYNC));
            return batch;
        } else {
            if ((state&IS_TERM) == 0) {
                //noinspection unchecked
                B queue = (B) QUEUE.getAndSetAcquire(this, null);
                if (queue == null) queue = batch;
                else queue.append(batch);
                QUEUE.setRelease(this, queue);
                awake();
            } else {
                bt.recycle(batch);
            }
            return null;
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

    @Override protected void doRelease() {
        //noinspection unchecked
        bt.recycle((B)QUEUE.getAndSetAcquire(this, (B)null));
        super.doRelease();
    }

    @Override public void cancel() {
        int st = lock(statePlain()); // block setting of IS_ASYNC (if not set)
        try {
            if ((st&IS_ASYNC) == 0) { // not async, cancel up
                up.cancel();
                return;
            }
        } finally { unlock(st); }

        super.cancel(); // IS_ASYNC, cancel as a TaskEmitter
    }

    @Override protected void resume() { up.request(requested()); }

    @Override protected boolean mustAwake() {
        return plainTermination != 0 || plainQueue != null;
    }

    @Override protected int produceAndDeliver(int state) {
        //noinspection unchecked
        B queue = (B)QUEUE.getAndSetAcquire(this, null);
        if (queue != null)
            bt.recycle(deliver(queue));
        int termState = (int)TERMINATION.getAcquire(this);
        return plainQueue != null || termState == 0 ? state : termState;
    }
}
