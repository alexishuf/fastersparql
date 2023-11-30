package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.emit.AbstractStage;
import com.github.alexishuf.fastersparql.emit.EmitterStats;
import com.github.alexishuf.fastersparql.emit.Receiver;
import com.github.alexishuf.fastersparql.emit.exceptions.RebindException;
import com.github.alexishuf.fastersparql.emit.exceptions.RegisterAfterStartException;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.ENABLED;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public class ScatterStage<B extends Batch<B>> extends AbstractStage<B, B> {
    private static final VarHandle SUBSCRIBE_LOCK;
    static {
        try {
            SUBSCRIBE_LOCK = MethodHandles.lookup().findVarHandle(ScatterStage.class, "plainSubscribeLock", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private int extraDownstreamCount;
    @SuppressWarnings("unchecked")
    private Receiver<B>[] extraDownstream = new Receiver[10];
    @SuppressWarnings("unused")
    private int plainSubscribeLock;
    private boolean started;
    private byte delayRelease;
    private int lastRebindSeq = -1;

    public ScatterStage(BatchType<B> batchType, Vars vars) { super(batchType, vars); }

    @Override public void subscribe(Receiver<B> receiver) {
        while ((int)SUBSCRIBE_LOCK.compareAndExchangeAcquire(this, 0, 1) != 0) Thread.onSpinWait();
        try {
            if (started)
                throw new RegisterAfterStartException(this);
            if (downstream == null || downstream == receiver) {
                downstream = receiver;
            } else {
                for (int i = 0; i < extraDownstreamCount; i++) {
                    if (extraDownstream[i] == receiver) return;
                }
                if (extraDownstreamCount == extraDownstream.length)
                    extraDownstream = Arrays.copyOf(extraDownstream, extraDownstream.length*2);
                extraDownstream[extraDownstreamCount++] = receiver;
            }
            if (ENABLED)
                journal("subscribed", receiver, "to", this);
        } finally { SUBSCRIBE_LOCK.setRelease(this, 0); }
    }

    @Override public void rebind(BatchBinding binding) throws RebindException {
        started = false;
        if (binding.sequence == lastRebindSeq)
            return; //duplicate rebind() due to diamond in emitters graph
        lastRebindSeq = binding.sequence;
        super.rebind(binding);
    }

    @Override public void rebindAcquire() {
        super.rebindAcquire();
        delayRelease = (byte)Math.min(0xff, (0xff&delayRelease)+1);
    }

    @Override public void rebindRelease() {
        super.rebindRelease();
        delayRelease = (byte)Math.max(0, (0xff&delayRelease)-1);
    }

    @Override public void request(long rows) {
        started = true;
        super.request(rows);
    }

    @Override public @Nullable B onBatch(B batch) {
        if (batch.rows == 1 && batch.next == null) {
            onRow(batch, 0);
            return batch;
        }
        if (EmitterStats.ENABLED && stats != null)
            stats.onBatchPassThrough(batch);
        if (downstream == null)
            throw new NoReceiverException();
        B copy = null;
        for (int i = 0; i < extraDownstreamCount; i++)
            copy = extraDownstream[i].onBatch(copy == null ? batch.dup() : copy);
        batchType.recycle(copy);
        return downstream.onBatch(batch);
    }

    @Override public void onRow(B batch, int row) {
        if (EmitterStats.ENABLED && stats != null)
            stats.onRowPassThrough();
        if (downstream == null)
            throw new NoReceiverException();
        downstream.onRow(batch, row);
        for (int i = 0; i < extraDownstreamCount; i++)
            extraDownstream[i].onRow(batch, row);
    }

    @Override public void onComplete() {
        if (downstream == null)
            throw new NoReceiverException();
        downstream.onComplete();
        for (int i = 0; i < extraDownstreamCount; i++)
            extraDownstream[i].onComplete();
    }

    @Override public void onCancelled() {
        if (downstream == null)
            throw new NoReceiverException();
        downstream.onCancelled();
        for (int i = 0; i < extraDownstreamCount; i++)
            extraDownstream[i].onCancelled();
    }

    @Override public void onError(Throwable cause) {
        if (downstream == null)
            throw new NoReceiverException();
        downstream.onError(cause);
        for (int i = 0; i < extraDownstreamCount; i++)
            extraDownstream[i].onError(cause);
    }
}
