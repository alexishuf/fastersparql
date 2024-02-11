package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;
import static com.github.alexishuf.fastersparql.util.concurrent.ThreadJournal.journal;

public abstract class CallbackEmitter<B extends Batch<B>> extends TaskEmitter<B>
        implements CompletableBatchQueue<B> {
    private @Nullable B ready, filling;
    private int avgRows;

    public CallbackEmitter(BatchType<B> batchType, Vars vars, EmitterService runner, int worker,
                           int initState, Flags flags) {
        super(batchType, vars, runner, worker, initState, flags);
    }

    @Override protected void doRelease() {
        try {
            ready   = bt.recycle(ready);
            filling = bt.recycle(filling);
        } finally {
            super.doRelease();
        }
    }

    /**
     * Causes calls to {@link #offer(Batch)} and to stop sometime after entry into this
     * method. This method undoes and is undone by {@link #resume()}
     */
    protected abstract void pause();

    /**
     * Causes calls to {@link #offer(Batch)} and to resume at some point after the entry
     * into this method. This method undoes and is undone by {@link #pause()}
     */
    protected abstract void resume();

    @Override public @Nullable Throwable error() {
        return (state()&~IS_TERM_DELIVERED) == FAILED ? error : null;
    }

    protected int complete2state(int current, @Nullable Throwable cause) {
        if (cause != null) {
            if (error == UNSET_ERROR) error = cause;
            return PENDING_FAILED;
        } else if ((current&IS_CANCEL_REQ) != 0) {
            return PENDING_CANCELLED;
        } else {
            return PENDING_COMPLETED;
        }
    }

    @Override public boolean complete(@Nullable Throwable cause) {
        int st = state(), tgt = complete2state(st, cause);
        if (tgt == 0) {
            journal("ignoring (for retry?) complete", cause, "on", this);
        } else if (moveStateRelease(st, tgt)) {
            awake();
            return true;
        } else {
            journal("move to ", tgt, flags, "from", statePlain(), flags, "rejected for", this);
        }
        return false;
    }

    @Override public boolean cancel(boolean ack) { return cancel(); }

    @Override public B fillingBatch() {
        int st = lock(statePlain());
        try {
            B f = filling;
            if (f == null) f = bt.createForThread(threadId, outCols);
            else           filling = null;
            return f;
        } finally { unlock(st); }
    }

    public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        if (b == null || b.rows == 0)
            return b;
        b.requireUnpooled();
        int st = lock(statePlain());
        try {
            if ((st&(IS_TERM|IS_CANCEL_REQ)) == 0) {
                if (ready == null) ready = b;
                else               filling = Batch.quickAppend(filling, b);
            } else {
                bt.recycle(b);
                if (isCancelled(st) || (st&IS_CANCEL_REQ)!=0) throw CancelledException.INSTANCE;
                else                                          throw TerminatedException.INSTANCE;
            }
        } finally {
            unlock(st);
        }
        awake();
        return null;
    }

    @Override protected void task(int threadId) {
        this.threadId = (short)threadId;
        int st = lock(state()), termState = 0;
        try {
            if ((st&(IS_TERM_DELIVERED|IS_INIT)) != 0)
                return; // no work to do
            long deadline = Timestamp.nextTick(1);
            while (ready != null) {
                B b     = ready;
                ready   = filling;
                filling = null;
                st = unlock(st);
                avgRows = ((avgRows<<4) - avgRows + b.rows) >> 4;
                if (b.rows > 0 && (b = deliver(b)) != null)
                    b.recycle();
                if (Timestamp.nanoTime() > deadline) {
                    awake();
                    break;
                }
                st = lock(st);
            }
            if (requested() <= 0 && (st&IS_LIVE) != 0)
                pause();
            if (ready == null) {
                assert filling == null : "ready == null but filling != null";
                termState =  (st&IS_CANCEL_REQ)   != 0 ? CANCELLED
                              : ((st&IS_PENDING_TERM) != 0 ? (st&~IS_PENDING_TERM)|IS_TERM : 0);
                if (termState != 0) {
                    if ((st&LOCKED_MASK) != 0)
                        st = unlock(st);
                    deliverTermination(st, termState);
                }
            }
        } catch (Throwable t) {
            if (termState != 0 || (st&IS_TERM) != 0)
                throw t;
            if ((st&LOCKED_MASK) != 0)
                st = unlock(st);
            if (error == UNSET_ERROR)
                error = t;
            deliverTermination(st, FAILED);
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock(st);
        }
    }

    @Override protected int produceAndDeliver(int state) {
        throw new UnsupportedOperationException();
    }
}
