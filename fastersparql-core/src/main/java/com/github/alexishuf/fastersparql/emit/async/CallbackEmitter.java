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
    private @Nullable B queue;
    private int avgRows;

    public CallbackEmitter(BatchType<B> batchType, Vars vars, EmitterService runner, int worker,
                           int initState, Flags flags) {
        super(batchType, vars, runner, worker, initState, flags);
    }

    @Override public B createBatch() {
        return bt.createForThread(threadId, outCols);
    }

    /**
     * Causes calls to {@link #offer(Batch)} and {@link #putRow(Batch, int)} to stop sometime
     * after entry into this method. This method undoes and is undone by {@link #resume()}
     */
    protected abstract void pause();

    /**
     * Causes calls to {@link #offer(Batch)} and {@link #putRow(Batch, int)} to resume at
     * some point the entry of this method. This method undoes and is undone by {@link #pause()}
     */
    protected abstract void resume();

    @Override public boolean isTerminated() {return (state()&IS_TERM) != 0;}
    @Override public boolean isComplete() {return isCompleted(state());}
    @Override public boolean isCancelled() {return isCancelled(state());}
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

    @Override public void complete(@Nullable Throwable cause) {
        int st = state(), tgt = complete2state(st, cause);
        if (tgt == 0)
            journal("ignoring (for retry?) complete", cause, "on", this);
        else if (moveStateRelease(st, tgt))
            awake();
        else
            journal("move to ", tgt, flags, "from", statePlain(), flags, "rejected for", this);
    }

    public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        if (b == null || b.rows == 0)
            return b;
        b.requireUnpooled();
        int st = lock(statePlain());
        try {
            if ((st&(IS_TERM|IS_CANCEL_REQ)) == 0)
                queue = Batch.quickAppend(queue, b);
            else if (isCancelled(st) || (st&IS_CANCEL_REQ) != 0)
                throw CancelledException.INSTANCE;
            else // if ((st&IS_TERM) != 0)
                throw TerminatedException.INSTANCE;
        } finally {
            unlock(st);
        }
        awake();
        return null;
    }

    public void putRow(B batch, int row) throws TerminatedException, CancelledException {
        int st = lock(statePlain());
        try {
            if ((st&STATE_MASK) == CANCEL_REQUESTED) {
                throw CancelledException.INSTANCE;
            } else if ((st&IS_TERM) != 0) {
                throw TerminatedException.INSTANCE;
            } else {
                B dst = queue;
                if (dst == null) {
                    queue = dst = bt.createForThread(threadId, batch.cols);
                    dst.reserveAddLocals(avgRows*(batch.localBytesUsed()/batch.rows));
                }
                dst.putRow(batch, row);
            }
        } finally {
            unlock(st);
        }
        awake();
    }

    @Override protected void task(int threadId) {
        this.threadId = (short)threadId;
        int st = lock(state()), termState = 0;
        try {
            if ((st&(IS_TERM_DELIVERED|IS_INIT)) != 0)
                return; // no work to do
            long deadline = Timestamp.nextTick(1);
            while (queue != null) {
                B b = queue;
                queue = null;
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
            if (queue == null) {
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
