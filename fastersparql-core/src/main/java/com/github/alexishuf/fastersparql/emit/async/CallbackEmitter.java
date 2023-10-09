package com.github.alexishuf.fastersparql.emit.async;

import com.github.alexishuf.fastersparql.batch.CompletableBatchQueue;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static com.github.alexishuf.fastersparql.util.UnsetError.UNSET_ERROR;

public abstract class CallbackEmitter<B extends Batch<B>> extends TaskEmitter<B>
        implements CompletableBatchQueue<B> {
    private static final VarHandle RECYCLED;
    static {
        try {
            RECYCLED = MethodHandles.lookup().findVarHandle(CallbackEmitter.class, "recycled", Batch.class);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private @Nullable B b0, b1, recycled;
    private int avgRows;

    public CallbackEmitter(BatchType<B> batchType, Vars vars, EmitterService runner, int worker,
                           int initState, Flags flags) {
        super(batchType, vars, runner, worker, initState, flags);
    }

    @Override protected void doRelease() {
        if (recycled != null)
            recycled = recycled.untracedUnmarkPooled().recycle();
        super.doRelease();
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
        if (tgt != 0 && moveStateRelease(st, tgt))
            awake();
    }

    public @Nullable B offer(B b) throws TerminatedException, CancelledException {
        if (b == null || b.rows == 0)
            return b;
        b.requireUnpooled();
        int st = lock(statePlain());
        try {
            if ((st&STATE_MASK) == CANCEL_REQUESTED) {
                throw CancelledException.INSTANCE;
            } else if ((st&IS_TERM) != 0) {
                throw TerminatedException.INSTANCE;
            } else if (b0 == null || b1 == null) {
                if (b0 == null) b0 = b;
                else            b1 = b;
                if ((b = recycled) != null) {
                    b.unmarkPooled();
                    recycled = null;
                }
            } else {
                b1 = b1.put(b, RECYCLED, this);
            }
        } finally {
            unlock(st);
        }
        awake();
        return b;
    }

    public void putRow(B batch, int row) throws TerminatedException, CancelledException {
        int st = lock(statePlain());
        try {
            if ((st&STATE_MASK) == CANCEL_REQUESTED) {
                throw CancelledException.INSTANCE;
            } else if ((st&IS_TERM) != 0) {
                throw TerminatedException.INSTANCE;
            } else {
                B dst;
                if (b1 != null){
                    dst = b1;
                } else if (b0 != null) {
                    dst = b0;
                } else {
                    int bytes = avgRows * (batch.localBytesUsed()/batch.rows);
                    dst = b0 = batchType.create(avgRows, batch.cols, bytes);
                }
                dst.putRow(batch, row);
            }
        } finally {
            unlock(st);
        }
        awake();
    }

    @Override protected void task() {
        int st = lock(state());
        try {
            if ((st&(IS_TERM_DELIVERED|IS_INIT)) != 0)
                return; // no work to do
            long deadline = Timestamp.nextTick(1);
            while (b0 != null) {
                B b = b0;
                b0 = b1;
                b1 = null;
                st = unlock(st);
                avgRows = ((avgRows << 4) - avgRows + b.rows) >> 4;
                if (b.rows > 0 && (b = deliver(b)) != null)
                    b.recycle();
                if (Timestamp.nanoTime() > deadline) {
                    awake();
                    break;
                }
                st = lock(st);
            }
            if (b0 == null) {
                int termState =  (st&IS_CANCEL_REQ)   != 0 ? CANCELLED
                              : ((st&IS_PENDING_TERM) != 0 ? (st&~IS_PENDING_TERM)|IS_TERM : 0);
                if (termState != 0) {
                    if ((st&LOCKED_MASK) != 0)
                        st = unlock(st);
                    deliverTermination(st, termState);
                }
            }
        } finally {
            if ((st&LOCKED_MASK) != 0)
                unlock(st);
        }
    }

    @Override protected int produceAndDeliver(int state) {
        throw new UnsupportedOperationException();
    }
}
