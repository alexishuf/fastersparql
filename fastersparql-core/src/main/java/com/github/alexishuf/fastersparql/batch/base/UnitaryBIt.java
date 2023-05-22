package com.github.alexishuf.fastersparql.batch.base;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Timestamp;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.checkerframework.common.returnsreceiver.qual.This;

/** Implements {@link BIt} methods around {@code hasNext()/next()}. */
public abstract class UnitaryBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private @Nullable Throwable pendingError;
    protected long fillingStart = ORIGIN;
//    private DebugJournal.RoleJournal journal;

    public UnitaryBIt(BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        minWaitNs = QUICK_MIN_WAIT_NS;
        //journal = DebugJournal.SHARED.role(toStringNoArgs());
    }

    /* --- --- --- helpers --- --- --- */

    /**
     * {@link Batch#beginPut()}/{@link Batch#commitPut()} at most one row to {@code this.filling}.
     *
     * @return {@code true} iff a row was added and {@code false} iff the iterator has reached
     *         its end.
     */
    protected abstract boolean fetch(B dest) throws Exception;

    @RequiresNonNull("pendingError")
    private void throwPending() {
        RuntimeException e = pendingError instanceof RuntimeException r
                           ? r : new RuntimeException(pendingError);
        onTermination(e);
        pendingError = null;
        throw e;
    }

    /* --- --- --- implementations --- --- --- */

    @Override public @This BIt<B> tempEager() {
        eager = true;
        return this;
    }

    @Override public @Nullable B nextBatch(@Nullable B b) {
        if (pendingError != null)
            throwPending();
        //journal.write("UBIt.nextBatch: &offer=", System.identityHashCode(b));
        b = getBatch(b);
        //journal.write("UBIt.nextBatch: &b=", System.identityHashCode(b));
        long start = fillingStart;
        if (needsStartTime && start == ORIGIN)
            fillingStart = start = Timestamp.nanoTime();
        try {//noinspection StatementWithEmptyBody
            while (fetch(b) && readyInNanos(b.rows, start) > 0) {}
        } catch (Throwable t) { pendingError = t; }
        fillingStart = ORIGIN;
        if (b.rows == 0) {
            batchType.recycle(b);
            if (pendingError != null) throwPending();
            else                      onTermination(null);
            return null;
        }
        onBatch(b);
        //journal.write("UBIt.nextBatch: return &b=", System.identityHashCode(b), "rows=", b.rows, "[0][0]=", b.get(0, 0));
        return b;
    }
}
