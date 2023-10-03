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
    /** This field will be set to {@code true} by {@link #fetch(Batch)} once the end of
     * the source is met */
    protected boolean exhausted;
    protected long fillingStart = Timestamp.ORIGIN;
//    private DebugJournal.RoleJournal journal;

    public UnitaryBIt(BatchType<B> batchType, Vars vars) {
        super(batchType, vars);
        minWaitNs = QUICK_MIN_WAIT_NS;
        //journal = DebugJournal.SHARED.role(toStringNoArgs());
    }

    /* --- --- --- helpers --- --- --- */

    /**
     * Tries to put rows into {@code dest} with minimal blocking. Implementations should block to
     * ensure that at least one row is appended or {@link #exhausted} becomes {@code true}. If
     * more rows can be appended without further blocking, implementations may also append them to
     * {@code dest}
     *
     * @return {@code dest} or a new batch to replace {@code dest} (see {@link Batch#put(Batch)}
     */
    protected abstract B fetch(B dest);

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
        if (needsStartTime && start == Timestamp.ORIGIN)
            fillingStart = start = Timestamp.nanoTime();
        try {
            while (!exhausted && readyInNanos(b.rows, start) > 0)
                b = fetch(b);
        } catch (Throwable t) { pendingError = t; }
        fillingStart = Timestamp.ORIGIN;
        if (b.rows == 0) {
            batchType.recycle(b);
            if (pendingError != null) throwPending();
            else                      onTermination(null);
            return null;
        }
        onNextBatch(b);
        //journal.write("UBIt.nextBatch: return &b=", System.identityHashCode(b), "rows=", b.rows, "[0][0]=", b.get(0, 0));
        return b;
    }
}
