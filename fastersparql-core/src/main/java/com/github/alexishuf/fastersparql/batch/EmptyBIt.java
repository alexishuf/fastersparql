package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import com.github.alexishuf.fastersparql.store.batch.StoreBatchType;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

public final class EmptyBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private static final EmptyBIt<TermBatch> TERM_EMPTY             = new EmptyBIt<>(TermBatchType.TERM, Vars.EMPTY);
    private static final EmptyBIt<CompressedBatch> COMPRESSED_EMPTY = new EmptyBIt<>(CompressedBatchType.COMPRESSED, Vars.EMPTY);
    private static final EmptyBIt<StoreBatch> STORE_EMPTY           = new EmptyBIt<>(StoreBatchType.STORE, Vars.EMPTY);

    static {
        Orphan.recycle(TERM_EMPTY.nextBatch(null));
        Orphan.recycle(COMPRESSED_EMPTY.nextBatch(null));
        Orphan.recycle(STORE_EMPTY.nextBatch(null));
    }

    public EmptyBIt(BatchType<B> batchType, Vars vars) { super(batchType, vars); }
    public EmptyBIt(BatchType<B> batchType, Vars vars, @Nullable MetricsFeeder metrics) {
        super(batchType, vars);
        this.metrics = metrics;
    }

    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> EmptyBIt<B> of(BatchType<B> type) {
        if      (type ==       TermBatchType.TERM)       return (EmptyBIt<B>) TERM_EMPTY;
        else if (type == CompressedBatchType.COMPRESSED) return (EmptyBIt<B>) COMPRESSED_EMPTY;
        else if (type ==      StoreBatchType.STORE)      return (EmptyBIt<B>) STORE_EMPTY;
        else                                             return new EmptyBIt<>(type, Vars.EMPTY);
    }

    @Override public Orphan<B> nextBatch(@Nullable Orphan<B> b) {
        Orphan.recycle(b);
        lock();
        try {
            if (state() == State.ACTIVE)
                onTermination(null);
            return null;
        } finally { unlock(); }
    }
    @Override public @This BIt<B> tempEager() { return this; }
    @Override public String        toString() { return "EMPTY"; }
}
