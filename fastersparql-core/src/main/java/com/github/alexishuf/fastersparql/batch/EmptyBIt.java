package com.github.alexishuf.fastersparql.batch;

import com.github.alexishuf.fastersparql.batch.base.AbstractBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch;
import com.github.alexishuf.fastersparql.batch.type.TermBatch;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.store.batch.StoreBatch;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.returnsreceiver.qual.This;

public final class EmptyBIt<B extends Batch<B>> extends AbstractBIt<B> {
    private static final EmptyBIt<TermBatch> TERM_EMPTY             = new EmptyBIt<>(Batch.TERM, Vars.EMPTY);
    private static final EmptyBIt<CompressedBatch> COMPRESSED_EMPTY = new EmptyBIt<>(Batch.COMPRESSED, Vars.EMPTY);
    private static final EmptyBIt<StoreBatch> STORE_EMPTY           = new EmptyBIt<>(StoreBatch.TYPE, Vars.EMPTY);

    public EmptyBIt(BatchType<B> batchType, Vars vars) { super(batchType, vars); }

    @SuppressWarnings("unchecked")
    public static <B extends Batch<B>> EmptyBIt<B> of(BatchType<B> type) {
        if      (type ==       Batch.TERM) return (EmptyBIt<B>) TERM_EMPTY;
        else if (type == Batch.COMPRESSED) return (EmptyBIt<B>) COMPRESSED_EMPTY;
        else if (type ==  StoreBatch.TYPE) return (EmptyBIt<B>) STORE_EMPTY;
        else                               return new EmptyBIt<>(type, Vars.EMPTY);
    }

    @Override public B            nextBatch(@Nullable B b) {
        batchType.recycle(b);
        if (!terminated)
            onTermination(null);
        return null;
    }
    @Override public @This BIt<B> tempEager() { return this; }
    @Override public String        toString() { return "EMPTY"; }
}
