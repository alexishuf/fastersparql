package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public final class TermBatchType extends BatchType<TermBatch> {
    public static final TermBatchType INSTANCE = new TermBatchType(
            new LevelPool<>(TermBatch.class, 32, 8*BIt.PREFERRED_MIN_BATCH));

    public static TermBatchType get() { return INSTANCE; }

    public TermBatchType(LevelPool<TermBatch> pool) {
        super(TermBatch.class, pool);
    }

    @Override public TermBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        TermBatch b = pool.get(rowsCapacity);
        if (b == null)
            return new TermBatch(rowsCapacity, cols);
        b.clear(cols);
//        b.reserve(rowsCapacity, bytesCapacity);
        return b;
    }

    @Override public @Nullable TermBatch recycle(@Nullable TermBatch batch) {
        if (batch == null) return null;
        Arrays.fill(batch.arr, null); // allow collection of Terms
        batch.rows = 0;
        pool.offer(batch, batch.rowsCapacity());
        return null; // fill(null) forces us to take ownership even the pool rejected.
    }

    @Override public RowBucket<TermBatch> createBucket(int rowsCapacity, int cols) {
        return new TermBatchBucket(rowsCapacity, cols);
    }

    @Override
    public @Nullable Merger projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger(this, out, sources);
    }

    @Override
    public @NonNull Merger merger(Vars out, Vars left, Vars right) {
        return new Merger(this, out, mergerSources(out, left, right));
    }

    @Override public Filter filter(Vars out, Vars in, RowFilter<TermBatch> filter,
                                   BatchFilter<TermBatch> before) {
        return new Filter(this, out, projector(out, in), filter, before);
    }

    @Override public Filter filter(Vars vars, RowFilter<TermBatch> filter,
                                   BatchFilter<TermBatch> before) {
        return new Filter(this, vars, null, filter, before);
    }

    @Override public String toString() { return "TermBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof TermBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }
}
