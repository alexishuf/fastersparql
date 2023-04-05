package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;

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
    public @Nullable BatchMerger<TermBatch> projector(Vars out, Vars in) {
        int[] sources = mergerSources(out, in, Vars.EMPTY);
        return sources == null ? null : new TermBatch.Merger(this, out, sources);
    }

    @Override
    public @Nullable BatchMerger<TermBatch> merger(Vars out, Vars left, Vars right) {
        int[] sources = mergerSources(out, left, right);
        return sources == null ? null : new TermBatch.Merger(this, out, sources);
    }

    @Override
    public BatchFilter<TermBatch> filter(Vars out, Vars in, RowFilter<TermBatch> filter) {
        return new TermBatch.Filter(this, projector(out, in), filter);
    }

    @Override public BatchFilter<TermBatch> filter(RowFilter<TermBatch> filter) {
        return new TermBatch.Filter(this, null, filter);
    }

    @Override public String toString() { return "TermBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof TermBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }
}
