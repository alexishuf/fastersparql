package com.github.alexishuf.fastersparql.batch.type;

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
            new LevelPool<>(TermBatch.class));

    private final LevelPool<TermBatch> pool;

    public static TermBatchType get() { return INSTANCE; }

    public TermBatchType(LevelPool<TermBatch> pool) {
        super(TermBatch.class);
        this.pool = pool;
    }

    @Override public TermBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        TermBatch b = pool.getAtLeast(rowsCapacity);
        if (b == null)
            return new TermBatch(rowsCapacity, cols);
        b.unmarkPooled();
        b.clear(cols);
        return b;
    }

    @Override public @Nullable TermBatch recycle(@Nullable TermBatch batch) {
        if (batch == null) return null;
        Arrays.fill(batch.arr, null); // allow collection of Terms
        batch.rows = 0;
        batch.markPooled();
        if (pool.offer(batch, batch.rowsCapacity()) != null)
            batch.markGC();
        return null;
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

}
