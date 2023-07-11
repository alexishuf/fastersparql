package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import com.github.alexishuf.fastersparql.util.concurrent.StealingLevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.batch.type.CompressedBatch.MIN_LOCALS;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    private static final int  SMALL_LEVEL_CAP = 32;
    private static final int MEDIUM_LEVEL_CAP = 16;
    private static final int  LARGE_LEVEL_CAP = 8;
    private static final int   HUGE_LEVEL_CAP = 1;
    public static final CompressedBatchType INSTANCE = new CompressedBatchType(
            new StealingLevelPool<>(new LevelPool<>(CompressedBatch.class,
                    SMALL_LEVEL_CAP, MEDIUM_LEVEL_CAP, LARGE_LEVEL_CAP, HUGE_LEVEL_CAP)));

    private final StealingLevelPool<CompressedBatch> pool;

    public static CompressedBatchType get() { return INSTANCE; }

    public CompressedBatchType(StealingLevelPool<CompressedBatch> pool) {
        super(CompressedBatch.class);
        this.pool = pool;
    }

    @Override public CompressedBatch create(int rows, int cols, int bytes) {
        var b = pool.getAtLeast(rows*((cols+1)<<1));
        if (b == null)
            return new CompressedBatch(rows, cols, bytes == 0 ? rows* MIN_LOCALS : bytes);
        b.unmarkPooled();
        b.hydrate(rows, cols, bytes);
        return b;
    }

    @Override public @Nullable CompressedBatch recycle(@Nullable CompressedBatch b) {
        if (b == null) return null;
        b.markPooled();
        if (pool.offer(b, b.rowsCapacity() * (b.cols + 1 << 1)) != null)
            b.recycleInternals();
        return null;
    }

    @Override public int bytesRequired(Batch<?> b) {
        if (b instanceof CompressedBatch cb) return cb.bytesUsed();
        int required = 0;
        for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
            for (int c = 0; c < cols; c++) required += b.localLen(r, c);
        }
        return required;
    }

    @Override public RowBucket<CompressedBatch> createBucket(int rowsCapacity, int cols) {
        return new CompressedRowBucket(rowsCapacity, cols);
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

    @Override
    public Filter filter(Vars out, Vars in, RowFilter<CompressedBatch> filter,
                         BatchFilter<CompressedBatch> before) {
        return new Filter(this, out, projector(out, in), filter, before);
    }

    @Override
    public Filter filter(Vars vars, RowFilter<CompressedBatch> filter,
                         BatchFilter<CompressedBatch> before) {
        return new Filter(this, vars, null, filter, before);
    }
}
