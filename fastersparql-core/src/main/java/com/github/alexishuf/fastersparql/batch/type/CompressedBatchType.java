package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.AffinityPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    public static final CompressedBatchType INSTANCE = new CompressedBatchType(
            new AffinityPool<>(CompressedBatch.class, 1024));

    private final AffinityPool<CompressedBatch> pool;

    public static CompressedBatchType get() { return INSTANCE; }

    public CompressedBatchType(AffinityPool<CompressedBatch> pool) {
        super(CompressedBatch.class);
        this.pool = pool;
    }

    @Override public CompressedBatch create(int rows, int cols, int bytes) {
        var b = pool.get();
        if (b == null)
            return new CompressedBatch(rows, cols, bytes == 0 ? rows* CompressedBatch.MIN_LOCALS : bytes);
        b.hydrate(rows, cols, bytes);
        return b;
    }

    @Override public @Nullable CompressedBatch recycle(@Nullable CompressedBatch batch) {
        if (batch != null) {
            batch.recycle();
            pool.offer(batch);
        }
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
