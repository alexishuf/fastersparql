package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.FSProperties.DEF_OP_REDUCED_CAPACITY;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    private static final int SINGLETON_CAPACITY = 8 * DEF_OP_REDUCED_CAPACITY;
    public static final CompressedBatchType INSTANCE = new CompressedBatchType(
            new LevelPool<>(CompressedBatch.class, 32, SINGLETON_CAPACITY));

    public static CompressedBatchType get() { return INSTANCE; }

    public CompressedBatchType(LevelPool<CompressedBatch> pool) {
        super(CompressedBatch.class, pool);
    }

    @Override public CompressedBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        var b = pool.get(rowsCapacity);
        if (b == null)
            return new CompressedBatch(rowsCapacity, cols, bytesCapacity);
        b.clear(cols);
//        b.reserve(rowsCapacity, bytesCapacity);
        return b;
    }

    @Override public int bytesRequired(Batch<?> b) {
        if (b instanceof CompressedBatch cb) return cb.bytesUsed();
        int required = 0;
        for (int r = 0, rows = b.rows, cols = b.cols; r < rows; r++) {
            int rowBytes = 0;
            for (int c = 0; c < cols; c++) rowBytes += b.localLen(r, c);
            required += CompressedBatch.vecCeil(rowBytes);
        }
        return required;
    }

    @Override
    public @Nullable BatchMerger<CompressedBatch> projector(Vars out, Vars in) {
        int[] sources = mergerSources(out, in, Vars.EMPTY);
        return sources == null ? null : new CompressedBatch.Merger(this, out, sources);
    }

    @Override
    public @Nullable BatchMerger<CompressedBatch> merger(Vars out, Vars left, Vars right) {
        int[] sources = mergerSources(out, left, right);
        return sources == null ? null : new CompressedBatch.Merger(this, out, sources);
    }

    @Override
    public BatchFilter<CompressedBatch> filter(Vars out, Vars in, RowFilter<CompressedBatch> filter) {
        return new CompressedBatch.Filter(this, projector(out, in), filter);
    }

    @Override
    public BatchFilter<CompressedBatch> filter(RowFilter<CompressedBatch> filter) {
        return new CompressedBatch.Filter(this, null, filter);
    }

    @Override public String toString() { return "CompressedBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof CompressedBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }
}
