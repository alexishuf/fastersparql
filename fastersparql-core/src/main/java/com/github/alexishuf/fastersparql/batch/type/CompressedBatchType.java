package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import jdk.incubator.vector.ByteVector;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.FSProperties.DEF_OP_REDUCED_CAPACITY;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    private static final int SINGLETON_CAPACITY = 8 * DEF_OP_REDUCED_CAPACITY;
    public static final CompressedBatchType INSTANCE = new CompressedBatchType(
            new LevelPool<>(CompressedBatch.class, 32, SINGLETON_CAPACITY));
    private static final int MIN_LOCALS = ByteVector.SPECIES_PREFERRED.length();

    public static CompressedBatchType get() { return INSTANCE; }

    public CompressedBatchType(LevelPool<CompressedBatch> pool) {
        super(CompressedBatch.class, pool);
    }

    @Override public CompressedBatch create(int rows, int cols, int bytes) {
        var b = pool.get(rows);
        if (b == null)
            return new CompressedBatch(rows, cols, bytes == 0 ? rows*MIN_LOCALS : bytes);
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
            required += CompressedBatch.localsCeil(rowBytes);
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

    @Override public String toString() { return "CompressedBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof CompressedBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }
}
