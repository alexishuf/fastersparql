package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    /**
     * Offered and required {@link Batch#directBytesCapacity()} should be shifted left by this
     * constant when determining the level in the {@link LevelPool}. A batch with 1 row and 1
     * column with an 8-byte local segment in its only term would already report/require a
     * capacity of 24 bytes.
     */
    private static final int POOL_SHIFT = 4;
    private static final int MIN_TERM_LOCAL_SHIFT = 3;
    public static final CompressedBatchType INSTANCE = new CompressedBatchType();

    public CompressedBatchType() {super(CompressedBatch.class);}

    public static CompressedBatchType get() { return INSTANCE; }

    @Override public CompressedBatch create(int rows, int cols, int bytes) {
        int terms = rows*cols;
        terms += terms-1 >>> 31; // branch-less Math.max(terms, 1)
        bytes = Math.max(bytes, terms<<MIN_TERM_LOCAL_SHIFT);
        int capacity = terms*12 + (rows<<2) + bytes;

        var b = pool.getAtLeast(capacity>>POOL_SHIFT);
        if (b == null)
            return new CompressedBatch(rows, cols, bytes);
        b.unmarkPooled();
        b.hydrate(rows, cols, bytes);
        BatchEvent.Unpooled.record(capacity);
        return b;
    }

    @Override public @Nullable CompressedBatch recycle(@Nullable CompressedBatch b) {
        if (b == null) return null;
        b.markPooled();
        int capacity = b.directBytesCapacity();
        if (pool.offerToNearest(b, capacity>>POOL_SHIFT) == null) {
            BatchEvent.Pooled.record(capacity);
        } else {
            b.recycleInternals();
            BatchEvent.Garbage.record(capacity);
        }
        return null;
    }

    @Override public int localBytesRequired(Batch<?> b) {
        if (b instanceof CompressedBatch cb)
            return cb.bytesUsed();
        return coldBytesRequired(b);
    }

    private static int coldBytesRequired(Batch<?> b) {
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
