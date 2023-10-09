package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.DEF_HUGE_LEVEL_CAPACITY;
import static com.github.alexishuf.fastersparql.util.concurrent.LevelPool.LARGE_MAX_CAPACITY;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    public static final CompressedBatchType INSTANCE = new CompressedBatchType();
    /**
     * A {@code (1, 1)} batch will report a {@link CompressedBatch#directBytesCapacity()} {@code == 12}.
     * Shifting the bytes capacity by this ammount will reduce the value so that the tiny-capacity
     * pool levels do get used.
     */
    private static final int POOL_SHIFT = 3;

    static {
        int offers = Runtime.getRuntime().availableProcessors();
        int hugeOffers = Math.min(DEF_HUGE_LEVEL_CAPACITY, offers);
        for (int cap = 1; cap < LARGE_MAX_CAPACITY; cap<<=1) {
            int terms = cap<<POOL_SHIFT;
            for (int i = 0; i < offers; i++) {
                var b = new CompressedBatch(terms, 1, terms * 16);
                b.markPooled();
                INSTANCE.pool.offer(b, cap);
            }
            for (int i = 0; i < hugeOffers; i++) {
                var b = new CompressedBatch(terms, 1, terms * 16);
                b.markPooled();
                INSTANCE.pool.offer(b, cap);
            }
        }
    }

    private CompressedBatchType() {super(CompressedBatch.class);}

    public static CompressedBatchType get() { return INSTANCE; }

    @Override public CompressedBatch create(int rows, int cols, int bytes) {
        int terms = rows*cols, capacity = 12*terms;
        if (bytes == 0)
            bytes = capacity;
        var b = pool.getAtLeast(capacity>>POOL_SHIFT);
        if (b == null)
            return new CompressedBatch(rows, cols, bytes);
        BatchEvent.Unpooled.record(capacity);
        return b.clearAndUnpool(cols);
    }

    @Override public CompressedBatch poll(int rows, int cols, int bytes) {
        int terms = rows*cols, capacity = 12*terms;
        var b = pool.getAtLeast(capacity>>POOL_SHIFT);
        if (b != null) {
            if (b.hasTermsCapacity(terms)) {
                BatchEvent.Unpooled.record(capacity);
                return b.clearAndReserveAndUnpool(terms, cols, bytes);
            }
            if (pool.shared.offerToNearest(b, b.directBytesCapacity()) != null)
                b.markGarbage();
        }
        return null;
    }

    @Override
    public CompressedBatch empty(@Nullable CompressedBatch offer, int rows, int cols, int localBytes) {
        if (offer != null) {
            offer.clear(cols);
            return offer;
        }
        return create(rows, cols, localBytes);
    }

    @Override
    public CompressedBatch reserved(@Nullable CompressedBatch offer, int rows, int cols, int localBytes) {
        int terms = rows*cols;
        if (offer != null) {
            if (offer.hasCapacity(terms, localBytes)) {
                offer.clear(cols);
                return offer;
            } else {
                recycle(offer);
            }
        }
        int capacity = 12*terms;
        var b = pool.getAtLeast(capacity>>POOL_SHIFT);
        if (b == null)
            return new CompressedBatch(rows, cols, localBytes);
        BatchEvent.Unpooled.record(capacity);
        return b.clearAndReserveAndUnpool(terms, cols, localBytes);
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

    @Override public int localBytesRequired(Batch<?> b, int row) {
        if (b instanceof CompressedBatch cb)
            return (int)cb.rowOffsetAndLen(row);
        return coldBytesRequired(b, row);
    }

    @Override public int localBytesRequired(Batch<?> b) {
        if (b instanceof CompressedBatch cb)
            return cb.localBytesUsed();
        return coldBytesRequired(b);
    }

    private static int coldBytesRequired(Batch<?> b, int row) {
        int sum = 0;
        if (row < 0 || row  >= b.rows)
            return 0;
        for (int c = 0, cols = b.cols; c < cols; c++)
            sum += b.uncheckedLocalLen(row, c);
        return sum;
    }

    private static int coldBytesRequired(Batch<?> b) {
        return b.rows * ((coldBytesRequired(b, 0)&~3) + 4);
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
