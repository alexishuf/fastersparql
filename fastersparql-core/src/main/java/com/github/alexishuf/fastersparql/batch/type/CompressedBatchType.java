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

    static {
        int offers = Runtime.getRuntime().availableProcessors();
        int hugeOffers = Math.min(DEF_HUGE_LEVEL_CAPACITY, offers);
        for (int terms = 1; terms < LARGE_MAX_CAPACITY; terms<<=1) {
            for (int i = 0; i < offers; i++) {
                var b = new CompressedBatch(terms, 1);
                b.markPooled();
                INSTANCE.pool.offer(b, terms);
            }
            for (int i = 0; i < hugeOffers; i++) {
                var b = new CompressedBatch(terms, 1);
                b.markPooled();
                INSTANCE.pool.offer(b, terms);
            }
        }
    }

    private CompressedBatchType() {super(CompressedBatch.class);}

    public static CompressedBatchType get() { return INSTANCE; }

    @Override public CompressedBatch create(int rows, int cols) {
        return createForTerms(rows > 0 ? rows*cols : cols, cols);
    }

    @Override public CompressedBatch createForTerms(int terms, int cols) {
        CompressedBatch b = pool.getAtLeast(terms);
        if (b == null)
            return new CompressedBatch(terms, cols);
        BatchEvent.Unpooled.record(b);
        return b.clear(cols).markUnpooled();
    }

    @Override
    public CompressedBatch empty(@Nullable CompressedBatch offer, int rows, int cols) {
        return emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols);
    }

    @Override
    public CompressedBatch emptyForTerms(@Nullable CompressedBatch offer, int terms, int cols) {
        var b = offer == null ? null : terms > offer.termsCapacity() ? recycle(offer) : offer;
        if (b == null && (b = pool.getAtLeast(terms)) == null)
            return new CompressedBatch(terms, cols);
        b.clear(cols);
        if (b != offer)
            BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    @Override
    public CompressedBatch withCapacity(@Nullable CompressedBatch offer, int rows, int cols) {
        if (offer      == null) return createForTerms(rows > 0 ? rows*cols : cols, cols);
        if (offer.cols != cols) throw new IllegalArgumentException("offer.cols != cols");
        return offer.withCapacity(rows);
    }

    //    @Override
//    public CompressedBatch reserved(@Nullable CompressedBatch offer, int rows, int cols, int localBytes) {
//        int terms = rows*cols;
//        if (offer != null) {
//            if (offer.hasCapacity(terms, localBytes)) {
//                offer.clear(cols);
//                return offer;
//            } else {
//                recycle(offer);
//            }
//        }
//        int capacity = 12*terms;
//        var b = pool.getAtLeast(capacity>>POOL_SHIFT);
//        if (b == null)
//            return new CompressedBatch(rows, cols, localBytes);
//        BatchEvent.Unpooled.record(capacity);
//        return b.clearAndReserveAndUnpool(terms, cols, localBytes);
//    }

    @Override public @Nullable CompressedBatch recycle(@Nullable CompressedBatch b) {
        if (b == null)
            return null;
        BatchEvent.Pooled.record(b.markPooled());
        if (pool.offerToNearest(b, b.termsCapacity()) != null)
            b.markGarbage();
        return null;
    }

    @Override public int localBytesRequired(Batch<?> b) {
        if (b instanceof CompressedBatch cb)
            return cb.localBytesUsed();
        return coldBytesRequired(b);
    }

    private static int coldBytesRequired(Batch<?> b) {
        if (b.rows == 0) return 0;
        int sum = 0;
        for (int c = 0, cols = b.cols; c < cols; c++)
            sum += b.uncheckedLocalLen(0, c);
        return b.rows * ((sum &~3) + 4);
    }

    @Override public <O extends Batch<O>> CompressedBatch convert(O src) {
        var cb = create(src.rows, src.cols);
        cb.reserveAddLocals(localBytesRequired(src));
        return cb.putConverting(src);
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
