package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.expr.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public final class TermBatchType extends BatchType<TermBatch> {
    public static final TermBatchType INSTANCE = new TermBatchType();

    @SuppressWarnings("SameReturnValue") public static TermBatchType get() { return INSTANCE; }

    private TermBatchType() {super(TermBatch.class);}

    @Override public TermBatch create(int rows, int cols) {
        return createForTerms(rows > 0 ? rows*cols : cols, cols);
    }

    @Override public TermBatch createForTerms(int terms, int cols) {
        TermBatch b = pool.getAtLeast(terms);
        if (b == null)
            return new TermBatch(new Term[terms], 0, cols);
        BatchEvent.Unpooled.record(b);
        return b.clear(cols).markUnpooled();
    }

    @Override
    public TermBatch empty(@Nullable TermBatch offer, int rows, int cols) {
        return emptyForTerms(offer, rows > 0 ? rows*cols : cols, cols);
    }

    @Override
    public TermBatch emptyForTerms(@Nullable TermBatch offer, int terms, int cols) {
        var b = offer == null ? null : offer.hasCapacity(terms, 0) ? offer : recycle(offer);
        if (b == null && (b = pool.getAtLeast(terms)) == null)
            return new TermBatch(new Term[terms], 0, cols);
        b.clear(cols);
        if (b != offer)
            BatchEvent.Unpooled.record(b.markUnpooled());
        return b;
    }

    @Override public TermBatch withCapacity(@Nullable TermBatch offer, int rows, int cols) {
        if (offer      == null) return createForTerms(rows > 0 ? rows*cols : cols, cols);
        if (offer.cols != cols) throw new IllegalArgumentException("offer.cols != cols");
        return offer.withCapacity(rows);
    }

    //    @Override
//    public TermBatch reserved(@Nullable TermBatch offer, int rows, int cols, int localBytes) {
//        int capacity = rows*cols;
//        if (offer != null) {
//            offer.clear(cols);
//            if (offer.arr.length >= capacity)
//                return offer;
//            recycle(offer);
//        }
//        var b = pool.getAtLeast(capacity);
//        if (b != null) {
//            if (b.arr.length < capacity)
//                b.arr = new Term[Math.min(capacity, b.arr.length<<1)];
//            b.clear(cols);
//            b.unmarkPooled();
//            return b;
//        }
//        return new TermBatch(capacity, cols);

//    }

    @Override public @Nullable TermBatch recycle(@Nullable TermBatch b) {
        if (b == null)
            return null;
        Arrays.fill(b.arr, null); // allow collection of Terms
        BatchEvent.Pooled.record(b.markPooled());
        if (pool.offerToNearest(b, b.arr.length) != null)
            b.markGarbage();
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
