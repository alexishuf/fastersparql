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

    @Override public TermBatch create(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity * cols;
        TermBatch b = pool.getAtLeast(capacity);
        if (b == null)
            return new TermBatch(rowsCapacity, cols);
        BatchEvent.Unpooled.record(capacity);
        b.clear(cols);
        b.unmarkPooled();
        return b;
    }

    @Override
    public TermBatch empty(@Nullable TermBatch offer, int rows, int cols, int localBytes) {
        if (offer != null) {
            offer.clear(cols);
            return offer;
        }
        return create(rows, cols, 0);
    }

    @Override
    public TermBatch reserved(@Nullable TermBatch offer, int rows, int cols, int localBytes) {
        int capacity = rows*cols;
        if (offer != null) {
            offer.clear(cols);
            if (offer.arr.length >= capacity)
                return offer;
            recycle(offer);
        }
        var b = pool.getAtLeast(capacity);
        if (b != null) {
            if (b.arr.length < capacity)
                b.arr = new Term[Math.min(capacity, b.arr.length<<1)];
            b.clear(cols);
            b.unmarkPooled();
            return b;
        }
        return new TermBatch(capacity, cols);
    }

    @Override public @Nullable TermBatch poll(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity * cols;
        var b = pool.getAtLeast(capacity);
        if (b != null) {
            if (capacity <= b.arr.length) {
                BatchEvent.Unpooled.record(capacity);
                b.clear(cols);
                b.unmarkPooled();
                return b;
            } else {
                if (pool.shared.offerToNearest(b, capacity) != null)
                    b.markGarbage();
            }
        }
        return null;
    }

    @Override public @Nullable TermBatch recycle(@Nullable TermBatch batch) {
        if (batch == null) return null;
        Arrays.fill(batch.arr, null); // allow collection of Terms
        batch.rows = 0;
        batch.markPooled();
        int capacity = batch.directBytesCapacity();
        if (pool.offerToNearest(batch, capacity) == null) BatchEvent. Pooled.record(capacity);
        else                                              BatchEvent.Garbage.record(capacity);
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
