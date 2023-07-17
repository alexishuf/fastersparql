package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public class HdtBatchType extends BatchType<HdtBatch> {
    /**
     * A batch with 1 row and 1 column will report/require a
     * {@link HdtBatch#directBytesCapacity()} of 12. Applying {@code >> POOL_SHIFT}, turns
     * ensures that the lower pool levels also get used.
     */
    private static final int POOL_SHIFT = 3;
    public static final HdtBatchType INSTANCE = new HdtBatchType();

    public HdtBatchType() {super(HdtBatch.class);}

    @Override public HdtBatch create(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity * cols;
        HdtBatch b = pool.getAtLeast(capacity);
        if (b == null)
            return new HdtBatch(rowsCapacity, cols);
        b.unmarkPooled();
        b.clear(cols);
        BatchEvent.Unpooled.record(capacity<<POOL_SHIFT);
        return b;
    }

    @Override public @Nullable HdtBatch recycle(@Nullable HdtBatch batch) {
        if (batch == null) return null;
        batch.markPooled();
        int capacity = batch.directBytesCapacity();
        if (pool.offerToNearest(batch, capacity>>POOL_SHIFT) == null) {
            BatchEvent.Pooled.record(capacity);
        } else {
            batch.recycleInternals();
            BatchEvent.Garbage.record(capacity);
        }
        return null;
    }

    public <O extends Batch<O>> BIt<HdtBatch> convert(BIt<O> other, int dictId) {
        if (equals(other.batchType())) //noinspection unchecked
            return (BIt<HdtBatch>) other;
        return new HdtBatchConverterBIt<>(other, this, dictId);
    }

    private static class HdtBatchConverterBIt<S extends Batch<S>>
            extends IdConverterBIt<HdtBatch, S> {
        private final int dictId;

        public HdtBatchConverterBIt(BIt<S> delegate, BatchType<HdtBatch> batchType,
                                    int dictId) {
            super(delegate, batchType);
            this.dictId = dictId;
        }

        @Override protected HdtBatch putConverting(HdtBatch out, S in) {
            return out.putConverting(in, dictId);
        }
    }

    @Override public RowBucket<HdtBatch> createBucket(int rowsCapacity, int cols) {
        return new HdtBatchBucket(rowsCapacity, cols);
    }

    @Override public @Nullable Merger<HdtBatch> projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger<>(this, out, sources);
    }

    @Override public @NonNull Merger<HdtBatch> merger(Vars out, Vars left, Vars right) {
        return new Merger<>(this, out, mergerSources(out, left, right));
    }

    @Override public Filter<HdtBatch> filter(Vars out, Vars in, RowFilter<HdtBatch> filter,
                                             BatchFilter<HdtBatch> before) {
        return new Filter<>(this, out, projector(out, in), filter, before);
    }

    @Override public Filter<HdtBatch> filter(Vars vars, RowFilter<HdtBatch> filter,
                                             BatchFilter<HdtBatch> before) {
        return new Filter<>(this, vars, null, filter, before);
    }

}
