package com.github.alexishuf.fastersparql.store.batch;

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

public class StoreBatchType extends BatchType<StoreBatch> {
    /**
     * A batch with 1 row and 1 column will report/require a
     * {@link StoreBatch#directBytesCapacity()} of 12. Applying {@code >> POOL_SHIFT}, turns
     * ensures that the lower pool levels also get used.
     */
    private static final int POOL_SHIFT = 3;
    public static final StoreBatchType INSTANCE = new StoreBatchType();

    public StoreBatchType() {super(StoreBatch.class);}

    @Override public StoreBatch create(int rowsCapacity, int cols, int localBytes) {
        int capacity = rowsCapacity*cols;
        StoreBatch b = pool.getAtLeast(capacity);
        if (b == null)
            return new StoreBatch(rowsCapacity, cols);
        b.unmarkPooled();
        b.clear(cols);
        BatchEvent.Unpooled.record(capacity<<POOL_SHIFT);
        return b;
    }

    @Override public @Nullable StoreBatch recycle(@Nullable StoreBatch batch) {
        if (batch == null) return null;
        int capacity = batch.directBytesCapacity();
        batch.markPooled();
        if (pool.offerToNearest(batch, capacity>>POOL_SHIFT) == null) {
            BatchEvent.Pooled.record(capacity);
        } else {
            batch.recycleInternals(); // could not pool batch, try recycling arr and hashes
            BatchEvent.Garbage.record(capacity);
        }
        return null;
    }

    public <O extends Batch<O>> BIt<StoreBatch> convert(BIt<O> other, int dictId) {
        if (equals(other.batchType())) //noinspection unchecked
            return (BIt<StoreBatch>) other;
        return new StoreBatchConverterBIt<>(other, this, dictId);
    }

    private static class StoreBatchConverterBIt<S extends Batch<S>>
            extends IdConverterBIt<StoreBatch, S> {
        private final int dictId;

        public StoreBatchConverterBIt(BIt<S> delegate, BatchType<StoreBatch> batchType,
                                    int dictId) {
            super(delegate, batchType);
            this.dictId = dictId;
        }

        @Override protected StoreBatch putConverting(StoreBatch out, S in) {
            return out.putConverting(in, dictId);
        }
    }

    @Override public RowBucket<StoreBatch> createBucket(int rowsCapacity, int cols) {
        return new StoreBatchBucket(rowsCapacity, cols);
    }

    @Override public @Nullable Merger<StoreBatch> projector(Vars out, Vars in) {
        int[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger<>(this, out, sources);
    }

    @Override public @NonNull Merger<StoreBatch> merger(Vars out, Vars left, Vars right) {
        return new Merger<>(this, out, mergerSources(out, left, right));
    }

    @Override public Filter<StoreBatch> filter(Vars out, Vars in, RowFilter<StoreBatch> filter,
                                               BatchFilter<StoreBatch> before) {
        return new Filter<>(this, out, projector(out, in), filter, before);
    }

    @Override public Filter<StoreBatch> filter(Vars vars, RowFilter<StoreBatch> filter,
                                               BatchFilter<StoreBatch> before) {
        return new Filter<>(this, vars, null, filter, before);
    }

}
