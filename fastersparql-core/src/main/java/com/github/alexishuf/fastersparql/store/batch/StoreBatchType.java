package com.github.alexishuf.fastersparql.store.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.BIt.PREFERRED_MIN_BATCH;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public class StoreBatchType extends BatchType<StoreBatch> {
    public static final StoreBatchType INSTANCE = new StoreBatchType(
            new LevelPool<>(StoreBatch.class, 32, 8*PREFERRED_MIN_BATCH));

    public StoreBatchType(LevelPool<StoreBatch> pool) {
        super(StoreBatch.class, pool);
    }

    @Override public StoreBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        StoreBatch b = pool.get(rowsCapacity);
        if (b == null)
            return new StoreBatch(rowsCapacity, cols);
        b.clear(cols);
        return b;
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

    @Override public String toString() { return "StoreBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof StoreBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }

}
