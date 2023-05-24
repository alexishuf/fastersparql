package com.github.alexishuf.fastersparql.hdt.batch;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.IdConverterBIt;
import com.github.alexishuf.fastersparql.batch.type.*;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.concurrent.LevelPool;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;

public class HdtBatchType extends BatchType<HdtBatch> {
    public static final HdtBatchType INSTANCE = new HdtBatchType(
            new LevelPool<>(HdtBatch.class, 32, 8* BIt.PREFERRED_MIN_BATCH));

    public HdtBatchType(LevelPool<HdtBatch> pool) {
        super(HdtBatch.class, pool);
    }

    @Override public HdtBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        HdtBatch b = pool.get(rowsCapacity);
        if (b == null)
            return new HdtBatch(rowsCapacity, cols);
        b.clear(cols);
        return b;
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

    @Override public String toString() { return "HdtBatch"; }

    @Override public boolean equals(Object obj) { return obj instanceof HdtBatchType; }

    @Override public int hashCode() { return getClass().hashCode(); }
}
