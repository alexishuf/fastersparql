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
            new LevelPool<>(HdtBatch.class));

    private final LevelPool<HdtBatch> pool;

    public HdtBatchType(LevelPool<HdtBatch> pool) {
        super(HdtBatch.class);
        this.pool = pool;
    }

    @Override public HdtBatch create(int rowsCapacity, int cols, int bytesCapacity) {
        HdtBatch b = pool.getAtLeast(rowsCapacity*cols);
        if (b == null)
            return new HdtBatch(rowsCapacity, cols);
        b.unmarkPooled();
        b.clear(cols);
        return b;
    }

    @Override public @Nullable HdtBatch recycle(@Nullable HdtBatch batch) {
        if (batch == null) return null;
        batch.markPooled();
        if (pool.offer(batch, batch.arr.length) != null) {
            batch.recycleInternals(); // could not pool batch, try recycling arr and hashes
            batch.markGC();
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
