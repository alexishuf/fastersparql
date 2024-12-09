package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Thread.currentThread;

public class CABatchType extends BatchType<CABatch> {
    public static final CABatchType CA = new CABatchType();

    private CABatchType() {
        super(CABatch.class, CABatchCleaner.INSTANCE.newInstance,
                             CABatchCleaner.INSTANCE.clearElseMake, CABatch.BYTES);
        CABatchCleaner.INSTANCE.pool = this.pool;
    }

    public static CABatchType get() { return CA; }

    @Override public Orphan<CABatch> createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Orphan<CABatch> pollForThread(int threadId, int cols) {
        CABatch b = pollForThread0(threadId);
        return b == null ? null : b.clear(cols).releaseOwnership(RECYCLED);
    }

    @Override
    public CABatch emptyForThread(int threadId, @Nullable CABatch offer,
                                          Object owner, int cols) {
        return emptyForThread0(threadId, offer, owner).clear(cols);
    }

    @Override public Orphan<CABatch> create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }

    @Override public @Nullable Orphan<CABatch> poll(int cols) {
        return pollForThread((int)currentThread().threadId(), cols);
    }

    @Override public CABatch empty(@Nullable CABatch offer, Object owner, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, owner, cols);
    }

    @Override public Orphan<CABucket> createBucket(int rowsCapacity, int cols) {
        return CABucket.create(rowsCapacity, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return CABucket.estimateBytes(rowsCapacity, cols);
    }

    @Override
    public @Nullable Orphan<CABatch.Merger>
    projector(Vars out, Vars in, @Nullable Orphan<? extends BatchProcessor<CABatch, ?>> before) {
        short[] sources = projectorSources(out, in);
        if (sources == null) {
            if (before != null) {
                Owned.safeRecycle(before.takeOwnership(this), this);
                throw new IllegalArgumentException("nop projection with before != null");
            }
            return null;
        }
        return new CABatch.Merger.Concrete(this, out, sources, before);
    }

    @Override
    public @Nullable Orphan<? extends BatchMerger<CABatch, ?>> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new CABatch.Merger.Concrete(this, out, sources, null);
    }

    @Override
    public @NonNull Orphan<CABatch.Merger>
    merger(Vars out, Vars left, Vars right,
           @Nullable Orphan<? extends BatchProcessor<CABatch, ?>> before) {
        return new CABatch.Merger.Concrete(this, out, mergerSources(out, left, right),
                                           before);
    }

    @Override
    public @NonNull Orphan<? extends BatchMerger<CABatch, ?>>
    merger(Vars out, Vars left, Vars right) {return merger(out, left, right, null);}

    @Override
    public Orphan<CABatch.Filter> filter(Vars out, Vars in, Orphan<? extends RowFilter<CABatch, ?>> filter,
                                         @Nullable Orphan<? extends BatchProcessor<CABatch, ?>> before) {
        return new CABatch.Filter.Concrete(this, out, projector(out, in, null), filter, before);
    }

    @Override
    public Orphan<CABatch.Filter> filter(Vars vars, Orphan<? extends RowFilter<CABatch, ?>> filter,
                                         @Nullable Orphan<? extends BatchProcessor<CABatch, ?>> before) {
        return new CABatch.Filter.Concrete(this, vars, null, filter, before);
    }

}
