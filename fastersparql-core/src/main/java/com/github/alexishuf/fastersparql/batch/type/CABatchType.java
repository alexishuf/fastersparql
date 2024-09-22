package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
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
    public @Nullable Orphan<CABatch.Merger> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new CABatch.Merger.Concrete(this, out, sources);
    }

    @Override
    public @NonNull Orphan<CABatch.Merger> merger(Vars out, Vars left, Vars right) {
        return new CABatch.Merger.Concrete(this, out, mergerSources(out, left, right));
    }

    @Override
    public Orphan<CABatch.Filter> filter(Vars out, Vars in, Orphan<? extends RowFilter<CABatch, ?>> filter,
                                                 Orphan<? extends BatchFilter<CABatch, ?>> before) {
        return new CABatch.Filter.Concrete(this, out, projector(out, in), filter, before);
    }

    @Override
    public Orphan<CABatch.Filter> filter(Vars vars, Orphan<? extends RowFilter<CABatch, ?>> filter,
                                                 Orphan<? extends BatchFilter<CABatch, ?>> before) {
        return new CABatch.Filter.Concrete(this, vars, null, filter, before);
    }

}
