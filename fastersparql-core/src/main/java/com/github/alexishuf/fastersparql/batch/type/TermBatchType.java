package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.TermBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.TermBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Thread.currentThread;

public final class TermBatchType extends BatchType<TermBatch> {
    public static final TermBatchType TERM = new TermBatchType();

    @SuppressWarnings("SameReturnValue") public static TermBatchType get() { return TERM; }

    private TermBatchType() {
        super(TermBatch.class, TermBatchCleaner.INSTANCE.newInstance,
              TermBatchCleaner.INSTANCE.clearElseMake, TermBatch.BYTES);
        TermBatchCleaner.INSTANCE.pool = this.pool;
    }

    @Override public Orphan<TermBatch> createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Orphan<TermBatch> pollForThread(int threadId, int cols) {
        TermBatch b = pollForThread0(threadId);
        return b == null ? null : b.clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public TermBatch emptyForThread(int threadId, @Nullable TermBatch offer,
                                              Object owner, int cols) {
        return emptyForThread0(threadId, offer, owner).clear(cols);
    }

    @Override public Orphan<TermBatch> create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }

    @Override public @Nullable Orphan<TermBatch> poll(int cols) {
        return pollForThread((int)currentThread().threadId(), cols);
    }

    @Override public TermBatch empty(@Nullable TermBatch offer, Object owner, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, owner, cols);
    }

    @Override public Orphan<TermBatchBucket> createBucket(int rowsCapacity, int cols) {
        return new TermBatchBucket.Concrete(rowsCapacity, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return TermBatchBucket.estimateBytes(rowsCapacity, cols);
    }

    @Override
    public @Nullable Orphan<Merger> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger.Concrete(this, out, sources);
    }

    @Override
    public @NonNull Orphan<Merger> merger(Vars out, Vars left, Vars right) {
        return new Merger.Concrete(this, out, mergerSources(out, left, right));
    }

    @Override public Orphan<Filter>
    filter(Vars out, Vars in, Orphan<? extends RowFilter<TermBatch, ?>> filter,
           @Nullable Orphan<? extends BatchFilter<TermBatch, ?>> before) {
        return new Filter.Concrete(this, out, projector(out, in), filter, before);
    }

    @Override public Orphan<Filter>
    filter(Vars vars, Orphan<? extends RowFilter<TermBatch, ?>> filter,
           @Nullable Orphan<? extends BatchFilter<TermBatch, ?>> before) {
        return new Filter.Concrete(this, vars, null, filter, before);
    }
}
