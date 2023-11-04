package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.BatchEvent;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static java.lang.Thread.currentThread;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    public static final CompressedBatchType COMPRESSED = new CompressedBatchType();

    private static final class CompressedBatchFactory
            implements BatchPool.Factory<CompressedBatch> {
        @Override public CompressedBatch create() {
            var b = new CompressedBatch(PREFERRED_BATCH_TERMS, (short)1);
            b.markPooled();
            return b;
        }
    }

    private CompressedBatchType() { super(CompressedBatch.class, new CompressedBatchFactory()); }

    public static CompressedBatchType get() { return COMPRESSED; }

    @Override public CompressedBatch createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols);
    }

    @Override
    public CompressedBatch emptyForThread(int threadId, @Nullable CompressedBatch offer, int cols) {
        return emptyForThread0(threadId, offer).clear(cols);
    }

    @Override public @Nullable CompressedBatch
    recycleForThread(int threadId, @Nullable CompressedBatch b) {
        for (CompressedBatch next; b != null; b = next) {
            next = b.next;
            b.next = null;
            b.tail = b;
            BatchEvent.Pooled.record(b.markPooled());
            if (pool.offer(threadId, b) != null) b.markGarbage();
        }
        return null;
    }

    @Override public CompressedBatch create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }
    @Override public CompressedBatch empty(@Nullable CompressedBatch offer, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, cols);
    }
    @Override public @Nullable CompressedBatch recycle(@Nullable CompressedBatch batch) {
        return recycleForThread((int)currentThread().threadId(), batch);
    }

    @Override public RowBucket<CompressedBatch> createBucket(int rowsCapacity, int cols) {
        return new CompressedRowBucket(rowsCapacity, cols);
    }

    @Override
    public @Nullable Merger projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger(this, out, sources);
    }

    @Override
    public @NonNull Merger merger(Vars out, Vars left, Vars right) {
        return new Merger(this, out, mergerSources(out, left, right));
    }

    @Override
    public Filter filter(Vars out, Vars in, RowFilter<CompressedBatch> filter,
                         BatchFilter<CompressedBatch> before) {
        return new Filter(this, out, projector(out, in), filter, before);
    }

    @Override
    public Filter filter(Vars vars, RowFilter<CompressedBatch> filter,
                         BatchFilter<CompressedBatch> before) {
        return new Filter(this, vars, null, filter, before);
    }
}
