package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.CompressedBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.FinalSegmentRope;
import com.github.alexishuf.fastersparql.util.concurrent.Bytes;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Thread.currentThread;

public final class CompressedBatchType extends BatchType<CompressedBatch> {
    private static final class CompressedBatchFac implements Supplier<CompressedBatch> {
        @Override public CompressedBatch get() {
            var locals = Bytes.createPooled(new byte[PREFERRED_BATCH_TERMS << 5]);
            var slices = new short[PREFERRED_BATCH_TERMS << 1];
            var shared = new FinalSegmentRope[PREFERRED_BATCH_TERMS];
            var b = new CompressedBatch.Concrete(locals, slices, shared, (short) 1);
            b.takeOwnership(RECYCLED);
            return b;
        }
        @Override public String toString() {return "CompressedBatchType.FAC";}
    }
    private static final CompressedBatchFac FAC = new CompressedBatchFac();
    public static final CompressedBatchType COMPRESSED = new CompressedBatchType();

    private CompressedBatchType() {
        super(CompressedBatch.class, FAC, FAC, CompressedBatch.BYTES);
    }

    public static CompressedBatchType get() { return COMPRESSED; }

    @Override public Orphan<CompressedBatch> createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Orphan<CompressedBatch> pollForThread(int threadId, int cols) {
        CompressedBatch b = pollForThread0(threadId);
        return b == null ? null : b.clear(cols).releaseOwnership(RECYCLED);
    }

    @Override
    public CompressedBatch emptyForThread(int threadId, @Nullable CompressedBatch offer,
                                          Object owner, int cols) {
        return emptyForThread0(threadId, offer, owner).clear(cols);
    }

    @Override public Orphan<CompressedBatch> create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }

    @Override public @Nullable Orphan<CompressedBatch> poll(int cols) {
        return pollForThread((int)currentThread().threadId(), cols);
    }

    @Override public CompressedBatch empty(@Nullable CompressedBatch offer, Object owner, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, owner, cols);
    }

    @Override public Orphan<CompressedRowBucket> createBucket(int rowsCapacity, int cols) {
        return new CompressedRowBucket.Concrete(rowsCapacity, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return CompressedRowBucket.estimateBytes(rowsCapacity, cols);
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

    @Override
    public Orphan<Filter> filter(Vars out, Vars in, Orphan<? extends RowFilter<CompressedBatch, ?>> filter,
                         Orphan<? extends BatchFilter<CompressedBatch, ?>> before) {
        return new Filter.Concrete(this, out, projector(out, in), filter, before);
    }

    @Override
    public Orphan<Filter> filter(Vars vars, Orphan<? extends RowFilter<CompressedBatch, ?>> filter,
                         Orphan<? extends BatchFilter<CompressedBatch, ?>> before) {
        return new Filter.Concrete(this, vars, null, filter, before);
    }
}
