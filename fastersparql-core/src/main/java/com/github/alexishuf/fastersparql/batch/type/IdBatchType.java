package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.batch.type.IdBatch.Filter;
import com.github.alexishuf.fastersparql.batch.type.IdBatch.Merger;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.model.rope.ByteSink;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Thread.currentThread;

public abstract class IdBatchType<B extends IdBatch<B>> extends BatchType<B> {
    public IdBatchType(Class<B> cls, Supplier<B> factory) {
        super(cls, factory, factory, IdBatch.BYTES);
    }

    @Override public Orphan<B> createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Orphan<B> pollForThread(int threadId, int cols) {
        B b = pollForThread0(threadId);
        return b == null ? null : b.clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public B emptyForThread(int threadId, @Nullable B offer, Object owner, int cols) {
        return emptyForThread0(threadId, offer, owner).clear(cols);
    }

    @Override public Orphan<B> create(int cols) {
        return createForThread((int)currentThread().threadId(), cols);
    }

    @Override public @Nullable Orphan<B> poll(int cols) {
        return pollForThread((int)currentThread().threadId(), cols);
    }

    @Override public B empty(@Nullable B offer, Object owner, int cols) {
        return emptyForThread((int)currentThread().threadId(), offer, owner, cols);
    }

    @Override public Orphan<IdBatchBucket<B>> createBucket(int rows, int cols) {
        return new IdBatchBucket.Concrete<>(this, rows, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return IdBatchBucket.estimateBytes(rowsCapacity, cols);
    }

    @Override public @Nullable Orphan<Merger<B>> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new Merger.Concrete<>(this, out, sources);
    }

    @Override public @NonNull Orphan<Merger<B>> merger(Vars out, Vars left, Vars right) {
        return new Merger.Concrete<>(this, out, mergerSources(out, left, right));
    }

    @Override public Orphan<Filter<B>>
    filter(Vars out, Vars in, Orphan<? extends RowFilter<B, ?>> filter,
           Orphan<? extends BatchFilter<B, ?>> before) {
        return new Filter.Concrete<>(this, out, projector(out, in), filter, before);
    }

    @Override public Orphan<Filter<B>>
    filter(Vars vars, Orphan<? extends RowFilter<B, ?>> filter,
           Orphan<? extends BatchFilter<B, ?>> before) {
        return new Filter.Concrete<>(this, vars, null, filter, before);
    }

    public abstract int hashId(long id);
    public abstract boolean equals(long lId, long rId);
    public abstract ByteSink<?, ?> appendNT(ByteSink<?, ?> sink, long id, byte[] nullValue);
}
