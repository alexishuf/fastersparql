package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.mergerSources;
import static com.github.alexishuf.fastersparql.batch.type.BatchMerger.projectorSources;
import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;
import static java.lang.Thread.currentThread;

public abstract class ObjBatchType<T, B extends ObjBatch<B, T>> extends BatchType<B> {
    public final Class<T> termClass;

    public ObjBatchType(Class<B> cls, Class<T> termClass, Supplier<B> primerFactory,
                        Supplier<B> factory, int bytesPerBatch) {
        super(cls, primerFactory, factory, bytesPerBatch);
        this.termClass = termClass;
    }

    public Class<T> termClass() { return termClass; }

    @Override public Orphan<B> createForThread(int threadId, int cols) {
        return createForThread0(threadId).clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public @Nullable Orphan<B> pollForThread(int threadId, int cols) {
        B b = pollForThread0(threadId);
        return b == null ? null : b.clear(cols).releaseOwnership(RECYCLED);
    }

    @Override public B emptyForThread(int threadId, @Nullable B offer,
                                              Object owner, int cols) {
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

    @Override public Orphan<? extends RowBucket<B, ?>> createBucket(int rowsCapacity, int cols) {
        return ObjBatchBucket.create(this, rowsCapacity, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return ObjBatchBucket.estimateBytes(rowsCapacity, cols);
    }

    @Override
    public @Nullable Orphan<ObjBatch.Merger<T, B>> projector(Vars out, Vars in) {
        short[] sources = projectorSources(out, in);
        return sources == null ? null : new ObjBatch.Merger.Concrete<>(this, out, sources);
    }

    @Override
    public @NonNull Orphan<ObjBatch.Merger<T, B>> merger(Vars out, Vars left, Vars right) {
        return new ObjBatch.Merger.Concrete<>(this, out, mergerSources(out, left, right));
    }

    @Override public Orphan<ObjBatch.Filter<T, B>>
    filter(Vars out, Vars in, Orphan<? extends RowFilter<B, ?>> filter,
           @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
        return new ObjBatch.Filter.Concrete<>(this, out, projector(out, in), filter, before);
    }

    @Override public Orphan<ObjBatch.Filter<T, B>>
    filter(Vars vars, Orphan<? extends RowFilter<B, ?>> filter,
           @Nullable Orphan<? extends BatchFilter<B, ?>> before) {
        return new ObjBatch.Filter.Concrete<>(this, vars, null, filter, before);
    }
}
