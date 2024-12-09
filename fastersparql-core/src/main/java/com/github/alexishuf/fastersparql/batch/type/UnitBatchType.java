package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static com.github.alexishuf.fastersparql.util.owned.SpecialOwner.RECYCLED;

public class UnitBatchType extends BatchType<UnitBatch> {
    private static final class Fac implements Supplier<UnitBatch> {
        @Override public UnitBatch get() {return UnitBatch.create().takeOwnership(RECYCLED);}
        @Override public String toString() {return "UnitBatch.Fac";}
    }
    private static final Fac FAC = new Fac();
    public static final UnitBatchType UNIT = new UnitBatchType();

    private UnitBatchType() {
        super(UnitBatch.class, null, FAC, UnitBatch.BYTES);
    }

    @Override public Orphan<UnitBatch> create(int cols) {return UnitBatch.create(cols);}

    @Override public @Nullable Orphan<UnitBatch> poll(int cols) {return null;}

    @Override public UnitBatch empty(@Nullable UnitBatch offer, Object owner, int cols) {
        return UnitBatch.create(cols).takeOwnership(owner);
    }

    @Override public Orphan<UnitBatch> createForThread(int threadId, int cols) {
        return UnitBatch.create(cols);
    }

    @Override public @Nullable Orphan<UnitBatch> pollForThread(int threadId, int cols) {
        return null;
    }

    @Override
    public UnitBatch emptyForThread(int threadId, @Nullable UnitBatch offer, Object owner, int cols) {
        return offer == null ? UnitBatch.create(cols).takeOwnership(owner)
                             : offer.requireOwner(owner).clear(cols);
    }

    @Override
    public Orphan<? extends RowBucket<UnitBatch, ?>> createBucket(int rowsCapacity, int cols) {
        return UnitBucket.create(rowsCapacity, cols);
    }

    @Override public int bucketBytesCost(int rowsCapacity, int cols) {
        return UnitBucket.estimateBytes(rowsCapacity);
    }

    @Override
    public @Nullable Orphan<? extends BatchMerger<UnitBatch, ?>>
    projector(Vars out, Vars in,
              @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
        short[] sources = BatchMerger.projectorSources(out, in);
        if (sources == null) {
            if (before != null) {
                Owned.safeRecycle(before.takeOwnership(this), this);
                throw new IllegalArgumentException("nop projection with before != null");
            }
            return null;
        }
        return UnitBatch.Merger.create(out, sources, before);
    }

    @Override
    public @Nullable Orphan<UnitBatch.Merger> projector(Vars out, Vars in) {
        short[] sources = BatchMerger.projectorSources(out, in);
        return sources == null ? null : UnitBatch.Merger.create(out, sources, null);
    }

    @Override
    public @NonNull Orphan<UnitBatch.Merger>
    merger(Vars out, Vars left, Vars right,
           @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
        return UnitBatch.Merger.create(out, BatchMerger.mergerSources(out, left, right), before);
    }

    @Override
    public @NonNull Orphan<? extends BatchMerger<UnitBatch, ?>>
    merger(Vars out, Vars left, Vars right) {return merger(out, left, right, null);}

    @Override
    public Orphan<UnitBatch.Filter>
    filter(Vars out, Vars in, Orphan<? extends RowFilter<UnitBatch, ?>> filter,
           @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
        return UnitBatch.Filter.create(out, projector(out, in), filter, before);
    }

    @Override
    public Orphan<UnitBatch.Filter>
    filter(Vars vars, Orphan<? extends RowFilter<UnitBatch, ?>> filter,
           @Nullable Orphan<? extends BatchProcessor<UnitBatch, ?>> before) {
        return UnitBatch.Filter.create(vars, null, filter, before);
    }
}
