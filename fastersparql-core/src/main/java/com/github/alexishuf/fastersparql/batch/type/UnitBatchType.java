package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
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
    public @Nullable Orphan<UnitBatch.Merger> projector(Vars out, Vars in) {
        short[] sources = BatchMerger.projectorSources(out, in);
        return sources == null ? null : UnitBatch.Merger.create(out, sources);
    }

    @Override
    public @NonNull Orphan<UnitBatch.Merger> merger(Vars out, Vars left, Vars right) {
        return UnitBatch.Merger.create(out, BatchMerger.mergerSources(out, left, right));
    }

    @Override
    public Orphan<UnitBatch.Filter> filter(Vars out, Vars in,
                                           Orphan<? extends RowFilter<UnitBatch, ?>> filter,
                                           Orphan<? extends BatchFilter<UnitBatch, ?>> before) {
        return UnitBatch.Filter.create(out, projector(out, in), filter, before);
    }

    @Override
    public Orphan<UnitBatch.Filter> filter(Vars vars,
                                           Orphan<? extends RowFilter<UnitBatch, ?>> filter,
                                           Orphan<? extends BatchFilter<UnitBatch, ?>> before) {
        return UnitBatch.Filter.create(vars, null, filter, before);
    }
}
