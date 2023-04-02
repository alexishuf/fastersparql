package com.github.alexishuf.fastersparql.batch.type;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract class BatchFilter<B extends Batch<B>> extends BatchProcessor<B> {
    public final @Nullable BatchMerger<B> projector;
    public final RowFilter<B> rowFilter;

    public BatchFilter(BatchType<B> batchType, @Nullable BatchMerger<B> projector,
                       RowFilter<B> rowFilter) {
        super(batchType);
        this.projector = projector;
        this.rowFilter = rowFilter;
    }


    @Override public final B processInPlace(B b) { return filterInPlace(b, projector); }

    @Override public final B process(B b) { return filter(null, b); }

    public abstract B filterInPlace(B in, @Nullable BatchMerger<B> projector);

    public final B filterInPlace(B in) { return filterInPlace(in, projector); }

    public B filter(@Nullable B dest, B in) {
        int rows = in.rows;
        BatchMerger<B> projector = this.projector;
        if (dest == null) {
            int cols = projector == null ? in.cols : projector.sources.length;
            dest = getBatch(rows, cols, in.bytesUsed());
        }
        if (rowFilter.targetsProjection() && projector != null) {
            dest = projector.project(dest, in);
            return filterInPlace(dest, null);
        }
        if (projector == null) {
            for (int r = 0; r < rows; r++) {
                if (!rowFilter.drop(in, r)) dest.putRow(in, r);
            }
        } else {
            int[] columns = Objects.requireNonNull(projector.columns);
            for (int r = 0; r < rows; r++) {
                if (rowFilter.drop(in, r)) continue;
                dest.beginPut();
                for (int c : columns) {
                    if   (c < 0) dest.putTerm(null);
                    else         dest.putTerm(in, r, c);
                }
                dest.commitPut();
            }
        }
        return dest;
    }
}
