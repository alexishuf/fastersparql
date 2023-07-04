package com.github.alexishuf.fastersparql.batch.type;

import com.github.alexishuf.fastersparql.FSProperties;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.StrongDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public abstract class BatchFilter<B extends Batch<B>> extends BatchProcessor<B> {
    public final @Nullable BatchMerger<B> projector;
    public final RowFilter<B> rowFilter;
    public final @Nullable BatchFilter<B> before;
    protected boolean released;

    public BatchFilter(BatchType<B> batchType, Vars outVars,
                       @Nullable BatchMerger<B> projector,
                       RowFilter<B> rowFilter, @Nullable BatchFilter<B> before) {
        super(batchType, outVars);
        this.projector = projector;
        this.rowFilter = rowFilter;
        this.before = before;
    }

    @Override public void release() {
        if (released)
            return;
        released = true;
        if (before != null)
            before.release();
        if (rowFilter instanceof WeakCrossSourceDedup<B> d) {
            batchType.dedupPool.recycleWeakCross(d);
        } else if (rowFilter instanceof WeakDedup<B> d) {
            batchType.dedupPool .recycleWeak(d);
        } else if (rowFilter instanceof StrongDedup<B> d) {
            if (d.weakenAt() >= FSProperties.distinctCapacity())
                batchType.dedupPool.recycleDistinct(d);
            else
                batchType.dedupPool.recycleReduced(d);
        }
        super.release();
    }

    @Override public final B processInPlace(B b) { return filterInPlace(b, projector); }

    @Override public final B process(B b) { return filter(null, b); }

    /**
     * Some {@link BatchFilter}s may change their internal state on everytime a batch gets
     * filtered through it. This method reverts all such changes, resetting the state to what
     * it was before the first abtch got filtered through this {@link BatchFilter} instance.
     */
    public final void reset() {
        rowFilter.reset();
        if (before != null)
            before.reset();
    }

    public final boolean isDedup() {
        return rowFilter instanceof Dedup<B> || (before != null && before.isDedup());
    }

    public abstract B filterInPlace(B in, @Nullable BatchMerger<B> projector);

    public final B filterInPlace(B in) { return filterInPlace(in, projector); }

    public B filter(@Nullable B dest, B in) {
        if (before != null)
            in = before.filter(null, in);
        if (in == null)
            return null;
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
                switch (rowFilter.drop(in, r)) {
                    case KEEP      -> dest.putRow(in, r);
                    case DROP      -> {}
                    case TERMINATE -> rows = -1;
                }
            }
        } else {
            int[] columns = Objects.requireNonNull(projector.columns);
            for (int r = 0; r < rows; r++) {
                switch (rowFilter.drop(in, r)) {
                    case KEEP -> {
                        dest.beginPut();
                        for (int c = 0, s; c < columns.length; c++) {
                            if ((s = columns[c]) >= 0)
                                dest.putTerm(c, in, r, s);
                        }
                        dest.commitPut();
                    }
                    case DROP      -> {}
                    case TERMINATE -> rows = -1;
                }
            }
        }
        if (rows == -1 && dest.rows == 0) {
            batchType.recycle(dest);
            return null;
        }
        return dest;
    }
}
