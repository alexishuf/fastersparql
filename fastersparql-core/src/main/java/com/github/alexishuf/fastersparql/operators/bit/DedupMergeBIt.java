package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;


public final class DedupMergeBIt<R> extends MeteredMergeBIt<R> {
    private final Dedup<R> dedup;

    public DedupMergeBIt(List<? extends BIt<R>> sources, Plan plan, Dedup<R> dedup) {
        super(sources, plan);
        this.dedup = dedup;
    }

    public DedupMergeBIt(List<? extends BIt<R>> sources, Dedup<R> dedup, Plan join, int opIdx,
                         @Nullable JoinMetrics metrics) {
        super(sources, join, opIdx, metrics);
        this.dedup = dedup;
    }

    /* --- --- --- customize MergeBIt behavior --- --- --- */

    @Override protected void process(Batch<R> batch, int sourceIdx, RowType<R>.Merger projector) {
        dedup.filter(batch, sourceIdx, projector);
        if (batch.size > 0) {
            if (metrics != null) metrics.rowsEmitted(batch.size);
            feedLock.lock();
            try {
                feed(batch);
            } finally { feedLock.unlock(); }
        }
    }
}
