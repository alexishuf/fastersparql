package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.dedup.Dedup;
import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.List;

import static com.github.alexishuf.fastersparql.client.util.Merger.forProjection;

public final class DedupMergeBIt<R> extends MeteredMergeBIt<R> {
    private final Dedup<R> dedup;

    public DedupMergeBIt(List<? extends BIt<R>> sources, Plan<R, ?> plan, Dedup<R> dedup) {
        super(sources, plan);
        this.dedup = dedup;
    }

    /* --- --- --- customize MergeBIt behavior --- --- --- */

    @Override protected void drain(BIt<R> source, int sourceIdx) {
        var projector = source.vars().equals(vars)
                      ? null : forProjection(plan.rowType, vars, source.vars());
        long sourceBit = 1L << sourceIdx;
        for (var b = source.nextBatch(); b.size > 0; b = source.nextBatch()) {
            dedup.filter(b, sourceIdx, projector);
            if (b.size > 0) {
                feed(b);
                recycling |= sourceBit;
                rows.addAndGet(b.size);
            } else if (b.array.length > 0) {
                source.recycle(b);
            }
        }
    }
}
