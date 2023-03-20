package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;


public final class DedupMergeBIt<B extends Batch<B>> extends MeteredMergeBIt<B> {
    private final Dedup<B> dedup;

    public DedupMergeBIt(List<? extends BIt<B>> sources, Plan plan, Dedup<B> dedup) {
        super(sources, plan, false);
        this.dedup = dedup;
        start();
    }

    public DedupMergeBIt(List<? extends BIt<B>> sources, Dedup<B> dedup, Plan join, int opIdx,
                         @Nullable JoinMetrics metrics) {
        super(sources, join, opIdx, metrics, false);
        this.dedup = dedup;
        start();
    }

    /* --- --- --- customize MergeBIt behavior --- --- --- */

    @Override protected @Nullable BatchProcessor<B> createProcessor(int sourceIdx) {
        Vars inVars = sources.get(sourceIdx).vars();
        return batchType.filter(vars, inVars, dedup.sourcedFilter(sourceIdx));
    }
}
