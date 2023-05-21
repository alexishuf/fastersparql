package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.DedupPool;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.RowFilter;
import com.github.alexishuf.fastersparql.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DedupConcatBIt<B extends Batch<B>> extends ConcatBIt<B> {
    private final Dedup<B> dedup;

    public DedupConcatBIt(List<? extends BIt<B>> sources, Vars vars, Dedup<B> dedup) {
        super(sources, dedup.batchType(), vars);
        this.dedup = dedup;
    }

    /* --- --- --- customize ConcatBIt behavior --- --- --- */

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        DedupPool<B> pool = batchType.dedupPool;
        if (dedup instanceof WeakCrossSourceDedup<B> cd)
            pool.offerWeakCross(cd);
        else if (dedup instanceof WeakDedup<B> wd)
            pool.offerWeak(wd);
    }

    @Override protected @Nullable BatchProcessor<B> createProcessor(BIt<B> source, int sourceIdx) {
        RowFilter<B> rowFilter = dedup.sourcedFilter(sourceIdx);
        return batchType.filter(vars, source.vars(), rowFilter);
    }
}
