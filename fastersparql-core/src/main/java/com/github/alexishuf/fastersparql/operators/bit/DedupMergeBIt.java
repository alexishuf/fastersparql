package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.MetricsFeeder;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import com.github.alexishuf.fastersparql.util.owned.Owned;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;


public final class DedupMergeBIt<B extends Batch<B>> extends MergeBIt<B> {
    private @Nullable Dedup<B, ?> dedup;

    public DedupMergeBIt(List<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars,
                         @Nullable MetricsFeeder metrics, Orphan<? extends Dedup<B, ?>> dedup) {
        super(sources,  batchType, vars, metrics, false);
        this.dedup = dedup.takeOwnership(this);
        start();
    }

    @Override public void close() {
        super.close();
        dedup = Owned.recycle(dedup, this);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        dedup = Owned.recycle(dedup, this);
    }

    /* --- --- --- customize MergeBIt behavior --- --- --- */

    @Override protected @Nullable BatchProcessor<B, ?>
    createProcessor(int sourceIdx) {
        Dedup<B, ?> dedup = this.dedup;
        if (dedup == null)
            return null;
        Vars inVars = sources.get(sourceIdx).vars();
        return batchType.filter(vars, inVars, dedup.sourcedFilter(sourceIdx)).takeOwnership(this);
    }
}
