package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.util.owned.Orphan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DedupConcatBIt<B extends Batch<B>> extends ConcatBIt<B> {
    private final Dedup<B, ?> dedup;

    public DedupConcatBIt(List<? extends BIt<B>> sources, BatchType<B> batchType, Vars vars,
                          Orphan<? extends Dedup<B, ?>> dedup) {
        super(sources, batchType, vars);
        this.dedup = dedup.takeOwnership(this);
    }

    /* --- --- --- customize ConcatBIt behavior --- --- --- */

    @Override protected void cleanup(@Nullable Throwable cause) {
        try {
            super.cleanup(cause);
        } finally { dedup.recycle(this); }
    }

    @Override protected @Nullable Orphan<? extends BatchProcessor<B, ?>>
    createProcessor(BIt<B> source, int sourceIdx) {
        return batchType.filter(vars, source.vars(), dedup.sourcedFilter(sourceIdx));
    }
}
