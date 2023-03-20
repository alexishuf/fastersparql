package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.BItClosedAtException;
import com.github.alexishuf.fastersparql.batch.dedup.Dedup;
import com.github.alexishuf.fastersparql.batch.dedup.DedupPool;
import com.github.alexishuf.fastersparql.batch.dedup.WeakCrossSourceDedup;
import com.github.alexishuf.fastersparql.batch.dedup.WeakDedup;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchFilter;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class DedupConcatBIt<B extends Batch<B>> extends ConcatBIt<B> {
    private final Dedup<B> dedup;
    private final @Nullable Metrics metrics;
    private BatchFilter<B> filter;
    private int sourceIdx = -1;

    public DedupConcatBIt(List<? extends BIt<B>> sources, Plan plan, Dedup<B> dedup) {
        super(sources, sources.iterator().next().batchType(), plan.publicVars());
        this.metrics = Metrics.createIf(plan);
        this.dedup = dedup;
    }

    /* --- --- --- customize ConcatBIt behavior --- --- --- */

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (metrics != null)
            metrics.complete(cause, BItClosedAtException.isClosedFor(cause, this)).deliver();
        DedupPool<B> pool = batchType.dedupPool;
        if (dedup instanceof WeakCrossSourceDedup<B> cd)
            pool.offerWeakCross(cd);
        else if (dedup instanceof WeakDedup<B> wd)
            pool.offerWeak(wd);
    }

    @Override protected boolean nextSource() {
        if (!super.nextSource()) return false;
        var rowFilter = dedup.sourcedFilter(++sourceIdx);
        //noinspection DataFlowIssue
        filter = batchType().filter(vars, inner.vars(), rowFilter);
        return true;
    }

    @Override public @Nullable B recycle(B batch) {
        if (batch != null && super.recycle(batch) != null)
            return filter.recycle(batch);
        return null;
    }

    @Override public @Nullable B stealRecycled() {
        B b = filter.stealRecycled();
        return b == null ? super.stealRecycled() : b;
    }

    @Override public @Nullable B nextBatch(@Nullable B b) {
        try {
            while (true) { //noinspection DataFlowIssue
                b = inner.nextBatch(b);
                if (b == null) {
                    if (!nextSource()) break;
                } else {
                    b = filter.filterInPlace(b);
                    if (b.rows > 0) {
                        eager = false;
                        if (metrics != null) metrics.rowsEmitted(b.rows);
                        return b;
                    }
                }
            }
            onTermination(null);
            return null;
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
    }
}
