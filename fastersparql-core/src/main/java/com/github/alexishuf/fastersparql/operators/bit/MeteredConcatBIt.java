package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchMerger;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;


public class MeteredConcatBIt<B extends Batch<B>> extends ConcatBIt<B> {
    protected final @Nullable Metrics metrics;
    protected final Plan plan;
    protected @Nullable BatchMerger<B> projector;
    protected int sourceIdx;

    public MeteredConcatBIt(Collection<? extends BIt<B>> sources, Plan plan) {
        super(sources, sources.iterator().next().batchType(), plan.publicVars());
        this.plan = plan;
        this.metrics = Metrics.createIf(plan);
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (metrics != null)
            metrics.complete(cause, isClosedFor(cause, this)).deliver();
    }

    @Override protected boolean nextSource() {
        boolean has = super.nextSource();
        if (has) {
            ++sourceIdx;
            //noinspection DataFlowIssue
            projector = batchType.projector(vars, inner.vars());
        }
        return has;
    }

    @Override public @Nullable B recycle(B batch) {
        if (batch != null && super.recycle(batch) != null && projector != null)
            return projector.recycle(batch);
        return null;
    }

    @Override public @Nullable B stealRecycled() {
        B b = projector == null ? null : projector.stealRecycled();
        if (b != null)
            return b;
        return super.stealRecycled();
    }

    @Override public @Nullable B nextBatch(@Nullable B b) {
        b = super.nextBatch(b);
        try {
            if (b != null) {
                if (projector != null) b = projector.processInPlace(b);
                if (metrics   != null) metrics.rowsEmitted(b.rows);
            }
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
        return b;
    }

    @Override protected String toStringNoArgs() {
        return plan.algebraName()+"-"+plan.id()+"-"+id();
    }
}
