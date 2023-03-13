package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;


public class MeteredMergeBIt<R> extends MergeBIt<R> {
    protected final Plan namingPlan;
    protected final int namingIdx;
    protected final @Nullable Metrics metrics;
    protected final @Nullable JoinMetrics joinMetrics;

    public MeteredMergeBIt(Collection<? extends BIt<R>> sources, Plan plan) {
        super(sources, sources.iterator().next().rowType(), plan.publicVars());
        this.namingPlan = plan;
        this.namingIdx = 0;
        this.metrics = Metrics.createIf(plan);
        this.joinMetrics = null;
    }

    public MeteredMergeBIt(Collection<? extends BIt<R>> sources, Plan join, int operandIdx,
                           @Nullable JoinMetrics joinMetrics) {
        super(sources, sources.iterator().next().rowType(), join.publicVars());
        this.namingPlan = join;
        this.namingIdx = operandIdx;
        this.metrics = null;
        this.joinMetrics = joinMetrics;
    }

    @Override protected void process(Batch<R> batch, int sourceIdx,
                                     RowType<R>.@Nullable Merger projector) {
        if (metrics   != null) metrics.rowsEmitted(batch.size);
        if (projector != null) projector.projectInPlace(batch);
        feedLock.lock();
        try {
            feed(batch);
        } finally { feedLock.unlock(); }
    }

    @Override protected void cleanup(@Nullable Throwable cause) {
        super.cleanup(cause);
        if (joinMetrics != null)
            joinMetrics.completeAndDeliver(cause, this);
        if (metrics != null)
            metrics.complete(cause, isClosedFor(cause, this)).deliver();
    }

    @Override protected String toStringNoArgs() {
        return namingPlan.algebraName()+"-"+namingPlan.id()+"-"+id()
                +(namingIdx >= 0 ? "["+namingIdx+"]" : "");
    }
}
