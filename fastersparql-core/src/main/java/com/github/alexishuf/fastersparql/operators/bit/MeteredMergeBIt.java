package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchProcessor;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;


public class MeteredMergeBIt<B extends Batch<B>> extends MergeBIt<B> {
    protected final Plan namingPlan;
    protected final int namingIdx;
    protected final @Nullable Metrics metrics;
    protected final @Nullable JoinMetrics joinMetrics;

    public MeteredMergeBIt(Collection<? extends BIt<B>> sources, Plan plan) {
        this(sources, plan, true);
    }
    protected MeteredMergeBIt(Collection<? extends BIt<B>> sources, Plan plan, boolean autoStart) {
        super(sources, sources.iterator().next().batchType(), plan.publicVars(), autoStart);
        this.namingPlan = plan;
        this.namingIdx = 0;
        this.metrics = Metrics.createIf(plan);
        this.joinMetrics = null;
    }

    public MeteredMergeBIt(Collection<? extends BIt<B>> sources, Plan join, int operandIdx,
                           @Nullable JoinMetrics joinMetrics) {
        this(sources, join, operandIdx, joinMetrics, true);
    }
    protected MeteredMergeBIt(Collection<? extends BIt<B>> sources, Plan join, int operandIdx,
                           @Nullable JoinMetrics joinMetrics, boolean autoStart) {
        super(sources, sources.iterator().next().batchType(), join.publicVars(), autoStart);
        this.namingPlan = join;
        this.namingIdx = operandIdx;
        this.metrics = null;
        this.joinMetrics = joinMetrics;
    }

    @Override protected B process(int i, B b, BatchProcessor<B> processor) {
        b = super.process(i, b, processor);
        if (metrics != null) metrics.rowsEmitted(b.rows);
        return b;
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
