package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.MergeBIt;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class MeteredMergeBIt<R> extends MergeBIt<R> {
    protected final Plan<R, ?> plan;
    private final long startNanos = System.nanoTime();
    protected final AtomicLong rows = new AtomicLong(0);

    public MeteredMergeBIt(Collection<? extends BIt<R>> sources, Plan<R, ?> plan) {
        super(sources, plan.rowOperations().rowClass(), plan.publicVars());
        this.plan = plan;
    }

    @Override protected String toStringNoArgs() { return plan.name(); }

    @Override protected void feed(R item) {
        rows.incrementAndGet();
        super.feed(item);
    }

    @Override protected void feed(Batch<R> batch) {
        rows.addAndGet(batch.size);
        super.feed(batch);
    }

    @Override protected void cleanup(boolean interrupted) {
        PlanMetrics.buildAndSend(plan, rows.get(), startNanos, error, interrupted);
        super.cleanup(interrupted);
    }
}
