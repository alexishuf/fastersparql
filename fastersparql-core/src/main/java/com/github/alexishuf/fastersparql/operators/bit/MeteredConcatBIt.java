package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

import static com.github.alexishuf.fastersparql.client.util.Merger.forProjection;

public class MeteredConcatBIt<R> extends ConcatBIt<R> {
    protected final Plan<R, ?> plan;
    private final long startNanos = System.nanoTime();
    protected long rows = 0;
    protected @Nullable Throwable error;
    protected int sourceIdx = -1;
    protected @Nullable Merger<R, ?> projector;

    public MeteredConcatBIt(Collection<? extends BIt<R>> sources, Plan<R, ?> plan) {
        super(sources, plan.rowType.rowClass, plan.publicVars());
        this.plan = plan;
    }


    @Override protected void cleanup(boolean interrupted) {
        PlanMetrics.buildAndSend(plan, rows, startNanos, error, interrupted);
        super.cleanup(interrupted);
    }

    @Override public Batch<R> nextBatch() {
        try {
            Batch<R> b = super.nextBatch();
            if (projector != null) {
                R[] a = b.array;
                for (int i = 0, n = b.size; i < n; i++)
                    a[i] = projector.merge(a[i], null);
            }
            rows += b.size;
            return b;
        } catch (Throwable t) { error = error == null ? t : error; throw t; }
    }

    @Override protected boolean nextSource() {
        boolean has = super.nextSource();
        sourceIdx++;
        if (plan != null) {
            projector = source.vars().equals(plan.publicVars())
                      ? null : forProjection(plan.rowType, plan.publicVars(), source.vars());
        }
        return has;
    }

    @Override public R next() {
        try {
            R r = super.next();
            if (projector != null)
                r = projector.merge(r, null);
            ++rows;
            return r;
        } catch (Throwable t) { error = error == null ? t : error; throw t; }
    }

    @Override protected String toStringNoArgs() {
        return plan.name();
    }
}
