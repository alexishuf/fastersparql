package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.Batch;
import com.github.alexishuf.fastersparql.batch.operators.ConcatBIt;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

import static com.github.alexishuf.fastersparql.batch.BItClosedAtException.isClosedFor;


public class MeteredConcatBIt<R> extends ConcatBIt<R> {
    protected final @Nullable Metrics metrics;
    protected final Plan plan;
    protected @Nullable RowType<R>.Merger projector;
    protected int sourceIdx;

    public MeteredConcatBIt(Collection<? extends BIt<R>> sources, Plan plan) {
        super(sources, sources.iterator().next().rowType(), plan.publicVars());
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
            projector = vars.equals(source.vars()) ? null
                      : rowType.projector(vars, source.vars());
        }
        return has;
    }

    @Override public Batch<R> nextBatch() {
        Batch<R> b = super.nextBatch();
        try {
            if (b.size > 0 && projector != null)
                projector.projectInPlace(b);
            if (metrics != null) metrics.rowsEmitted(b.size);
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
        return b;
    }

    @Override protected String toStringNoArgs() {
        return plan.algebraName()+"-"+plan.id()+"-"+id();
    }

    @Override public R next() {
        R r = super.next();
        try {
            if (projector != null) r = projector.projectInPlace(r);
            if (metrics != null) metrics.rowsEmitted(1);
        } catch (Throwable t) {
            onTermination(t);
            throw t;
        }
        return r;
    }
}
