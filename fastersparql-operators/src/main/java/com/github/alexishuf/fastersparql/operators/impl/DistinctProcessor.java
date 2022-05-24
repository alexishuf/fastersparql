package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.row.RowSet;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

public class DistinctProcessor<R> extends AbstractProcessor<R, R> {
    private final Plan<R> plan;
    private final RowSet<R> set;

    public DistinctProcessor(FSPublisher<? extends R> source, Plan<R> plan, RowSet<R> set) {
        super(source);
        this.plan = plan;
        this.set = set;
    }

    @Override protected void handleOnNext(R row) {
        if (set.add(row))
            emit(row);
    }

    @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
        if (hasGlobalMetricsListeners())
            sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, error, cancelled));
    }
}
