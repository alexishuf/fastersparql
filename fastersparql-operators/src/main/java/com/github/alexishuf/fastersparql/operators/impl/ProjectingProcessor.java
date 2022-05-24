package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;

public final class ProjectingProcessor<T> extends AbstractProcessor<T, T> {
    private final @Nullable Plan<?> plan;
    private final Merger<T> merger;

    public ProjectingProcessor(Results<? extends T> source, List<String> outVars,
                               RowOperations rowOps) {
        this(source, outVars, rowOps, null);
    }

    public ProjectingProcessor(Results<? extends T> source, List<String> outVars,
                               RowOperations rowOps, Plan<T> plan) {
        this(source.publisher(), outVars, source.vars(), rowOps, plan);
    }

    public ProjectingProcessor(FSPublisher<? extends T> source, List<String> outVars,
                               List<String> inVars, RowOperations rowOps,
                               @Nullable Plan<?> originalPlan) {
        super(source);
        this.merger = Merger.forProjection(rowOps, outVars, inVars);
        this.plan = originalPlan;
    }

    @Override protected void handleOnNext(T item) {
        emit(merger.merge(item, null));
    }

    @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
        if (plan != null && hasGlobalMetricsListeners())
            sendMetrics(plan, new PlanMetrics(plan.name(), rows, start, error, cancelled));
    }
}
