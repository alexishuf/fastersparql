package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.reactive.AbstractProcessor;
import com.github.alexishuf.fastersparql.client.util.reactive.FSPublisher;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.metrics.PlanMetrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.hasGlobalMetricsListeners;
import static com.github.alexishuf.fastersparql.operators.FasterSparqlOps.sendMetrics;
import static java.lang.System.nanoTime;

public final class ProjectingProcessor<T> extends AbstractProcessor<T, T> {
    private final @Nullable Plan<?> originalPlan;
    private final List<String> outVars, inVars;
    private final RowOperations rowOps;
    private final int[] indices;

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
        this.outVars = outVars;
        this.inVars = inVars;
        this.rowOps = rowOps;
        this.originalPlan = originalPlan;
        this.indices = VarUtils.projectionIndices(outVars, inVars);
    }

    @Override protected void handleOnNext(T item) {
        @SuppressWarnings("unchecked")
        T row = (T) rowOps.createEmpty(outVars);
        for (int i = 0; i < indices.length; i++) {
            int inIdx = indices[i];
            if (inIdx >= 0)
                rowOps.set(row, i, outVars.get(i), rowOps.get(item, inIdx, inVars.get(inIdx)));
        }
        emit(row);
    }

    @Override protected void onTerminate(@Nullable Throwable error, boolean cancelled) {
        if (originalPlan != null && hasGlobalMetricsListeners()) {
            sendMetrics(originalPlan, new PlanMetrics(originalPlan.name(), rows,
                                                      start, nanoTime(), error, cancelled));
        }
    }
}
