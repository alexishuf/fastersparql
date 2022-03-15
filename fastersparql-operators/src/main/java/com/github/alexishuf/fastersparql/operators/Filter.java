package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.errors.IllegalSPARQLFilterException;
import com.github.alexishuf.fastersparql.operators.plan.FilterPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public interface Filter extends Operator {
    default OperatorName name() { return OperatorName.FILTER; }

    /**
     * Creates a plan for {@code run(input.execute(), filters, predicates)}.
     */
    default <R> FilterPlan<R> asPlan(Plan<R> input, @Nullable List<String> filters) {
        return new FilterPlan<>(rowClass(), this, input, filters, null, null);
    }

    default <R> FilterPlan.FilterPlanBuilder<R> asPlan() {
        return FilterPlan.<R>builder().rowClass(rowClass()).op(this);
    }

    /**
     * Create a {@link Results} without rows that fail any of the SPARQL filters in
     * {@code filters} or any of the predicates in {@code predicates}.
     *
     * @param plan The {@link FilterPlan} to execute
     * @param <R> the row type
     * @return a non-null {@link Results} with only rows that are accepted by all filters predicates.
     *
     * @throws IllegalSPARQLFilterException if any filter in {@code filter} is not a valid
     *         SPARQL boolean expression
     */
    <R> Results<R> checkedRun(FilterPlan<R> plan);

    /**
     * Equivalent to {@link Filter#checkedRun(FilterPlan)}, but returns exceptions thrown
     * by the method through {@link Results#publisher()}.
     */
    default <R> Results<R> run(FilterPlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
