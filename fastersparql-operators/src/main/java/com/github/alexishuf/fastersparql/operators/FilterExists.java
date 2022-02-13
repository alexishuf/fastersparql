package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.FilterExistsPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface FilterExists extends Operator {
    default OperatorName name() { return OperatorName.FILTER_EXISTS; }

    /**
     * Creates a {@link Plan} for {@code run(input, negate, filters)}.
     */
    default <R> FilterExistsPlan<R> asPlan(Plan<R> input, boolean negate, Plan<R> filter) {
        return new FilterExistsPlan<>(this, input, negate, filter);
    }

    /**
     * Create {@link Results} only with rows of {@code input} for which {@code filter} provides
     * at least one compatible (in join semantics) result (or a true result in case {@code filter}
     * is an ASK query).
     *
     * If {@code negate} is true, then rows will be discarded if {@code filter} produces at least
     * one compatible row.
     *
     * @param input the origin of rows to filter
     * @param negate Whether to negate the condition for keeping a row from {@code input}. If
     *               {@code true}, rows from {@code input} with a match in {@code filter} are
     *               removed. If {@code false}, rows from {@code input} <strong>without</strong>
     *               a match in {@code filter} will be removed.
     * @param filter An ASK or SELECT query to use as the filter condition.
     * @param <R> the row type.
     * @return a {@link Results} object only with rows from {@code input} that satisfy the filter
     *         (or that do not satisfy the filter if {@code negate == true}.
     */
    <R> Results<R> checkedRun(Plan<R> input, boolean negate, Plan<R> filter);

    /**
     * Equivalent to {@link FilterExists#checkedRun(Plan, boolean, Plan)} but returns
     * exceptions thrown by he method via {@link Results#publisher()}.
     */
    default <R> Results<R> run(Plan<R> input, boolean negate,
                               @Nullable Plan<R> filters) {
        try {
            return checkedRun(input, negate, filters);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }

}
