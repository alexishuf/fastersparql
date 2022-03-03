package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.DistinctPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.reactivestreams.Subscriber;

public interface Distinct extends Operator {
    default OperatorName name() { return OperatorName.DISTINCT; }

    /**
     * Creates a {@link Plan} for {@code run(input.execute()}.
     */
    default <R> DistinctPlan<R> asPlan(Plan<R> input) {
        return new DistinctPlan<>(this, input);
    }

    /**
     * Creates a new {@link Results} whose publisher does not produce duplicate rows.
     *
     * @param distinctPlan the {@link DistinctPlan} to execute
     * @param <R> the row type
     * @return A non-null {@link Results} without duplicate rows
     */
    <R> Results<R> checkedRun(DistinctPlan<R> distinctPlan);

    /**
     * Same as {@link Distinct#checkedRun(DistinctPlan)}, but reports any {@link Throwable} via
     * {@link Subscriber#onError(Throwable)}
     */
    default <R> Results<R> run(DistinctPlan<R> input) {
        try {
            return checkedRun(input);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
