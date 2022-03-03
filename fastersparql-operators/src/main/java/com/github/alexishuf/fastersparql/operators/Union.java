package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.UnionPlan;

import java.util.List;

public interface Union extends Operator {
    default OperatorName name() { return OperatorName.UNION; }

    /**
     * Create a plan for {@code run(inputs)}.
     */
    default <R> UnionPlan<R> asPlan(List<Plan<R>> inputs) {
        return new UnionPlan<>(rowClass(), this, inputs, null);
    }

    default <R> UnionPlan.UnionPlanBuilder<R> asPlan() {
        return UnionPlan.<R>builder().rowClass(rowClass());
    }

    /**
     * Create a {@link Results} with all rows from all inputs.
     *
     * @param plan the {@link UnionPlan} to execute
     * @param <R> the row type
     * @return a non-null {@link Results}
     */
    <R> Results<R> checkedRun(UnionPlan<R> plan);

    /**
     * Similar to {@link Union#checkedRun(UnionPlan)}, but anything thrown by the method itself
     * will be delivered via {@link org.reactivestreams.Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(UnionPlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
