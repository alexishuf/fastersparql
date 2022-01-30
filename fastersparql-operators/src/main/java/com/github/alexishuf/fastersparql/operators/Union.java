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
        return new UnionPlan<>(this, inputs);
    }

    /**
     * Create a {@link Results} with all rows from all inputs.
     *
     * @param inputs a list of input {@link Results}
     * @param <R> the row type
     * @return a non-null {@link Results}
     */
    <R> Results<R> checkedRun(List<Plan<R>> inputs);

    /**
     * Similar to {@link Union#checkedRun(List)}, but anything thrown by the method itself
     * will be delivered via {@link org.reactivestreams.Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(List<Plan<R>> inputs) {
        try {
            return checkedRun(inputs);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
