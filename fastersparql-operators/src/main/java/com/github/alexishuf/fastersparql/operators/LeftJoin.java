package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.LeftJoinPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.reactivestreams.Subscriber;

public interface LeftJoin extends Operator {
    default OperatorName name() { return OperatorName.LEFT_JOIN; }

    /**
     * Create a plan for {@code run(left, right)}.
     */
    default <R> LeftJoinPlan<R> asPlan(Plan<R> left, Plan<R> right) {
        return new LeftJoinPlan<>(this, left, right);
    }

    /**
     * Execute {@code LeftJoin(left, right)} from SPARQL algebra
     *
     * @param left left operand, whose rows are included even if there is no compatible row in
     *             {@code right}
     * @param right right operand, whose rows will be joined with left when possible.
     * @param <R> thr row type
     * @return a non-null {@link Results} with the left join rows.
     */
    <R>Results<R> checkedRun(Plan<R> left, Plan<R> right);

    /**
     * Same as {@link LeftJoin#checkedRun(Plan, Plan)} but reports exceptions via
     * {@link Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(Plan<R> left, Plan<R> right) {
        try {
            return checkedRun(left, right);
        } catch (Throwable t) {
            return Results.forError(Object.class, t);
        }
    }
}
