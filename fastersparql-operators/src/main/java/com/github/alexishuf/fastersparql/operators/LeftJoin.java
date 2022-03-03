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
     * @param plan the {@link LeftJoinPlan} to execute
     * @param <R> thr row type
     * @return a non-null {@link Results} with the left join rows.
     */
    <R>Results<R> checkedRun(LeftJoinPlan<R> plan);

    /**
     * Same as {@link LeftJoin#checkedRun(LeftJoinPlan)} but reports exceptions via
     * {@link Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(LeftJoinPlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
