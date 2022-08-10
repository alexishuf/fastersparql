package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.MinusPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.reactivestreams.Subscriber;

public interface Minus extends Operator {
    default OperatorName name() { return OperatorName.MINUS; }

    /**
     * Create a plan for {@code run(left.execute(), right)}.
     */
    default <R> MinusPlan<R> asPlan(Plan<R> left, Plan<R> right) {
        return new MinusPlan<>(this, left, right, null, null);
    }

    default <R> MinusPlan.Builder<R> asPlan() { return MinusPlan.builder(this); }

    /**
     * Returns the result of SPARQL algebra {@code Minus(left, right)} operator.
     *
     * @param plan the {@link MinusPlan} to execute
     * @param <R> the row type
     * @return a non-null {@link Results} with the result of the Minus operation.
     */
    <R> Results<R> checkedRun(MinusPlan<R> plan);

    /**
     * Same as {@link Minus#checkedRun(MinusPlan)} but reports any exception via
     * {@link Subscriber#onError(Throwable)}
     */
    default <R> Results<R> run(MinusPlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
