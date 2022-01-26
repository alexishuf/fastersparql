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
        return new MinusPlan<>(this, left, right);
    }

    /**
     * Returns the result of SPARQL algebra {@code Minus(left, right)} operator.
     *
     * @param left the rows to include if no compatible row in right exists
     * @param right the rows to use for removing rows from {@code left}
     * @param <R> the row type
     * @return a non-null {@link Results} with the result of the Minus operation.
     */
    <R> Results<R> checkedRun(Plan<R> left, Plan<R> right);

    /**
     * Same as {@link Minus#checkedRun(Plan, Plan)} but reports any exception via
     * {@link Subscriber#onError(Throwable)}
     */
    default <R> Results<R> run(Plan<R> left, Plan<R> right) {
        try {
            return checkedRun(left, right);
        } catch (Throwable t) {
            return Results.forError(Object.class, t);
        }
    }
}
