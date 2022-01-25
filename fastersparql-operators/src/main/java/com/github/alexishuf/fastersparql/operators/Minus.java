package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.reactivestreams.Subscriber;

import java.util.Arrays;

import static com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers.errorResults;
import static com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers.rowClass;

public interface Minus extends Operator {
    default OperatorName name() { return OperatorName.MINUS; }

    /**
     * Returns the result of SPARQL algebra {@code Minus(left, right)} operator.
     *
     * @param left the rows to include if no compatible row in right exists
     * @param right the rows to use for removing rows from {@code left}
     * @param <R> the row type
     * @return a non-null {@link Results} with the result of the Minus operation.
     */
    <R> Results<R> checkedRun(Results<R> left, Results<R> right);

    /**
     * Same as {@link Minus#checkedRun(Results, Results)} but reports any exception via
     * {@link Subscriber#onError(Throwable)}
     */
    default <R> Results<R> run(Results<R> left, Results<R> right) {
        try {
            return checkedRun(left, right);
        } catch (Throwable t) {
            return errorResults(left, null, rowClass(Arrays.asList(left, right)), t);
        }
    }
}
