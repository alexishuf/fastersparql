package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers;
import org.reactivestreams.Subscriber;

public interface Distinct extends Operator {
    default OperatorName name() { return OperatorName.DISTINCT; }

    /**
     * Creates a new {@link Results} whose publisher does not produce duplicate rows.
     *
     * @param input the input {@link Results}.
     * @param <R> the row type
     * @return A non-null {@link Results} without duplicate rows
     */
    <R> Results<R> checkedRun(Results<R> input);

    /**
     * Same as {@link Distinct#checkedRun(Results)}, but reports any {@link Throwable} via
     * {@link Subscriber#onError(Throwable)}
     */
    default <R> Results<R> run(Results<R> input) {
        try {
            return checkedRun(input);
        } catch (Throwable t) {
            return OperatorHelpers.errorResults(input, null, null, t);
        }
    }
}
