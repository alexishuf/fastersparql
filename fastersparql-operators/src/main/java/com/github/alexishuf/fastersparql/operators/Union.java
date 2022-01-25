package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers.*;

public interface Union extends Operator {
    default OperatorName name() { return OperatorName.UNION; }


    /**
     * Create a {@link Results} with all rows from all inputs.
     *
     * @param inputs a list of input {@link Results}
     * @param <R> the row type
     * @return a non-null {@link Results}
     */
    <R> Results<R> checkedRun(List<Results<R>> inputs);

    /**
     * Similar to {@link Union#checkedRun(List)}, but anything thrown by the method itself
     * will be delivered via {@link org.reactivestreams.Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(List<Results<R>> inputs) {
        try {
            return checkedRun(inputs);
        } catch (Throwable t) {
            return errorResults(null, varsUnion(inputs), rowClass(inputs), t);
        }
    }
}
