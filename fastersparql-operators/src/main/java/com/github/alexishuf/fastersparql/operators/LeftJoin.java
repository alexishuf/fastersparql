package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.reactivestreams.Subscriber;

import java.util.Arrays;
import java.util.List;

import static com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers.*;

public interface LeftJoin extends Operator {
    default OperatorName name() { return OperatorName.LEFT_JOIN; }

    /**
     * Execute {@code LeftJoin(left, right)} from SPARQL algebra
     *
     * @param left left operand, whose rows are included even if there is no compatible row in
     *             {@code right}
     * @param right right operand, whose rows will be joined with left when possible.
     * @param <R> thr row type
     * @return a non-null {@link Results} with the left join rows.
     */
    <R>Results<R> checkedRun(Results<R> left, Results<R> right);

    /**
     * Same as {@link LeftJoin#checkedRun(Results, Results)} but reports exceptions via
     * {@link Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(Results<R> left, Results<R> right) {
        try {
            return checkedRun(left, right);
        } catch (Throwable t) {
            List<Results<R>> list = Arrays.asList(left, right);
            return errorResults(null, varsUnion(list), rowClass(list), t);
        }
    }
}
