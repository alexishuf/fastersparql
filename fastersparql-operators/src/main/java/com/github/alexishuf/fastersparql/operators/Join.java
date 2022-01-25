package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.reactivestreams.Subscriber;

import java.util.List;

import static com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers.*;

public interface Join extends Operator {
    default OperatorName name() { return OperatorName.JOIN; }

    /**
     * Join the given results.
     *
     * An implementation is allowed to reorder operands which are not the first
     * in {@code resultsList} but are not expected to do so. The caller of this method should
     * provide good enough ordering presuming a left-associative execution order of binary joins.
     *
     * Although left-associative execution should be assumed for optimization purposes, there is
     * no requirement that implementations be made of binary join algorithms.
     *
     * As in the Join operator of SPARQL algebra, joins may act as cartesian products when
     * two operands do not share variables. Implementations should avoid or delay such cartesian
     * products, but are not required to.
     *
     * @param resultsList the list of operands
     * @param <R> the row type
     * @return a non-null {@link Results} with the join result.
     */
    <R> Results<R> checkedRun(List<Results<R>> resultsList);

    /**
     * Same as {@link Join#checkedRun(List)}, but returns errors via
     * {@link Subscriber#onError(Throwable)}.
     */
    default <R> Results<R> run(List<Results<R>> list) {
        try {
            return checkedRun(list);
        } catch (Throwable t) {
            return errorResults(null, varsUnion(list), rowClass(list), t);
        }
    }
}
