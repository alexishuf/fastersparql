package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.impl.BindHelpers;

import java.util.List;
import java.util.Map;

/**
 * Represents a tree of operators applied to their arguments
 * @param <R>
 */
public interface Plan<R> {
    /**
     * Create a {@link Results} from this plan.
     *
     * {@link Results#vars()} is not required to be ready and {@link Results#publisher()} may only
     * start processing after it is subscribed to.
     *
     * @return a non-null {@link Results}.
     */
    Results<R> execute();

    /**
     * Create a copy of this {@link Plan} replacing the variables with the values they map to.
     *
     * @param var2ntValue a map from variable names to RDF terms in N-Triples syntax.
     * @return a non-null Plan, being a copy of this with replaced variables or {@code this}
     *         if there is no variable to replace.
     */
    Plan<R> bind(Map<String, String> var2ntValue);

    /**
     * Version of {@link Plan#bind(Map)} where the i-th {@code ntValue} corresponds to the
     * i-th {@code var}.
     *
     * @param vars list of variable names
     * @param ntValues list of values, in N-Triples syntax. The i-th value corresponds to the i-th
     *                 variable.
     * @return a non-null {@link Plan} with the named variables replaced with the given values.
     */
    default Plan<R> bind(List<String> vars, List<String> ntValues) {
        return bind(BindHelpers.toMap(vars, ntValues));
    }

    /**
     * Version of {@link Plan#bind(Map)} where the i-th {@code ntValue} corresponds to the
     * i-th {@code var}.
     *
     * @param vars list of variable names
     * @param ntValues list of values, in N-Triples syntax. The i-th value corresponds to the i-th
     *                 variable.
     * @return a non-null {@link Plan} with the named variables replaced with the given values.
     */
    default Plan<R> bind(List<String> vars, String[] ntValues) {
        return bind(BindHelpers.toMap(vars, ntValues));
    }
}
