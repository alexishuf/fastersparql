package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;

import java.util.List;
import java.util.Map;

/**
 * Represents a tree of operators applied to their arguments
 * @param <R>
 */
public interface Plan<R> {
    /**
     * The would-be value of {@link Results#vars()} upon {@link Plan#execute()}.
     *
     * @return a non-null (but possibly empty) list of non-null and non-empty variable names
     *         (i.e., no leading {@code ?} or {@code $}).
     */
    List<String> publicVars();

    /**
     * All vars used within this plan, not only those exposed in results.
     *
     * This is the list of variables that should be used with {@link Plan#bind(Map)} and related
     * methods.
     *
     * @return a non-null (possibly empty) list of non-null and non-empty variable names
     *         (i.e., no preceding {@code ?} or {@code $}).
     */
    List<String> allVars();

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
        return bind(VarUtils.toMap(vars, ntValues));
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
        return bind(VarUtils.toMap(vars, ntValues));
    }
}
