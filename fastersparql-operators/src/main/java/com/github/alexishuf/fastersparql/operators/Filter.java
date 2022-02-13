package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.errors.IllegalSPARQLFilterException;
import com.github.alexishuf.fastersparql.operators.plan.FilterPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;

public interface Filter extends Operator {
    default OperatorName name() { return OperatorName.FILTER; }

    /**
     * Creates a plan for {@code run(input.execute(), filters, predicates)}.
     */
    default <R> FilterPlan<R> asPlan(Plan<R> input, @Nullable Collection<String> filters) {
        return new FilterPlan<>(this, input, filters);
    }

    /**
     * Create a {@link Results} without rows that fail any of the SPARQL filters in
     * {@code filters} or any of the predicates in {@code predicates}.
     *
     * @param input the input whose rows will be evaluated
     * @param filters a collection of strings, each being a SPARQL boolean expression. The
     *                strings may use the {@code xsd}, {@code rdf}, {@code rdfs} and {@code owl}
     *                prefixes, which are mapped to their conventional URIs. Any other IRI
     *                references must be explicit. If {@code null}, will treat as an empty set.
     * @param <R> the row type
     * @return a non-null {@link Results} with only rows that are accepted by all filters predicates.
     *
     * @throws IllegalSPARQLFilterException if any filter in {@code filter} is not a valid
     *         SPARQL boolean expression
     */
    <R> Results<R> checkedRun(Plan<R> input, @Nullable Collection<? extends CharSequence> filters);

    /**
     * Equivalent to {@link Filter#checkedRun(Plan, Collection)}, but returns exceptions thrown
     * by the method through {@link Results#publisher()}.
     */
    default <R> Results<R> run(Plan<R> input, @Nullable Collection<? extends CharSequence> filters) {
        try {
            return checkedRun(input, filters);
        } catch (Throwable t) {
            return Results.error(Object.class, t);
        }
    }
}
