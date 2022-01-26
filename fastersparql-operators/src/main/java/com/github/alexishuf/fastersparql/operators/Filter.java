package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.errors.IllegalSPARQLFilterException;
import com.github.alexishuf.fastersparql.operators.plan.FilterPlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.function.Predicate;

public interface Filter extends Operator {
    default OperatorName name() { return OperatorName.FILTER; }

    /**
     * Creates a plan for {@code run(input.execute(), filters, predicates)}.
     */
    default <R> FilterPlan<R> asPlan(Plan<R> input, @Nullable Collection<String> filters,
                                     @Nullable Collection<Predicate<R>> predicates) {
        return new FilterPlan<>(this, input, filters, predicates);
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
     * @param predicates a collection of java {@link Predicate}s that are able to evaluate
     *                   the rows produced by the input. If {@code null}, will treat as an empty list.
     * @param <R> the row type
     * @return a non-null {@link Results} with only rows that are accepted by all filters predicates.
     *
     * @throws IllegalSPARQLFilterException if any filter in {@code filter} is not a valid
     *         SPARQL boolean expression
     */
    <R> Results<R> checkedRun(Plan<R> input, @Nullable Collection<String> filters,
                              @Nullable Collection<Predicate<R>> predicates);

    default <R> Results<R> run(Plan<R> input, @Nullable Collection<String> filters,
                               @Nullable Collection<Predicate<R>> predicates) {
        try {
            return checkedRun(input, filters, predicates);
        } catch (Throwable t) {
            return Results.forError(Object.class, t);
        }
    }
}
