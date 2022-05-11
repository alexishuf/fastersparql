package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Represents a tree of operators applied to their arguments
 * @param <R>
 */
public interface Plan<R> {
    /**
     * A name for this plan.
     */
    String name();

    /**
     * Get {@code parent} in the {@code parent.bind()} call which created this plan, if this
     * was created by a bind operation.
     *
     * @return the original unbound {@link Plan}, or null if this was not create by a bind.
     */
    @Nullable Plan<R> parent();

    /**
     * {@link Results#rowClass()} of {@link Plan#execute()};
     */
    Class<? super R> rowClass();

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
     * This is the list of variables that should be used with {@link Plan#bind(Binding)} and related
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
     * @param binding a mapping from variable names to RDF terms in N-Triples syntax.
     * @return a non-null Plan, being a copy of this with replaced variables or {@code this}
     *         if there is no variable to replace.
     */
    Plan<R> bind(Binding binding);
}
