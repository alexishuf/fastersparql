package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.FS;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A factory for {@link SparqlClient} instances.
 */
public interface SparqlClientFactory {
    /**
     * A unique identifier (among {@link SparqlClientFactory} implementing classes).
     *
     * @return The tag for this {@link SparqlClientFactory} implementation.
     */
    @Pure String tag();

    /**
     * If there is a tie in {@link FS#clientFor(SparqlEndpoint, String)} and related methods,
     * the lowest-order factory wins.
     *
     * @return any integer, including negative values.
     */
    @Pure int order();

    /**
     * Whether this factory can create a {@link SparqlClient} for the given {@code endpoint}.
     */
    boolean supports(SparqlEndpoint endpoint);

    /**
     * Create a new {@link SparqlEndpoint} querying the given endpoint.
     *
     * @param endpoint the SPARQL endpoint URI and {@link SparqlConfiguration}
     * @return a new, non-null {@link SparqlEndpoint} querying the given endpoint.
     *         Ownership is passed to the caller, which must eventually call
     *         {@link SparqlClient#close()}.
     */
    SparqlClient createFor(SparqlEndpoint endpoint);
}
