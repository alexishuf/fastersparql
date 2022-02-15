package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;

import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

public class FasterSparql {
    /**
     * Alias for {@link FasterSparql#factory(List)} with variadic arguments
     *
     * @param preferredTagsOrClassNames list of preferred tags, fully qualified class
     *                                  names or simple class names.
     * @return the highest-scoring {@link SparqlClientFactory}. The returned factory may
     *         correspond to none of the preferred tags or class names.
     */
    public static SparqlClientFactory factory(String... preferredTagsOrClassNames) {
        return factory(Arrays.asList(preferredTagsOrClassNames));
    }

    /**
     * Get the highest-{@link SparqlClientFactory#order()} {@link SparqlClientFactory} instance
     * whose {@link SparqlClientFactory#tag()} or {@link Object#getClass()} appears
     * first in the given {@code preferredTagsOrClassNames} list.
     *
     * @param preferredTagsOrClassNames a list of {@link SparqlClientFactory#tag()}s,
     *                                  {@link Class#getName()} and {@link Class#getSimpleName()}.
     *                                  The earlier an entry appears, the higher its precedence.
     * @return a non-null {@link SparqlClientFactory}. The returned {@link SparqlClientFactory}
     *         may correspond to no tag or class name in {@code preferredTagsOrClassNames}.
     */
    public static SparqlClientFactory factory(List<String> preferredTagsOrClassNames) {
        SparqlClientFactory best = null;
        int bestIndex = Integer.MAX_VALUE, bestOrder = Integer.MAX_VALUE;
        for (SparqlClientFactory factory : ServiceLoader.load(SparqlClientFactory.class)) {
            int i = preferredTagsOrClassNames.indexOf(factory.tag());
            if (i < 0)
                i = preferredTagsOrClassNames.indexOf(factory.getClass().getName());
            if (i < 0)
                i = preferredTagsOrClassNames.indexOf(factory.getClass().getSimpleName());
            if (i < 0)
                i = Integer.MAX_VALUE-1;
            int order = factory.order();
            if (i < bestIndex) {
                bestIndex = i;
                bestOrder = order;
                best = factory;
            } else if (i == bestIndex && order < bestOrder) {
                bestOrder = order;
                best = factory;
            }
        }
        return best;
    }

    /**
     * Equivalent to {@link SparqlClientFactory#createFor(SparqlEndpoint, RowParser, FragmentParser)}
     * on {@link FasterSparql#factory(String...)}
     *
     * @param endpoint the {@link SparqlEndpoint} to be queried
     * @param rowParser {@link RowParser} to process rows of SPARQL results
     * @param fragmentParser {@link FragmentParser} to process fragments of RDF graphs
     * @param <R> type of SPARQL result rows (i.e. solutions) exposed by the {@code rowParser}
     * @param <F> type of RDF graph fragments exposed by the {@code fragmentParser}
     * @return a new, non-null {@link SparqlClient} whose ownership is given to the caller.
     */
    public static <R, F> SparqlClient<R, F>
    clientFor(SparqlEndpoint endpoint, RowParser<R> rowParser, FragmentParser<F> fragmentParser) {
        return factory().createFor(endpoint, rowParser, fragmentParser);
    }

    /**
     * Creates a {@link SparqlClient} for the given {@link SparqlEndpoint}.
     *
     * @param endpoint the SPARQL endpoint to receive the queries.
     * @return a new, non-null {@link SparqlClient} whose ownership is given to the caller.
     * @throws UnacceptableSparqlConfiguration if {@code endpoint.configuration()} is unfeasible,
     *         i.e., any query with the {@link SparqlClient} would throw this exception
     */
    public static SparqlClient<String[], byte[]>
    clientFor(SparqlEndpoint endpoint) {
        return factory().createFor(endpoint);
    }

    /**
     * Creates a {@link SparqlClient} for the endpoint at {@code augmentedUri} with given parsers.
     *
     * @param augmentedUri The URI of the SPARQL endpoint, optionally prefixed with configurations
     *                     to be embedded in {@link SparqlEndpoint#configuration()}.
     *                     See {@link SparqlEndpoint#parse(String)}.
     * @param rowParser a parser to convert rows of ASK/SELECT queries
     * @param fragmentParser a parser to convert fragments of graphs returned from
     *                       CONSTRUCT/DESCRIBE queries
     * @param <R> the type of rows
     * @param <F> the type of graph fragments
     * @return a new, non-null {@link SparqlClient} whose ownership is given to the caller.
     * @throws UnacceptableSparqlConfiguration if {@code endpoint.configuration()} is unfeasible,
     *         i.e., any query with the {@link SparqlClient} would throw this exception
     */
    public static <R, F> SparqlClient<R, F>
    clientFor(String augmentedUri, RowParser<R> rowParser, FragmentParser<F> fragmentParser) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri), rowParser, fragmentParser);
    }

    /**
     * Creates a {@link SparqlClient} for the endpoint at {@code augmentedUri}.
     *
     * @param augmentedUri The URI of the SPARQL endpoint, optionally prefixed with configurations
     *                     to be embedded in {@link SparqlEndpoint#configuration()}.
     *                     See {@link SparqlEndpoint#parse(String)}.
     * @return a new, non-null {@link SparqlClient} whose ownership is given to the caller.
     * @throws UnacceptableSparqlConfiguration if {@code endpoint.configuration()} is unfeasible,
     *         i.e., any query with the {@link SparqlClient} would throw this exception
     */
    public static SparqlClient<String[], byte[]>
    clientFor(String augmentedUri) {
        return factory().createFor(SparqlEndpoint.parse(augmentedUri));
    }
}
