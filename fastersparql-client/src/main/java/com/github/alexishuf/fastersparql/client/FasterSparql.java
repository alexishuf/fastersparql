package com.github.alexishuf.fastersparql.client;

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
     * whose {@link SparqlClientFactory#tag()} or {@link SparqlClientFactory#getClass()} appears
     * first in the given {@code preferredTagsOrClassNames} list.
     *
     * @param preferredTagsOrClassNames a list of {@link SparqlClientFactory#tag()}s,
     *                                  {@link Class#getName()} and {@link Class#getSimpleName()}.
     *                                  The earlier an entry appears, the higher its precedence.
     * @return a non-null {@link SparqlClientFactory} whose ownership is transferred to the caller,
     *         who must then call {@link SparqlClient#close()}. The returned {@link SparqlClient}
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
     * @param <Row> type of SPARQL result rows (i.e. solutions) exposed by the {@code rowParser}
     * @param <Fragment> type of RDF graph fragments exposed by the {@code fragmentParser}
     * @return a non-null ready to use {@link SparqlClient} targetting the given {@code endpoint}.
     *         Ownership is given to the caller, which must eventually call
     *         {@link SparqlClient#close()}
     */
    public static <Row, Fragment> SparqlClient<Row, Fragment>
    clientFor(SparqlEndpoint endpoint, RowParser<Row> rowParser,
              FragmentParser<Fragment> fragmentParser) {
        return factory().createFor(endpoint, rowParser, fragmentParser);
    }

    /**
     * Equivalent to {@link FasterSparql#clientFor(SparqlEndpoint, RowParser, FragmentParser)}
     * but with default row and fragment parsers.
     *
     * @param endpoint the SPARQL endpoint to receive the queries.
     * @return see {@link FasterSparql#clientFor(SparqlEndpoint, RowParser, FragmentParser)}.
     */
    public static SparqlClient<String[], byte[]>
    clientFor(SparqlEndpoint endpoint) {
        return factory("netty").createFor(endpoint);
    }
}
