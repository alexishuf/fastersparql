package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.client.parser.row.RowParser;
import com.github.alexishuf.fastersparql.client.parser.row.StringArrayRowParser;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A factory for {@link SparqlClient} instances.
 */
public interface SparqlClientFactory {
    /**
     * A unique identifier (among {@link SparqlClientFactory} implementating classes).
     *
     * This exists only because writing the fully qualified class name of the preferred
     * implementations in {@link FasterSparql#factory(String...)} and other methods would
     * be boring.
     *
     * @return The tag for this {@link SparqlClientFactory} implementation.
     */
    @Pure String tag();

    /**
     * If there is a tie in {@link FasterSparql#factory(String...)} and related methods,
     * the lowest-order factory wins.
     *
     * @return any integer, including negative values.
     */
    @Pure int order();

    /**
     * Create a new {@link SparqlEndpoint} querying the given endpoint.
     *
     * @param endpoint the SPARQL URI and allowed methods
     * @param rowParser the {@link RowParser} to be used with
     *                  {@link SparqlClient#query(CharSequence, SparqlConfiguration)}
     * @param fragmentParser the {@link FragmentParser} to be used with
     *                       {@link SparqlClient#queryGraph(CharSequence, SparqlConfiguration)}
     * @return a new, non-null {@link SparqlEndpoint} querying the given endpoint.
     *         Ownership is passed to the caller, which must eventually call
     *         {@link SparqlClient#close()}.
     */
    <R, F> SparqlClient<R, F> createFor(SparqlEndpoint endpoint, RowParser<R> rowParser,
                                        FragmentParser<F> fragmentParser);

    default SparqlClient<String[], byte[]> createFor(SparqlEndpoint endpoint) {
        return createFor(endpoint, StringArrayRowParser.INSTANCE, ByteArrayFragmentParser.INSTANCE);
    }
}
