package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.model.row.types.ArrayRow;
import com.github.alexishuf.fastersparql.client.parser.fragment.ByteArrayFragmentParser;
import com.github.alexishuf.fastersparql.client.parser.fragment.FragmentParser;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A factory for {@link SparqlClient} instances.
 */
public interface SparqlClientFactory {
    /**
     * A unique identifier (among {@link SparqlClientFactory} implementing classes).
     *
     * <p>This exists only because writing the fully qualified class name of the preferred
     * implementations in {@link FS#factory(String...)} and other methods would
     * be boring.</p>
     *
     * @return The tag for this {@link SparqlClientFactory} implementation.
     */
    @SuppressWarnings("SameReturnValue") @Pure String tag();

    /**
     * If there is a tie in {@link FS#factory(String...)} and related methods,
     * the lowest-order factory wins.
     *
     * @return any integer, including negative values.
     */
    @Pure int order();

    /**
     * Create a new {@link SparqlEndpoint} querying the given endpoint.
     *
     * @param endpoint the SPARQL endpoint URI and {@link SparqlConfiguration}
     * @param rowType A {@link RowType} that represents both the type of rows and provides
     *                basic operations their manipulation. The default is {@link ArrayRow#STRING}
     * @param fragmentParser the {@link FragmentParser} to be used with
     *                       {@link SparqlClient#queryGraph(SparqlQuery)}. The default is
     *                       {@link ByteArrayFragmentParser}
     * @return a new, non-null {@link SparqlEndpoint} querying the given endpoint.
     *         Ownership is passed to the caller, which must eventually call
     *         {@link SparqlClient#close()}.
     */
    <R, I, F> SparqlClient<R, I, F> createFor(SparqlEndpoint endpoint, RowType<R, I> rowType,
                                              FragmentParser<F> fragmentParser);

    /** See {@link SparqlClientFactory#createFor(SparqlEndpoint, RowType, FragmentParser)} */
    default SparqlClient<String[], String, byte[]> createFor(SparqlEndpoint endpoint) {
        return createFor(endpoint, ArrayRow.STRING, ByteArrayFragmentParser.INSTANCE);
    }

    /** See {@link SparqlClientFactory#createFor(SparqlEndpoint, RowType, FragmentParser)} */
    default <R, I> SparqlClient<R, I, byte[]> createFor(SparqlEndpoint endpoint, RowType<R, I> rowType) {
        return createFor(endpoint, rowType, ByteArrayFragmentParser.INSTANCE);
    }

    /** See {@link SparqlClientFactory#createFor(SparqlEndpoint, RowType, FragmentParser)} */
    default <F> SparqlClient<String[], String, F> createFor(SparqlEndpoint endpoint,
                                                            FragmentParser<F> fragmentParser) {
        return createFor(endpoint, ArrayRow.STRING, fragmentParser);
    }
}
