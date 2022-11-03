package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.exceptions.*;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.parser.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SparqlClient} allows submitting queries to remote SPARQL endpoints.
 *
 * <p>Implementations are expected asynchronously send the request and to asynchronously handle the
 * responses, delivering rows or fragments of the processed responses ASAP to the user. That is,
 * a call to {@link SparqlClient#query(SparqlQuery)} (or other query methods) should return even
 * before the request leaves into the network and results should be available without waiting
 * for the whole server response to arrive.</p>
 *
 * <p>This interface is generic to allow configurable parsing of query results. Not every user of
 * this interface would like to receive a {@code Binding} from Apache Jena for each solution of
 * a SPARQL SELECT, as there are other choices in jena, RDF4J, hdt-java and so on.</p>
 *
 * @param <R> type used to represent individual solutions within a results set.
 * @param <F> type used to represent the fragment of a serialized RDF graph
 */
public interface SparqlClient<R, I, F> extends AutoCloseable {
    /** Methods to manipulate rows produced by this client. */
    RowType<R, I> rowType();

    /** The {@link Class} for the {@code R} type parameter. */
    default Class<R> rowClass() { return rowType().rowClass(); }

    /** The {@link Class} for the {@code F} type parameter. */
    Class<F> fragmentClass();

    /** The endpoint queried by this {@link SparqlClient}. */
    SparqlEndpoint endpoint();

    /**
     * Execute a SELECT or ASK query and retrieve a {@link BIt} over the solutions (rows)
     * of the query.
     *
     * <p>{@code ASK} queries by definition will produce one zero-column row for a positive
     * result and no row for a negative result.</p>
     *
     * <p>Likely exceptions:</p>
     * <ul>
     *     <li>{@link InvalidSparqlQuery} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link SparqlClientServerException} if the server returns a non-200 response</li>
     *     <li>{@link InvalidSparqlResultsException} if the server-sent results are serialized
     *         in some violation of the standard such that the parser cannot safely guess
     *         the intent.</li>
     * </ul>
     *
     * @param sparql the SPARQL query.
     * @param bindings A {@link BIt} over rows of the left side of the bind operation. For each
     *                 such row, {@code sparql} will be executed with vars set in the left
     *                 row assigned to the value in that row. All solutions for the bound
     *                 {@code sparql} are merged with the original left row in accordance to
     *                 the requested bind {@code type}
     * @param type The semantics for the bind operation to be done with the given
     *                 {@code bindings}. Can be null if, and only if {@code bindings == null}
     * @return a {@link BIt} over the solutions
     * @throws NullPointerException if only one among {@code bindings} and {@code type}  is null.
     */
    BIt<R> query(SparqlQuery sparql, @Nullable BIt<R> bindings, @Nullable BindType type);

    /**
     * Whether {@link SparqlClient#query(SparqlQuery, BIt, BindType)}
     * uses a protocol extension that allows more efficient execution of bind-based joins,
     * {@code OPTIONAL}, {@code FILTER EXISTS} and {@code MINUS} SPARQL operators.
     */
    default boolean usesBindingAwareProtocol() {
        return false;
    }

    /** Equivalent to {@code query(sparql, null, null)}. */
    default BIt<R> query(SparqlQuery sparql)  { return query(sparql, null, null); }

    /**
     * Execute a CONSTRUCT or DESCRIBE SPARQL query and obtain fragments of the RDF serialization
     * as they arrive.
     *
     * <p>Likely exceptions</p>:
     * <ul>
     *     <li>{@link InvalidSparqlQuery} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link SparqlClientServerException} if the server returns a non-200 response</li>
     * </ul>
     *
     * @param sparql the SPARQL CONSTRUCT or DESCRIBE query
     * @return A {@link Graph}.
     */
    Graph<F> queryGraph(SparqlQuery sparql);

    /**
     * Closes the client, releasing all resources.
     *
     * <p>Subsequent query() calls will fail immediately. Not fully consumed {@link BIt} spawned
     * from this client may complete normally or raise an {@link SparqlClientException}.</p>
     */
    @Override void close();
}
