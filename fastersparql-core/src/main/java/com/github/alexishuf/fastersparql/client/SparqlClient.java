package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQuery;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.row.RowType;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics.JoinMetrics;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link SparqlClient} allows submitting queries to remote SPARQL endpoints.
 *
 * <p>Implementations are expected asynchronously send the request and to asynchronously handle the
 * responses, delivering rows or fragments of the processed responses ASAP to the user. That is,
 * a call to {@link SparqlClient#query(RowType, SparqlQuery)} (or other query methods) should return even
 * before the request leaves into the network and results should be available without waiting
 * for the whole server response to arrive.</p>
 *
 * <p>This interface is generic to allow configurable parsing of query results. Not every user of
 * this interface would like to receive a {@code Binding} from Apache Jena for each solution of
 * a SPARQL SELECT, as there are other choices in jena, RDF4J, hdt-java and so on.</p>
 *
 */
public interface SparqlClient extends AutoCloseable {
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
     *     <li>{@link FSServerException} if the server returns a non-200 response</li>
     *     <li>{@link InvalidSparqlResultsException} if the server-sent results are serialized
     *         in some violation of the standard such that the parser cannot safely guess
     *         the intent.</li>
     * </ul>
     *
     * @param rowType  {@link RowType} of resulting {@link BIt}. If {@code bindings != null}, MUST
     *                 be equals to {@code bindings.rowType()}.
     * @param sparql   the SPARQL query.
     * @param bindings A {@link BIt} over rows of the left side of the bind operation. For each
     *                 such row, {@code sparql} will be executed with vars set in the left
     *                 row assigned to the value in that row. All solutions for the bound
     *                 {@code sparql} are merged with the original left row in accordance to
     *                 the requested bind {@code type}
     * @param type     The semantics for the bind operation to be done with the given
     *                 {@code bindings}. Can be null if, and only if {@code bindings == null}
     * @param metrics  optional {@link JoinMetrics} that will receive events from the returned
     *                    {@link BIt}.
     * @return a {@link BIt} over the solutions
     * @throws NullPointerException if only one among {@code bindings} and {@code type}  is null.
     */
    <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql,
                     @Nullable BIt<R> bindings, @Nullable BindType type,
                     @Nullable JoinMetrics metrics);

    /** Equivalent to {@code query(rowType, sparql, bindings, type, null)}. */
    <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql,
                     @Nullable BIt<R> bindings, @Nullable BindType type);

    /**
     * Whether {@link SparqlClient#query(RowType, SparqlQuery, BIt, BindType)}
     * uses a protocol extension that allows more efficient execution of bind-based joins,
     * {@code OPTIONAL}, {@code FILTER EXISTS} and {@code MINUS} SPARQL operators.
     */
    boolean usesBindingAwareProtocol();

    /** Equivalent to {@code query(sparql, null, null)}. */
    default <R> BIt<R> query(RowType<R> rowType, SparqlQuery sparql)  {
        return query(rowType, sparql, null, null);
    }

    /**
     * Execute a CONSTRUCT or DESCRIBE SPARQL query and obtain fragments of the RDF serialization
     * as they arrive.
     *
     * <p>Likely exceptions</p>:
     * <ul>
     *     <li>{@link InvalidSparqlQuery} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link FSServerException} if the server returns a non-200 response</li>
     * </ul>
     *
     * @param sparql the SPARQL CONSTRUCT or DESCRIBE query
     * @return A {@link Graph}.
     */
    Graph queryGraph(SparqlQuery sparql);

    /**
     * Closes the client, releasing all resources.
     *
     * <p>Subsequent query() calls will fail immediately. Not fully consumed {@link BIt} spawned
     * from this client may complete normally or raise an {@link FSException}.</p>
     */
    @Override void close();
}
