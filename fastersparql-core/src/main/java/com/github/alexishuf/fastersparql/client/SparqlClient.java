package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQuery;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;

/**
 * A {@link SparqlClient} allows submitting queries to remote SPARQL endpoints.
 *
 * <p>Implementations are expected asynchronously send the request and to asynchronously handle the
 * responses, delivering rows or fragments of the processed responses ASAP to the user. That is,
 * a call to {@link SparqlClient#query(BatchType, SparqlQuery)} (or other query methods) should return even
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

    /** Delays the effects of {@link SparqlClient#close()} until after {@link Guard#close()} */
    interface Guard extends AutoCloseable {
        /**
         * Closes or allows {@link SparqlClient#close()} to effectively close the client.
         *
         * <p>This method is idempotent.</p>
         */
        @Override void close();
    }

    /**
     * Get a Guard object that guarantees this {@link SparqlClient} will remain usable so long
     * {@link Guard#close()} is not called.
     *
     * <p>Callers <strong>MUST</strong> ensure that {@link Guard#close()} is eventually
     * called at least once for every {@code acquire()} call. {@link Guard#close()} is
     * idempotent, thus it can be called more than once</p>
     *
     * @return a gaurd object ensuring {@link SparqlClient} is alive.
     */
    Guard retain();

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
     * @param bindQuery A specification of the bind operation and maybe a listener of the
     *                  {@link BindQuery#emptyBinding(long)} and
     *                  {@link BindQuery#nonEmptyBinding(long)} events.
     * @return a {@link BIt} over the solutions
     * @throws NullPointerException if only one among {@code bindings} and {@code type}  is null.
     */
    <B extends Batch<B>> BIt<B> query(BindQuery<B> bindQuery);

    /**
     * Whether {@link SparqlClient#query(BindQuery)} uses a protocol extension that allows more
     * efficient execution of bind-based joins, {@code OPTIONAL}, {@code FILTER EXISTS}
     * and {@code MINUS} SPARQL operators.
     */
    boolean usesBindingAwareProtocol();

    /** Equivalent to {@code query(sparql, null, null)}. */
    <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql);

    /**
     * Closes the client, releasing all resources.
     *
     * <p>Subsequent query() calls will fail immediately. Not fully consumed {@link BIt} spawned
     * from this client may complete normally or raise an {@link FSException}.</p>
     */
    @Override void close();
}
