package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.batch.type.BatchType;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.emit.Emitter;
import com.github.alexishuf.fastersparql.exceptions.FSException;
import com.github.alexishuf.fastersparql.exceptions.FSServerException;
import com.github.alexishuf.fastersparql.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.sparql.DistinctType;
import com.github.alexishuf.fastersparql.sparql.InvalidSparqlException;
import com.github.alexishuf.fastersparql.sparql.SparqlQuery;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import com.github.alexishuf.fastersparql.sparql.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.util.owned.Orphan;

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
     * For every row of the given bindings, replace use the row to bind a SELECT or ASK query and
     * return the solutions of that bound query. The solutions to the bound query are returned in
     * sequence, yielding a single stream of solutions. Solutions to the bound query will include
     * the values of the row used as binding in its leftmost columns.
     *
     * <p>See {@link com.github.alexishuf.fastersparql.model.BindType#resultVars(Vars, Vars)} for
     * how the set of vars in the returned {@link BIt} is determined.</p>
     *
     * <p>All {@link SparqlClient} implementations support this method. Clients for which
     * {@link #usesBindingAwareProtocol()} is true will, in general, produce more efficient
     * iterators (i.e., lower garbage collection load and faster wall-clock time when draining
     * the iterator until its end.</p>
     *
     * @param bindQuery A specification of the bind operation and maybe a listener of the
     *                  {@link ItBindQuery#emptyBinding(long)} and
     *                  {@link ItBindQuery#nonEmptyBinding(long)} events.
     * @return a {@link BIt} over the solutions
     * @throws NullPointerException if only one among {@code bindings} and {@code type}  is null.
     */
    <B extends Batch<B>> BIt<B> query(ItBindQuery<B> bindQuery);

    /**
     * Whether {@link SparqlClient#query(ItBindQuery)} uses a protocol extension that allows more
     * efficient execution of bind-based joins, {@code OPTIONAL}, {@code FILTER EXISTS}
     * and {@code MINUS} SPARQL operators.
     */
    boolean usesBindingAwareProtocol();

    /** Cheapest {@link DistinctType} supported by the server. */
    DistinctType cheapestDistinct();

    /**
     * Whether this {@link SparqlClient} executes queries within this JVM process against an
     * in-memory collection of triples or a local file.
     */
    boolean isLocalInProcess();

    /**
     * Execute a SELECT or ASK query and retrieve a {@link BIt} over the solutions (rows)
     * of the query.
     *
     * <p>{@code ASK} queries by definition will produce one zero-column row for a positive
     * result and no row for a negative result.</p>
     *
     * <p>Likely exceptions:</p>
     * <ul>
     *     <li>{@link InvalidSparqlException} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link FSServerException} if the server returns a non-200 response</li>
     *     <li>{@link InvalidSparqlResultsException} if the server-sent results are serialized
     *         in some violation of the standard such that the parser cannot safely guess
     *         the intent.</li>
     * </ul>
     *
     * @param batchType the type of batches to be produced
     * @param sparql The SPARQL query to execute.
     * @return a {@link BIt} of the requested type over solutions for {@code sparql}.
     * @throws NullPointerException if only one among {@code bindings} and {@code type}  is null.
     */
    <B extends Batch<B>> BIt<B> query(BatchType<B> batchType, SparqlQuery sparql);

    /**
     * Create an unstarted {@link Emitter} that will output batches with the solutions for
     * {@code sparql}.
     *
     * @param batchType the type of batches to be produced
     * @param sparql The SPARQL SELECT or ASK query to execute
     * @param rebindHint Likely set of vars in a future {@link Emitter#rebind(BatchBinding)}
     *                   call on the returned {@link Emitter} if no such call is planned or
     *                   if whether it will happen is unknown, this should be {@link Vars#EMPTY}
     * @return an {@link Emitter}
     * @param <B> the batch type
     */
    <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    emit(BatchType<B> batchType, SparqlQuery sparql, Vars rebindHint);

    /**
     * Analogous to {@link #query(ItBindQuery)} but returns an unstarted {@link Emitter}.
     *
     * @param bindQuery the specification fot the bind query.
     * @param rebindHint likely set of vars in future {@link Emitter#rebind(BatchBinding)}
     *                   calls on the returned {@link Emitter}. This shall be used only for
     *                   optimization purposes and future {@link Emitter#rebind(BatchBinding)}
     *                   calls might not occurr or use a diferent set of vars. This parameter
     *                   should be {@link Vars#EMPTY} if no {@link Emitter#rebind(BatchBinding)}
     *                   is expected or if whether a rebind will happen is not kown.
     * @return An unstarted {@link Emitter}
     * @param <B> the batch type
     */
    <B extends Batch<B>> Orphan<? extends Emitter<B, ?>>
    emit(EmitBindQuery<B> bindQuery, Vars rebindHint);

    /**
     * Closes the client, releasing all resources.
     *
     * <p>Subsequent query() calls will fail immediately. Not fully consumed {@link BIt} spawned
     * from this client may complete normally or raise an {@link FSException}.</p>
     */
    @Override void close();
}
