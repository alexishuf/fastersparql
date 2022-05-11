package com.github.alexishuf.fastersparql.client;

import com.github.alexishuf.fastersparql.client.exceptions.InvalidSparqlQuery;
import com.github.alexishuf.fastersparql.client.exceptions.InvalidSparqlQueryType;
import com.github.alexishuf.fastersparql.client.exceptions.SparqlClientServerException;
import com.github.alexishuf.fastersparql.client.exceptions.UnacceptableSparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.Graph;
import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import com.github.alexishuf.fastersparql.client.parser.results.InvalidSparqlResultsException;
import com.github.alexishuf.fastersparql.client.util.reactive.AsyncIterable;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link SparqlClient} allows submitting queries to remote SPARQL endpoints.
 *
 * Implementations are expected asynchronously send the request and to asynchronously handle the
 * responses, delivering rows or fragments of the processed responses ASAP to the user. That is,
 * a call to {@link SparqlClient#query(CharSequence)} (or other query methods) should return even
 * before the request leaves into the network and results should be available without waiting
 * for the whole server response to arrive.
 *
 * This interface is generic to allow configurable parsing of query results. Not every user of
 * this interface would like to receive a {@code Binding} from Apache Jena for each solution of
 * a SPARQL SELECT, as there are other choices in jena, RDF4J, hdt-java and so on.
 *
 * @param <R> type used to represent individual solutions within a results set.
 * @param <F> type used to represent the fragment of a serialzied RDF graph
 */
@MustCall("close")
public interface SparqlClient<R, F> extends AutoCloseable {

    /**
     * The {@link Class} for the {@code R} type parameter.
     */
    Class<R> rowClass();

    /**
     * The {@link Class} for the {@code F} type parameter.
     */
    Class<F> fragmentClass();

    /**
     * The endpoint queried by this {@link SparqlClient}.
     *
     * @return a non-null {@link SparqlEndpoint}.
     */
    SparqlEndpoint endpoint();

    /**
     * Execute a SELECT or ASK query and retrieve a {@link Publisher} for the
     * rows (header and solutions) in the results.
     *
     * Exceptions will be reported via {@link Subscriber#onError(Throwable)} by the
     * {@link Publisher} even if raised by the client code before the request is sent over
     * the network. If using {@link Results#iterable()}, such exceptions can be fetched via
     * {@link AsyncIterable#error()}.
     *
     * Likely exceptions:
     * <ul>
     *     <li>{@link InvalidSparqlQuery} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link SparqlClientServerException} if the server returns a non-200 response</li>
     *     <li>{@link InvalidSparqlResultsException} if the server-sent results are serialized
     *         in some violation of the standard such that the parser cannot safely guess
     *         the intent.</li>
     *     <li>{@link UnacceptableSparqlConfiguration} if {@code configuration} does not accept
     *         the {@link SparqlEndpoint#configuration()}</li>
     * </ul>
     *
     * @param sparql the SPARQL query.
     * @param configuration Additional configurations to apply when executing this query only.
     * @param bindings A {@link Results} object providing zero or more rows of variable assignments
     *                 for sparql. If this is non-null, the resulting rows will be obtained by a
     *                 the {@link Results} to be returned by this method is defined by
     *                 {@code bindType}
     * @param bindType The semantics for the bind operation to be done with the given
     *                 {@code bindings}. Can be null if, and only if {@code bindings == null}
     * @return a {@link Results} object wrapping the list of variables and a {@link Publisher}
     *         of solutions.
     * @throws NullPointerException if {@code bindings} and {@code bindType} null status does
     *                              not match: either both must be non-null or both must be null.
     */
    Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration,
                     @Nullable Results<R> bindings, @Nullable BindType bindType);

    /**
     * Indicates whether {@link SparqlClient#query(CharSequence, SparqlConfiguration, Results, BindType)}
     * uses a protocol extension that allows more efficient executing of the same query with
     * different bindings.
     *
     * This is not the case of the standard SPARQL protocol, thus most implementations return false.
     */
    default boolean usesBindingAwareProtocol() {
        return false;
    }

    /**
     * {@link SparqlClient#query(CharSequence, SparqlConfiguration, Results, BindType)} with
     * {@code bindings} and {@code bindType} set to {@code null}.
     */
    default Results<R> query(CharSequence sparql, @Nullable SparqlConfiguration configuration) {
        return query(sparql, configuration, null, null);
    }

    /**
     * {@link SparqlClient#query(CharSequence, SparqlConfiguration)} with {@code configuration}
     * set to null
     */
    default Results<R> query(CharSequence sparql)  {
        return query(sparql, null);
    }

    /**
     * Execute a CONSTRUCT or DESCRIBE SPARQL query and obtain fragments of the RDF serialization
     * as they arrive.
     *
     * Exceptions will be reported via {@link Subscriber#onError(Throwable)} by the
     * {@link Publisher} even if raised by the client code before the request is sent over
     * the network. If using {@link Results#iterable()}, such exceptions can be fetched via
     * {@link AsyncIterable#error()}.
     *
     * Likely exceptions:
     * <ul>
     *     <li>{@link InvalidSparqlQuery} if the {@link SparqlClient} implementation validates
     *         queries and the given query has a syntax error.</li>
     *     <li>{@link InvalidSparqlQueryType}: if the query is not a SELECT nor an ASK query</li>
     *     <li>{@link SparqlClientServerException} if the server returns a non-200 response</li>
     *     <li>{@link UnacceptableSparqlConfiguration} if {@code configuration} does not accept
     *         the {@link SparqlEndpoint#configuration()}</li>
     * </ul>
     *
     * @param sparql the SPARQL CONSTRUCT or DESCRIBE query
     * @param configuration A set of configurations to apply only for this query. This must be
     *                      within the bounds of the {@link SparqlClient#endpoint()}'s
     *                      {@link SparqlEndpoint#configuration()}.
     * @return A {@link Graph} wrapping the response media type and a {@link Publisher} of
     *         fragments of serialized resulting graph
     */
    Graph<F> queryGraph(CharSequence sparql, @Nullable SparqlConfiguration configuration);

    /**
     * {@link SparqlClient#queryGraph(CharSequence, SparqlConfiguration)} with null
     * {@link SparqlConfiguration} (use the one from {@link SparqlClient#endpoint()}.
     *
     * @param sparql the SPARQL CONSTRUCT or DESCRIBE query.
     * @return See {@link SparqlClient#queryGraph(CharSequence, SparqlConfiguration)}.
     */
    default Graph<F> queryGraph(CharSequence sparql) {
        return queryGraph(sparql, null);
    }


    /**
     * Closes the client, releasing all resources.
     *
     * Subsequent calls to the {@code query*()} methods will fail immediately (i.e., not via the
     * {@link Publisher}) with {@link IllegalStateException}.
     *
     * Active {@link Publisher}s spawned from this client may be interrupted and terminate
     * with {@link Subscriber#onError(Throwable)} after this method is called. Howerver,
     * implementations of {@code close} should not try to block until all active {@link Publisher}s
     * terminate.
     */
    @Override void close();
}
