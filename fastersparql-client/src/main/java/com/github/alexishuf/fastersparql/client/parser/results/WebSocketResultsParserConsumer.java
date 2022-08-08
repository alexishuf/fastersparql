package com.github.alexishuf.fastersparql.client.parser.results;

import org.reactivestreams.Subscription;

public interface WebSocketResultsParserConsumer extends ResultsParserConsumer {
    /**
     * Called when the server sends a {@code !bind-request n} control message.
     *
     * @param n the number of binding rows requested by the server.
     * @param incremental if a {@code +} was prefixed on n, indicating that the requested amount
     *                    must be treated as an additional request not overwriting previous
     *                    requests. The server only omits the {@code +} prefix on the first
     *                    {@code !bind-request} following a client-sent {@code !bind} message, thus
     *                    if {@code n} is being used on {@link Subscription#request(long)},
     *                    {@code incremental} can be safely ignored.
     */
    void bindRequest(long n, boolean incremental);

    /**
     * Called when the server sends a {@code !active-binding} message, which reports the binding
     * previously sent by the client that originates the following rows in response to a
     * {@code !bind} request.
     *
     * @param row a repetition of the binding row previously sent by the client: a
     *            {@code \t}-separated of RDF terms in N-Triples syntax.
     */
    void activeBinding(String[] row);

    /**
     * Called when the server has sent a {@code !action-queue-cap=n} control message.
     * @param n the capacity of the server action queue
     */
    void actionQueue(int n);

    /**
     * The server sent a {@code !cancelled} message.
     * An {@link ResultsParserConsumer#end()} will nevertheless follow
     */
    void cancelled();
}
