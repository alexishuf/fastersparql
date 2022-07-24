package com.github.alexishuf.fastersparql.client.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the server or with the network connecting us to the server.
 */
public class SparqlClientServerException extends SparqlClientException {
    public SparqlClientServerException(String message) { this(null, message, null); }
    public SparqlClientServerException(SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }
    public SparqlClientServerException(SparqlEndpoint endpoint, String message,
                                       @Nullable Throwable cause) {
        super(endpoint, message, cause);
    }

    public SparqlEndpoint endpoint() { return super.endpoint(); }
}
