package com.github.alexishuf.fastersparql.client.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the server or with the network connecting us to the server.
 */
public class SparqlClientServerException extends SparqlClientException {
    private boolean shouldRetry;

    public static SparqlClientException wrap(SparqlEndpoint endpoint, Throwable t) {
        if (t == null) {
            return null;
        } else if (t instanceof SparqlClientException ce) {
            ce.offerEndpoint(endpoint);
            return ce;
        } else {
            return new SparqlClientServerException(endpoint, t.getMessage(), t);
        }
    }

    public SparqlClientServerException(String message) { this(null, message, null); }
    public SparqlClientServerException(SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }
    public SparqlClientServerException(SparqlEndpoint endpoint, String message,
                                       @Nullable Throwable cause) {
        super(endpoint, message, cause);
    }

    public boolean shouldRetry() { return shouldRetry; }

    public SparqlClientServerException shouldRetry(boolean value) {
        shouldRetry = value;
        return this;
    }
}
