package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the server or with the network connecting us to the server.
 */
public class FSServerException extends FSException {
    private boolean shouldRetry;

    public FSServerException(String message) { this(null, message, null); }
    public FSServerException(@Nullable SparqlEndpoint endpoint, String message) {
        this(endpoint, message, null);
    }
    public FSServerException(@Nullable SparqlEndpoint endpoint, String message,
                             @Nullable Throwable cause) {
        super(endpoint, message, cause);
    }

    public boolean shouldRetry() { return shouldRetry; }

    public FSServerException shouldRetry(boolean value) {
        shouldRetry = value;
        return this;
    }
}
