package com.github.alexishuf.fastersparql.client.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the server or with the network connecting us to the server.
 */
public class SparqlClientServerException extends SparqlClientException {
    public SparqlClientServerException(String message) {
        super(message);
    }

    public SparqlClientServerException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
