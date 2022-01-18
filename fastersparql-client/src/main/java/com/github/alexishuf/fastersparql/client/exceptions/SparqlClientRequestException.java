package com.github.alexishuf.fastersparql.client.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the request. This includes 400 responses from the server.
 */
public class SparqlClientRequestException extends SparqlClientException {
    public SparqlClientRequestException(String message) {
        super(message);
    }

    public SparqlClientRequestException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
