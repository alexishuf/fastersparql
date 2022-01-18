package com.github.alexishuf.fastersparql.client.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;

public class SparqlClientException extends RuntimeException {
    public SparqlClientException(String message) {
        super(message);
    }

    public SparqlClientException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
