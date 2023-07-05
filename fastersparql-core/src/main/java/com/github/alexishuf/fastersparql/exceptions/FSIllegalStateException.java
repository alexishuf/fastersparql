package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FSIllegalStateException extends FSException {
    public FSIllegalStateException(String message) {
        super(message);
    }

    public FSIllegalStateException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }

    public FSIllegalStateException(@Nullable SparqlEndpoint endpoint, String message) {
        super(endpoint, message);
    }

    public FSIllegalStateException(@Nullable SparqlEndpoint endpoint, String message, @Nullable Throwable cause) {
        super(endpoint, message, cause);
    }
}
