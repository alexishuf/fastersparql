package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FSCancelledException extends FSException {
    public FSCancelledException() { this(null); }
    public FSCancelledException(@Nullable SparqlEndpoint endpoint) {
        this(endpoint, "Query was cancelled before completion");
    }

    public FSCancelledException(@Nullable SparqlEndpoint endpoint, String message) {
        super(endpoint, message);
    }
}
