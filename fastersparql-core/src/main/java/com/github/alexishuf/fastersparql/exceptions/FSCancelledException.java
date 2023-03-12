package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlEndpoint;
import org.checkerframework.checker.nullness.qual.Nullable;

public class FSCancelledException extends FSException {
    public FSCancelledException() { this(null); }
    public FSCancelledException(@Nullable SparqlEndpoint endpoint) {
        super(endpoint, "Query was cancelled before completion");
    }
}
