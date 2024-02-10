package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.batch.BItCancelledException;
import com.github.alexishuf.fastersparql.batch.BItReadCancelledException;
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

    public static boolean isCancel(Throwable t) {
        return t instanceof BItCancelledException || t instanceof BItReadCancelledException
                || t instanceof FSCancelledException;
    }
}
