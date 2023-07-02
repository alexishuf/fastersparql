package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.batch.BItClosedAtException;
import com.github.alexishuf.fastersparql.batch.BItReadClosedException;
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
        return t instanceof BItClosedAtException || t instanceof BItReadClosedException
                || t instanceof FSCancelledException;
    }
}
