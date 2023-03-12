package com.github.alexishuf.fastersparql.exceptions;

import com.github.alexishuf.fastersparql.client.model.SparqlConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with arguments provided to the client or to supporting classes,
 * such as {@link SparqlConfiguration}.
 */
public class FSInvalidArgument extends FSRequestException {
    public FSInvalidArgument(String message) {
        super(message);
    }

    public FSInvalidArgument(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
