package com.github.alexishuf.fastersparql.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Something is wrong with the request. This includes 400 responses from the server.
 */
public class FSRequestException extends FSException {
    public FSRequestException(String message) {
        super(message);
    }

    public FSRequestException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
