package com.github.alexishuf.fastersparql.client.util.async;

import java.util.concurrent.ExecutionException;

/**
 * Equivalent to an {@link ExecutionException}, but unchecked
 */
public class RuntimeExecutionException extends RuntimeException {
    /**
     * Wraps a Throwable or converts an {@link ExecutionException}.
     * @param cause the exception to be wrapped.
     */
    public RuntimeExecutionException(Throwable cause) {
        super(cause instanceof ExecutionException ? cause.getCause() : cause);
    }

    /**
     * Converts a {@link ExecutionException} into an unchecked exception.
     * @param executionException the {@link ExecutionException}.
     */
    public RuntimeExecutionException(ExecutionException executionException) {
        this(executionException.getCause());
    }
}
