package com.github.alexishuf.fastersparql.client.exceptions;

public class AsyncIterableCancelled extends IllegalStateException {
    public AsyncIterableCancelled() {
        super("AsyncIterator.cancel() has been called");
    }
}
