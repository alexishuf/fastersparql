package com.github.alexishuf.fastersparql.util;

public final class UnsetError extends RuntimeException {
    public static final UnsetError UNSET_ERROR = new UnsetError();
    public UnsetError() {
        super("Error has not been set");
    }
}
