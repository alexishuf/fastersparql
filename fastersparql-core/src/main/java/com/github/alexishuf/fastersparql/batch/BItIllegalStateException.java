package com.github.alexishuf.fastersparql.batch;

public class BItIllegalStateException extends IllegalStateException {
    public BItIllegalStateException(String s) { super(s); }

    @Override public String toString() {
        return getClass().getSimpleName()+": "+getMessage();
    }
}
