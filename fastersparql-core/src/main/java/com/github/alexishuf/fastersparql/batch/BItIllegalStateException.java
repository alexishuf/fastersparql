package com.github.alexishuf.fastersparql.batch;

public class BItIllegalStateException extends IllegalStateException {
    private final BIt<?> it;
    public BItIllegalStateException(String s, BIt<?> it) { super(s); this.it = it;}
    public BIt<?> it() { return it; }
}
