package com.github.alexishuf.fastersparql.client.model.batch;

public class BItClosedException extends BItIllegalStateException {
    public BItClosedException(BIt<?> it) { super(it+": already close()d", it); }
}
