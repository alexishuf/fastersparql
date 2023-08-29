package com.github.alexishuf.fastersparql.emit.exceptions;

public class RebindReleasedException extends RebindException {
    public RebindReleasedException(Object e) {
        super(e+" has been released, use rebindAcquire() to avoid this");
    }
}
