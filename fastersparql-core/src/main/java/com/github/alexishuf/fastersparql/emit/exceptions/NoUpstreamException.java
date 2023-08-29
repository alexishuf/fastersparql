package com.github.alexishuf.fastersparql.emit.exceptions;

public class NoUpstreamException extends IllegalEmitStateException {
    public NoUpstreamException(Object receiver) {
        super("No upstream set for "+receiver);
    }
}
