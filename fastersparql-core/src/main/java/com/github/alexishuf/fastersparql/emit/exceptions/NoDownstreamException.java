package com.github.alexishuf.fastersparql.emit.exceptions;

public class NoDownstreamException extends IllegalEmitStateException {
    public NoDownstreamException(Object emitter) {
        super("No downstream set for "+emitter);
    }
}
