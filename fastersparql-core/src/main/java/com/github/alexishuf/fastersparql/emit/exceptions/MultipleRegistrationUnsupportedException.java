package com.github.alexishuf.fastersparql.emit.exceptions;

public class MultipleRegistrationUnsupportedException extends IllegalEmitStateException {
    public MultipleRegistrationUnsupportedException(Object emitter) {
        super("Multiple subscriptions are not supported by "+emitter);
    }
}
