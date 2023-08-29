package com.github.alexishuf.fastersparql.emit.exceptions;

public class RegisterAfterStartException extends IllegalEmitStateException {
    public RegisterAfterStartException(Object emitter) {
        super("subscribe()/register() after request(long) on "+emitter);
    }
}
