package com.github.alexishuf.fastersparql.emit.exceptions;

public class RebindStateException  extends RebindException {
    public RebindStateException(Object e) {
        super(e+" has received a request() and has not delivered termination downstream" );
    }
}
