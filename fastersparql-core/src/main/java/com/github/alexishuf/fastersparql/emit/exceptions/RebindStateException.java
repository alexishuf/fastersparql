package com.github.alexishuf.fastersparql.emit.exceptions;

import com.github.alexishuf.fastersparql.emit.async.Stateful;

public class RebindStateException  extends RebindException {
    public RebindStateException(Object e) {
        super(e+" has received a request() and has not delivered termination downstream"
                +(e instanceof Stateful s ? ". state="+s.stateName() : "") );
    }
}
