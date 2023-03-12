package com.github.alexishuf.fastersparql.sparql.expr;

public class InvalidTermException extends InvalidExprException {
    public InvalidTermException(Object input, int pos, String reason) {
        super(input, pos, reason);
    }
}
