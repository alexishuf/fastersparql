package com.github.alexishuf.fastersparql.sparql.expr;

public class InvalidTermException extends InvalidExprException {
    public InvalidTermException(String input, int pos, String reason) {
        super(input, pos, reason);
    }
    public InvalidTermException(String input, int pos, char expected) {
        super(input, pos, "Expected '"+expected+"'");
    }
}
