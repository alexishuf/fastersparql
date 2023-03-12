package com.github.alexishuf.fastersparql.sparql.expr;

public class UnboundVarException extends InvalidExprTypeException {
    public UnboundVarException(Object v) {
        super("Tried to get value of unbound var "+v);
    }
}
