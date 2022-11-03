package com.github.alexishuf.fastersparql.sparql.expr;

public class UnboundVarException extends InvalidExprTypeException {
    public UnboundVarException(Term.Var v) {
        super("Tried to get value of unbound var "+v);
    }
}
