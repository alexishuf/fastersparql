package com.github.alexishuf.fastersparql.sparql.expr;

public class InvalidExprTypeException extends ExprEvalException {
    public InvalidExprTypeException(String message) {
        super(message);
    }

    public InvalidExprTypeException(Expr expr, Expr value, String expectedType) {
        super("Expected "+expectedType+", but expression "+expr+" evaluated to "+value);
    }
}
