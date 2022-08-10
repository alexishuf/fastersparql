package com.github.alexishuf.fastersparql.operators.expressions;

public class ExprSyntaxException extends ExprCompilerException {
    private final String expr;

    public ExprSyntaxException(String expr, String message) {
        super(message);
        this.expr = expr;
    }

    @SuppressWarnings("unused")
    public String expr() { return expr; }
}
