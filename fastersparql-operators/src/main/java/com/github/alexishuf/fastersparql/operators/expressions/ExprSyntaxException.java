package com.github.alexishuf.fastersparql.operators.expressions;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter @Accessors(fluent = true)
public class ExprSyntaxException extends ExprCompilerException {
    private final String expr;

    public ExprSyntaxException(String expr, String message) {
        super(message);
        this.expr = expr;
    }
}
