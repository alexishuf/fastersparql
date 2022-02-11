package com.github.alexishuf.fastersparql.operators.expressions;

public class ExprCompilerException extends RuntimeException {
    public ExprCompilerException(String message) {
        super(message);
    }

    public ExprCompilerException(String message, Throwable cause) {
        super(message, cause);
    }
}
