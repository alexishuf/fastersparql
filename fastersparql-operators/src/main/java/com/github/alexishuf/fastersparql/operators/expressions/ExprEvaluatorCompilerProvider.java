package com.github.alexishuf.fastersparql.operators.expressions;

public interface ExprEvaluatorCompilerProvider {
    /**
     * Get a {@link ExprEvaluatorCompiler}
     */
    ExprEvaluatorCompiler get();

    /**
     * The preferred order for this provider. Lower-order providers will be preferred.
     */
    int order();

    /**
     * The provider name.
     */
    String name();
}
