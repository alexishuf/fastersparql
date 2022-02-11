package com.github.alexishuf.fastersparql.operators.expressions;

/**
 * Evaluates some expression binding variables with values in a given row.
 *
 * @param <R> the row type
 */
public interface ExprEvaluator<R> {
    /**
     * Evaluates the expression for which this object was compiled after binding variables in the
     * expression with corresponding values in the given row.
     *
     * @param row the source of values for variables in the expression.
     * @return The result of the expression as an RDF term in N-Triples syntax
     */
    String evaluate(R row);
}
