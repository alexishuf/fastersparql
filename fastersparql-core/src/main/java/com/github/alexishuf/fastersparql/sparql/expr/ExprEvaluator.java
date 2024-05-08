package com.github.alexishuf.fastersparql.sparql.expr;

import com.github.alexishuf.fastersparql.batch.type.Batch;

/**
 * An evaluator evaluates some {@link Expr} replacing vars with values from a given
 * row in a {@link Batch}.
 *
 * <p>Evaluators are stateful and optimized for repeated evaluation during. The same evaluator
 * must not be used concurrently by two threads. It also must not be evaluated recursively (i.e.,
 * evaluation of a sub-evaluator cannot invoke {@link #evaluate(Batch, int)} on an evaluator which
 * triggered its {@link #evaluate(Batch, int)} call. Implementations do not check for concurrency
 * or reentrancy, thus their presence might lead to unpredictable outcomes. </p>
 */
public interface ExprEvaluator extends AutoCloseable {
    Term evaluate(Batch<?> batch, int row);

    @Override void close();
}
