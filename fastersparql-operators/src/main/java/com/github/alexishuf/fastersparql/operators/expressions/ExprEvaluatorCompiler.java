package com.github.alexishuf.fastersparql.operators.expressions;

import com.github.alexishuf.fastersparql.client.model.row.NoRowOperationsException;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.model.row.RowOperationsRegistry;

import java.util.List;

public interface ExprEvaluatorCompiler {
    /**
     * Creates a {@link ExprEvaluator} that evaluates {@code sparqlExpression} using
     * bindings taken from rows of type {@code R}
     *
     * @param rowClass the class of row instances the resulting evaluator will receive.
     * @param rowOperations {@link RowOperations}, used to get values out of rows
     * @param rowVarNames the list of variable names in rows that will be given to the evaluator.
     *                    The order must match the order of values in the rows.
     * @param sparqlExpression The SPARQL expression to evaluate.
     * @param <R> the row type
     * @return A new {@link ExprEvaluator}.
     */
    <R> ExprEvaluator<R> compile(Class<? super R> rowClass, RowOperations rowOperations,
                                 List<String> rowVarNames,
                                 CharSequence sparqlExpression);

    /**
     * Shorthand for {@link ExprEvaluatorCompiler#compile(Class, RowOperations, List, CharSequence)}
     * taking the {@link RowOperations} from {@link RowOperationsRegistry}.
     *
     * @throws NoRowOperationsException if there is no {@link RowOperations} registered for
     *                                  {@code rowClass}
     */
    default <R> ExprEvaluator<R> compile(Class<? super R> rowClass, List<String> rowVarNames,
                                         CharSequence sparqlExpression)
            throws NoRowOperationsException {
        RowOperations ro = RowOperationsRegistry.get().forClass(rowClass);
        return compile(rowClass, ro, rowVarNames, sparqlExpression);
    }
}
