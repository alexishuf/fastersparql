package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.async.Async;
import com.github.alexishuf.fastersparql.operators.errors.IllegalOperatorArgumentException;
import com.github.alexishuf.fastersparql.operators.impl.OperatorHelpers;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface Project extends Operator {
    default OperatorName name() { return OperatorName.PROJECT; }

    /**
     * Perform a projection, returning a {@link Results} with the given variables.
     *
     * Additional rules:
     * <ul>
     *     <li>The order of variables in {@code vars} is retained.</li>
     *     <li>If a variable appears more than once, only the first use will be
     *         taken into consideration.</li>
     *     <li>If a variable in {@code vars} does not occur in {@code input.vars()}, it will
     *         be included in the output but all rows will have {@code null}s as value.</li>
     * </ul>
     *
     * @param input the input {@link Results}
     * @param vars {@link Results#vars()} of the returned {@link Results}.
     * @param <R> the row type
     * @return a non-null {@link Results} with the given vars
     *
     * @throws IllegalOperatorArgumentException if there are {@code null}s in {@code vars}
     */
    <R> Results<R> checkedRun(Results<R> input, List<String> vars);

    /**
     * Same as {@link Project#checkedRun(Results, List)}, but any {@link Throwable} is wrapped in
     * the returned an {@link Results}.
     */
    default <R> Results<R> run(Results<R> input, List<String> vars) {
        try {
            return checkedRun(input, vars);
        } catch (Throwable t) {
            List<String> sanitized = vars.stream().filter(Objects::nonNull).distinct()
                                         .collect(Collectors.toList());
            return OperatorHelpers.errorResults(input, Async.wrap(sanitized), null, t);
        }
    }
}