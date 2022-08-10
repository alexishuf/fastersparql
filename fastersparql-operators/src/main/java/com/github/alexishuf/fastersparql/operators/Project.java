package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.errors.IllegalOperatorArgumentException;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.ProjectPlan;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface Project extends Operator {
    default OperatorName name() { return OperatorName.PROJECT; }

    /**
     * Create a plan for {@code run(input.execute(), vars)}
     */
    default <R> ProjectPlan<R> asPlan(Plan<R> input, List<String> vars) {
        return new ProjectPlan<>(this, input, vars, null, null);
    }

    default <R> ProjectPlan.Builder<R> asPlan() { return ProjectPlan.builder(this); }

    /**
     * Perform a projection, returning a {@link Results} with the given variables.
     *
     * <p>Additional rules:</p>
     * <ul>
     *     <li>The order of variables in {@code vars} is retained.</li>
     *     <li>If a variable appears more than once, only the first use will be
     *         taken into consideration.</li>
     *     <li>If a variable in {@code vars} does not occur in {@code input.vars()}, it will
     *         be included in the output but all rows will have {@code null}s as value.</li>
     * </ul>
     *
     * @param plan the {@link ProjectPlan} to execute
     * @param <R> the row type
     * @return a non-null {@link Results} with the given vars
     *
     * @throws IllegalOperatorArgumentException if there are {@code null}s in {@code vars}
     */
    <R> Results<R> checkedRun(ProjectPlan<R> plan);

    /**
     * Same as {@link Project#checkedRun(ProjectPlan)}, but any {@link Throwable} is wrapped in
     * the returned an {@link Results}.
     */
    default <R> Results<R> run(ProjectPlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            List<String> sanitized = plan.publicVars().stream().filter(Objects::nonNull).distinct()
                                         .collect(Collectors.toList());
            return Results.error(sanitized, Object.class, t);
        }
    }
}
