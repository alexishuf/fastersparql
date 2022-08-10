package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.MergePlan;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.UnionPlan;

import java.util.List;

public interface Merge extends Operator {
    default OperatorName name() { return OperatorName.MERGE; }

    default <R> MergePlan<R> asPlan(List<? extends Plan<R>> inputs) {
        return new MergePlan<>(this, inputs, null, null);
    }

    default <R> MergePlan.Builder<R> asPlan() { return MergePlan.builder(this); }

    /**
     * Create a {@link Results} with all rows {@code r} such that the following conditions hold:
     * <ol>
     *     <li>{@code r} is output by at least one input in {@code plan.inputs()}.</li>
     *     <li>If {@code r} appears {@code n} times in an input {@code i} in {@code plan.inputs()},
     *         then {@code r} appears at least {@code n} times in the {@link Results} returned by
     *         this method.</li>
     * </ol>
     *
     * Unlike {@link Union#checkedRun(UnionPlan)}, the above definition allows de-duplication of
     * rows across sources without requiring it to be done. However, like {@link Union},
     * deduplication among results from the same input are not allowed. The semantics defined here
     * violates the definition of Union in the SPARQL algebra (hence the name difference), but is
     * useful in federated scenarios where the same query is issued to multiple sources introducing
     * duplicate rows that would not exist had all data been previously materialized in a single
     * triple store.
     *
     * @param plan The {@link MergePlan} to execute
     * @param <R> The row type
     * @return a non-null {@link Results} whose publisher may be empty.
     */
    <R> Results<R> checkedRun(MergePlan<R> plan);

    /**
     * Same return as {@link Merge#checkedRun(MergePlan)}, but catches any {@link Throwable} and
     * delivers it via the {@link Results#publisher()}.
     *
     * @param plan The {@link MergePlan} to execute
     * @param <R> The row type
     * @return a non-null {@link Results} with results or with any {@link Throwable} thrown
     *         by {@link Merge#checkedRun(MergePlan)}.
     */
    default <R> Results<R> run(MergePlan<R> plan) {
        try {
            return checkedRun(plan);
        } catch (Throwable t) {
            return Results.error(rowClass(), t);
        }
    }
}
