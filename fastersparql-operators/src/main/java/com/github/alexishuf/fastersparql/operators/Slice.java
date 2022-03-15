package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.operators.plan.SlicePlan;
import org.reactivestreams.Subscriber;

public interface Slice extends Operator {
    default OperatorName name() { return OperatorName.SLICE; }

    /**
     * Create a plan for {@code run(input.execute(), offset, limit)}
     */
    default <R> SlicePlan<R> asPlan(Plan<R> input, long offset, long limit) {
        return new SlicePlan<>(rowClass(), this, input, offset, limit, null, null);
    }

    default <R> SlicePlan.SlicePlanBuilder<R> asPlan() {
        return SlicePlan.<R>builder().rowClass(rowClass()).op(this);
    }

    /**
     * Select at most {@code limit} rows after {@code offset} from the input.
     *
     * @param plan the {@link SlicePlan} to execute
     * @param <R> row type
     * @return a non-null {@link Results} with same {@link Results#vars()} but
     *         whose {@link Results#publisher()} will emit at most {@code limit} rows
     *         starting from the {@code offset}-th row of {@code input}
     * @throws IllegalArgumentException if {@code offset} or {@code limit} are negative.
     */
    default <R> Results<R> checkedRun(SlicePlan<R> plan) {
        long offset = plan.offset(), limit = plan.limit();
        if (plan.input() == null)
            throw new NullPointerException("input is null");
        if (offset < 0)
            throw new IllegalArgumentException("Negative offset="+offset);
        if (limit < 0)
            throw new IllegalArgumentException("Negative limit="+limit);
        return run(plan);
    }

    /**
     * Similar to {@link Slice#checkedRun(SlicePlan)}, but negative {@code offset}
     * or {@code limit} will silently generate empty {@link Results} (no exception will be
     * thrown or reported via {@link Subscriber#onError(Throwable)}
     */
    <R> Results<R> run(SlicePlan<R> plan);
}
