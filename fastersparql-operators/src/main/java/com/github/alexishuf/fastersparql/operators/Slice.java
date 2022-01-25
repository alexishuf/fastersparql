package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;
import org.checkerframework.checker.index.qual.NonNegative;
import org.reactivestreams.Subscriber;

public interface Slice extends Operator {
    default OperatorName name() { return OperatorName.SLICE; }

    /**
     * Select at most {@code limit} rows after {@code offset} from the input.
     *
     * @param input the input result set
     * @param offset first row to include. Must be >= 0, else will silently treat as 0.
     * @param limit maximum number of rows to include. Must be >= 0, else will silently treat as 0.
     * @param <R> row type
     * @return a non-null {@link Results} with same {@link Results#vars()} but
     *         whose {@link Results#publisher()} will emit at most {@code limit} rows
     *         starting from the {@code offset}-th row of {@code input}
     * @throws IllegalArgumentException if {@code offset} or {@code limit} are negative.
     */
    default <R> Results<R> checkedRun(Results<R> input, long offset, long limit) {
        if (input == null)
            throw new NullPointerException("input is null");
        if (offset < 0)
            throw new IllegalArgumentException("Negative offset="+offset);
        if (limit < 0)
            throw new IllegalArgumentException("Negative limit="+limit);
        return run(input, offset, limit);
    }

    /**
     * Similar to {@link Slice#checkedRun(Results, long, long)}, but negative {@code offset}
     * or {@code limit} will silently generate empty {@link Results} (no exception will be
     * thrown or reported via {@link Subscriber#onError(Throwable)}
     */
    <R> Results<R> run(Results<R> input, @NonNegative long offset, @NonNegative long limit);
}
