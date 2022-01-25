package com.github.alexishuf.fastersparql.operators;

import com.github.alexishuf.fastersparql.client.model.Results;

public interface Slice extends Operator {
    default OperatorName name() { return OperatorName.SLICE; }

    /**
     * Select at most {@code limit} rows after {@code offset} from the input.
     *
     * @param input the input result set
     * @param offset first row to include
     * @param limit maximum number of rows to include
     * @param <R> row type
     * @return a non-null {@link Results} with same {@link Results#vars()} but
     *         whose {@link Results#publisher()} will emit at most {@code limit} rows
     *         starting from the {@code offset}-th row of {@code input}
     */
    <R> Results<R> run(Results<R> input, long offset, long limit);
}
