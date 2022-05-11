package com.github.alexishuf.fastersparql.operators.providers;

import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.operators.BidCosts;
import com.github.alexishuf.fastersparql.operators.Operator;
import com.github.alexishuf.fastersparql.operators.OperatorFlags;
import com.github.alexishuf.fastersparql.operators.OperatorName;
import org.checkerframework.checker.index.qual.NonNegative;

public interface OperatorProvider {
    /**
     * The {@link OperatorName} for produced by {@link OperatorProvider#create(long, RowOperations)}.
     * @return a non-null {@link OperatorName}
     */
    OperatorName operatorName();

    /**
     * Propose a cost for creating an operator given the flags (see {@link OperatorFlags}.
     *
     * If creating an operator for the given flags is unfeasible, {@link Integer#MAX_VALUE}
     * should be returned. Else, see {@link BidCosts} for reference values making up a bid.
     * An implementation is not required to follow all guidelines in {@link BidCosts}.
     *
     * @param flags or-ed set of bit flags from {@link OperatorFlags}
     * @return a non-negative cost for the operator or {@link Integer#MAX_VALUE}.
     */
    @NonNegative int bid(long flags);

    /**
     * Create a new {@link Operator} with given flags and the given {@link RowOperations}
     *
     * @param flags or-ed set of flags from {@link OperatorFlags}
     * @param rowOperations A {@link RowOperations} object to be used by the {@link Operator}.
     *                      This limits the class of rows that the operator can handle to rows
     *                      supported by this object.
     * @return a new {@link Operator}.
     * @throws IllegalArgumentException if {@link OperatorProvider#bid(long)} for flags
     *         is {@link Integer#MAX_VALUE}
     */
    Operator create(long flags, RowOperations rowOperations);
}
