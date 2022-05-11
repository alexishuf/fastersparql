package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class PlanMerger<R> extends Merger<R> {
    private final Plan<R> right;

    public PlanMerger(PlanMerger<R> other) {
        super(other);
        this.right = other.right;
    }

    public PlanMerger(RowOperations rowOps, List<String> leftPublicVars, Plan<R> right,
                      BindType bindType) {
        super(rowOps, leftPublicVars, "", right.publicVars(), right.allVars(), bindType);
        this.right = right;
    }

    public Plan<R> right() { return right; }


    /**
     * Create a binding of the right-side operand with values from a left-side row.
     *
     * @param leftRow A row of results of the left-operand (must match the
     *                {@code leftPublicVars} given to the constructor).
     * @return a {@link Plan} with vars in the right operand replaced with values from
     *         {@code leftRow}.
     */
    public Plan<R> bind(@Nullable R leftRow) {
        return leftRow == null || product ? right : right.bind(leftTempBinding.row(leftRow));
    }
}
