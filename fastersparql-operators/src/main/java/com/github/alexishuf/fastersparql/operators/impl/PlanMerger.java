package com.github.alexishuf.fastersparql.operators.impl;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.row.RowBinding;
import com.github.alexishuf.fastersparql.client.model.row.RowOperations;
import com.github.alexishuf.fastersparql.client.util.Merger;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public final class PlanMerger<R> {
    private final Merger<R> merger;
    private final Plan<R> right;
    private final RowBinding<R> leftTempBinding;
    private final boolean product;

    public PlanMerger(PlanMerger<R> other) {
        this.merger = other.merger;
        this.right = other.right;
        this.leftTempBinding = new RowBinding<>(other.rowOps(), other.leftTempBinding.vars());
        this.product = other.product;
    }

    public PlanMerger(RowOperations rowOps, List<String> leftPublicVars, Plan<R> right,
                      BindType bindType) {
        List<String> rAll = right.allVars();
        List<String> rFree = Merger.rightFreeVars(leftPublicVars, right.publicVars());
        this.merger = Merger.forMerge(rowOps, leftPublicVars, rFree, bindType);
        this.right = right;
        this.leftTempBinding = new RowBinding<>(rowOps, leftPublicVars);
        this.product = Merger.isProduct(leftPublicVars, rAll);
    }

    public RowOperations rowOps()  { return merger.rowOps(); }
    public List<String> outVars()  { return merger.outVars(); }
    public boolean isTrivialLeft() { return merger.isTrivialLeft(); }
    public Plan<R>        right()  { return right; }
    public boolean    isProduct()  { return product; }

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

    public R merge(@Nullable R left, @Nullable R right) { return merger.merge(left, right); }
}
