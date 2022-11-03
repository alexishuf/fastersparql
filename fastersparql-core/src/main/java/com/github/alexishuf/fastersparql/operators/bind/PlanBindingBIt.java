package com.github.alexishuf.fastersparql.operators.bind;

import com.github.alexishuf.fastersparql.client.BindType;
import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.client.util.bind.BindingBIt;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class PlanBindingBIt<R, I> extends BindingBIt<R, I> {
    private final Plan<R, I> right;
    private final boolean canDedup;

    public PlanBindingBIt(BIt<R> left, BindType bindType, RowType<R, I> rowType,
                          Vars leftPublicVars, Plan<R, I> right,
                          boolean canDedup, @Nullable Vars projection) {
        super(left, bindType, rowType, leftPublicVars, right.publicVars(), projection);
        this.right = right;
        this.canDedup = canDedup;
    }

    @Override protected BIt<R> bind(R input) {
        return right.bind(tempBinding.row(input)).execute(canDedup);
    }
}
