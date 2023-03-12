package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.util.BindingBIt;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class PlanBindingBIt<R> extends BindingBIt<R> {
    private final Plan right;
    private final boolean canDedup;

    public PlanBindingBIt(BIt<R> left, BindType bindType,
                          Vars leftPublicVars, Plan right,
                          boolean canDedup, @Nullable Vars projection,
                          Metrics.@Nullable JoinMetrics joinMetrics) {
        super(left, bindType, leftPublicVars, right.publicVars(), projection, joinMetrics);
        this.right = right;
        this.canDedup = canDedup;
    }

    @Override protected BIt<R> bind(R input) {
        return right.bound(tempBinding.row(input)).execute(rowType, canDedup);
    }

    @Override protected Object right() { return right; }
}
