package com.github.alexishuf.fastersparql.operators.bit;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.operators.BindingBIt;
import com.github.alexishuf.fastersparql.batch.type.Batch;
import com.github.alexishuf.fastersparql.model.BindType;
import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.metrics.Metrics;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import com.github.alexishuf.fastersparql.sparql.binding.BatchBinding;
import org.checkerframework.checker.nullness.qual.Nullable;

public final class PlanBindingBIt<B extends Batch<B>> extends BindingBIt<B> {
    private final Plan right;
    private final boolean canDedup;

    public PlanBindingBIt(BIt<B> left, BindType bindType,
                          Vars leftPublicVars, Plan right,
                          boolean canDedup, @Nullable Vars projection,
                          Metrics.@Nullable JoinMetrics joinMetrics) {
        super(left, bindType, leftPublicVars, right.publicVars(), projection, joinMetrics);
        this.right = right;
        this.canDedup = canDedup;
    }

    @Override protected BIt<B> bind(BatchBinding<B> binding) {
        return right.bound(binding).execute(batchType, canDedup);
    }

    @Override protected Object rightUnbound() { return right; }
}
