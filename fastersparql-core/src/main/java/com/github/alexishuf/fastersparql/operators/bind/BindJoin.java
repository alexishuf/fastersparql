package com.github.alexishuf.fastersparql.operators.bind;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import com.github.alexishuf.fastersparql.operators.plan.Join;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class BindJoin<R, I> extends Join<R, I> {
    private final Vars projection;

    public BindJoin(Plan<R, I> left, Plan<R, I> right, @Nullable Vars projection,
                    @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(List.of(left, right), unbound, name);
        this.projection = projection;
    }

    @Override protected Vars computeVars(boolean all) {
        return all || projection == null ? super.computeVars(all) : projection;
    }

    @Override public BIt<R> execute(boolean canDedup) {
        return NativeBind.preferNative(this, canDedup);
    }

    @Override public Plan<R, I> bind(Binding binding) {
        Plan<R, I> l = operands.get(0), r = operands.get(1);
        Plan<R, I> boundL = l.bind(binding), boundR = r.bind(binding);
        if (boundL == l && boundR == r)
            return this;
        var boundProjection = projection == null ? null : projection.minus(binding.vars());
        return new BindJoin<>(boundL, boundR, boundProjection, this, name);
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        if (replacement.size() != 2)
            throw new IllegalArgumentException("Expected 2 operands, got"+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name    = name    == null ? this.name    : name;
        return new BindJoin<>(replacement.get(0), replacement.get(1), projection, unbound, name);
    }
}
