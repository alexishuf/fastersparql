package com.github.alexishuf.fastersparql.operators.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.operators.plan.LeftJoin;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class BindLeftJoin<R, I> extends LeftJoin<R, I> {
    public BindLeftJoin(Plan<R, I> left, Plan<R, I> right,
                        @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(left.rowType, List.of(left, right), unbound, name);
    }

    public Plan<R, I>  left() { return operands.get(0); }
    public Plan<R, I> right() { return operands.get(1); }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        if (replacement.size() != 2)
            throw new IllegalArgumentException("Expected 2 operands, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name    = name    == null ? this.name    : name;
        return new BindLeftJoin<>(replacement.get(0), replacement.get(1), unbound, name);
    }

    @Override public BIt<R> execute(boolean canDedup) {
        return NativeBind.preferNative(this, canDedup);
    }
}
