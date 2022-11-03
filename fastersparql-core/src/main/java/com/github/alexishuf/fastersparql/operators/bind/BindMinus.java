package com.github.alexishuf.fastersparql.operators.bind;

import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.operators.plan.Minus;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class BindMinus<R, I> extends Minus<R, I> {
    public BindMinus(Plan<R, I> input, Plan<R, I> filter,
                     @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(input, filter, unbound, name);
    }

    @Override public BIt<R> execute(boolean canDedup) {
        return NativeBind.preferNative(this, canDedup);
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound, @Nullable String name) {
        if (replacement.size() != 2)
            throw new IllegalArgumentException("Expected 2 operands, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name    = name    == null ? this.name    : name;
        return new BindMinus<>(replacement.get(0), replacement.get(1), unbound, name);
    }
}
