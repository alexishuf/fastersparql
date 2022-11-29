package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Vars;
import com.github.alexishuf.fastersparql.batch.BIt;
import com.github.alexishuf.fastersparql.batch.EmptyBIt;
import com.github.alexishuf.fastersparql.client.model.row.RowType;
import com.github.alexishuf.fastersparql.sparql.binding.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class Empty<R, I> extends Plan<R, I> {
    private final Vars allVars, publicVars;

    public Empty(RowType<R, I> rowType, @Nullable Vars publicVars,
                 @Nullable Vars allVars, @Nullable Plan<R, I> unbound,
                 @Nullable String name) {
        super(rowType, List.of(), unbound, name);
        this.allVars    = allVars    == null ? Vars.EMPTY : allVars;
        this.publicVars = publicVars == null ? Vars.EMPTY : publicVars;
    }

    @Override
    public Plan<R, I> with(List<? extends Plan<R, I>> replacement, @Nullable Plan<R, I> unbound,
                        @Nullable String name) {
        if (replacement.size() > 0)
            throw new IllegalArgumentException("Expected no operands, got "+replacement.size());
        unbound = unbound == null ? this.unbound : unbound;
        name = name == null ? this.name : name;
        return new Empty<>(rowType, publicVars, allVars, unbound, name);
    }

    @Override public BIt<R> execute(boolean ignored) {
        return new EmptyBIt<>(rowOperations().rowClass(), publicVars);
    }

    @Override public Plan<R, I> bind(Binding binding) {
        var pub = publicVars.minus(binding.vars());
        var all = allVars.minus(binding.vars());
        if (pub.size() == publicVars.size() && all.size() == allVars.size())
            return this;
        return new Empty<>(rowType, pub, all, this, name);
    }

    @Override protected Vars computeVars(boolean all) {
        return all ? allVars : publicVars;
    }

    @Override public void groupGraphPatternInner(StringBuilder out, int indent) { }
}
