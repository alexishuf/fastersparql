package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class Exists<R, I> extends Plan<R, I> {
    protected final boolean negate;

    public Exists(Plan<R, I> input, boolean negate, Plan<R, I> filter,
                  @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(input.rowType, List.of(input, filter), unbound, name);
        this.negate = negate;
    }

    public final Plan<R, I> input()  { return operands.get(0); }
    public final Plan<R, I> filter() { return operands.get(1); }
    public final boolean negate()  { return negate; }

    @Override protected Vars computeVars(boolean all) {
        return all ? super.computeVars(true) : operands.get(0).publicVars();
    }

    @Override public String algebraName() { return negate ? "NotExists" : "Exists"; }

    @Override public boolean equals(Object o) {
        return o instanceof Exists<?, ?> that && negate == that.negate && super.equals(o);
    }

    @Override public int hashCode() { return 31*super.hashCode() + Boolean.hashCode(negate); }
}
