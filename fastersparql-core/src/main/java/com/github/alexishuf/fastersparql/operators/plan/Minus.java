package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Vars;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class Minus<R, I> extends Plan<R, I> {
    public Minus(Plan<R, I> input, Plan<R, I> filter, @Nullable Plan<R, I> unbound, @Nullable String name) {
        super(input.rowType, List.of(input, filter), unbound, name);
    }

    @Override protected Vars computeVars(boolean all) {
        return all ? super.computeVars(true) : operands.get(0).publicVars();
    }

    @Override protected void bgpSuffix(StringBuilder out, int indent) {
        newline(out, indent).append("MINUS");
        operands.get(1).groupGraphPattern(out, indent+1);
    }
}
