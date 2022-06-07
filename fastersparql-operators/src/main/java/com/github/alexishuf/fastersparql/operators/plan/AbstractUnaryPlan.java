package com.github.alexishuf.fastersparql.operators.plan;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class AbstractUnaryPlan<R, Implementation extends Plan<R>>
        extends AbstractPlan<R, Implementation> {
    public AbstractUnaryPlan(@lombok.NonNull Class<? super R> rowClass,
                             @lombok.NonNull List<? extends Plan<R>> operands,
                             @lombok.NonNull String name, @Nullable Implementation parent) {
        super(rowClass, operands, name, parent);
    }

    public           Plan<R>           input() { return operands().get(0); }
    @Override public List<String> publicVars() { return operands.get(0).publicVars(); }
    @Override public List<String>    allVars() { return operands.get(0).allVars(); }
}
