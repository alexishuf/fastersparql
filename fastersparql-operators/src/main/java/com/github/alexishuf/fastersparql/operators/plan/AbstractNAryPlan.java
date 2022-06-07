package com.github.alexishuf.fastersparql.operators.plan;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public abstract class AbstractNAryPlan<R, Implementation extends Plan<R>>
        extends AbstractPlan<R, Implementation> {
    private @MonotonicNonNull List<String> publicVars, allVars;

    public AbstractNAryPlan(@lombok.NonNull Class<? super R> rowClass,
                            @lombok.NonNull List<? extends Plan<R>> operands,
                            @lombok.NonNull String name,
                            @Nullable Implementation parent) {
        super(rowClass, operands, name, parent);
    }

    @Override public List<String> publicVars() {
        if (publicVars == null)
            publicVars = PlanHelpers.publicVarsUnion(operands);
        return publicVars;
    }

    @Override public List<String> allVars() {
        if (allVars == null)
            allVars = PlanHelpers.allVarsUnion(operands);
        return allVars;
    }
}
