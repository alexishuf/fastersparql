package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;

public class NoneJoinReorderStrategy implements JoinReorderStrategy {
    public static final NoneJoinReorderStrategy INSTANCE = new NoneJoinReorderStrategy();

    @Override public @Nullable Vars reorder(Plan[] operands) {
        return null;
    }

    @Override public @Nullable Vars reorder(Plan left, Plan right) {
        return null;
    }

    @Override public String name() { return "None"; }
}
