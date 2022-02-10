package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.List;

public class NullJoinReorderStrategy implements JoinReorderStrategy {
    public static final NullJoinReorderStrategy INSTANCE = new NullJoinReorderStrategy();

    @Override public <P extends Plan<?> > List<P> reorder(List<P> operands, boolean usesBind) {
        return operands;
    }

    @Override public String name() {
        return "Null";
    }
}
