package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.List;

public class NoneJoinReorderStrategy implements JoinReorderStrategy {
    public static final NoneJoinReorderStrategy INSTANCE = new NoneJoinReorderStrategy();

    @Override public <P extends Plan<?, ?> > List<P> reorder(List<P> operands, boolean usesBind) {
        return operands;
    }

    @Override public String name() { return "None"; }
}
