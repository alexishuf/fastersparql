package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.operators.plan.Plan;

import java.util.*;

import static com.github.alexishuf.fastersparql.operators.JoinHelpers.isProduct;

public class AvoidCartesianJoinReorderStrategy implements JoinReorderStrategy {
    public static final AvoidCartesianJoinReorderStrategy INSTANCE
            = new AvoidCartesianJoinReorderStrategy();

    @Override public <P extends Plan<?>> List<P> reorder(List<P> originalOperands, boolean useBind) {
        int size = originalOperands.size();
        if (size <= 1)
            return originalOperands;
        List<P> operands = new ArrayList<>(originalOperands);
        LinkedHashSet<String> accVars = new LinkedHashSet<>(size*8);
        accVars.addAll(operands.get(0).publicVars());
        boolean change = useBind && fixFirstProductWithBind(operands, accVars);
        int start = change ? 2 : 1;
        for (int i = start, last = size-1; i < last; i++) {
            P right = operands.get(i);
            if (isProduct(accVars, right, useBind)) {
                change = true;
                right = moveFirstCompatible(operands, accVars, i, useBind);
            }
            accVars.addAll(right.publicVars());
        }
        return change ? operands : originalOperands;
    }

    private <P extends Plan<?>>
    boolean fixFirstProductWithBind(List<P> operands, Set<String> accVars) {
        if (!isProduct(accVars, operands.get(1), true))
            return false;
        int size = operands.size();
        P first = operands.get(0);
        for (int i = 1; i < size; i++) {
            P candidate = operands.get(i);
            if (!isProduct(accVars, candidate, true)) {
                operands.remove(i);
                operands.add(1, candidate);
                accVars.addAll(candidate.publicVars());
                return true;
            } else if (!isProduct(candidate.publicVars(), first, true)) {
                operands.remove(i);
                operands.set(0, candidate);
                operands.add(1, first);
                accVars.addAll(candidate.publicVars());
                return true;
            }
        }
        return false;
    }

    private <P extends Plan<?>> P moveFirstCompatible(List<P> operands, Collection<String > accVars,
                                                      int i, boolean useBind) {
        for (int candidateIdx = i+1, size = operands.size(); candidateIdx < size; candidateIdx++) {
            P candidate = operands.get(candidateIdx);
            if (!isProduct(accVars, candidate, useBind)) {
                operands.remove(candidateIdx);
                operands.add(i, candidate);
                return candidate;
            }
        }
        return operands.get(i+1);
    }

    @Override public String name() {
        return "AvoidCartesian";
    }
}
