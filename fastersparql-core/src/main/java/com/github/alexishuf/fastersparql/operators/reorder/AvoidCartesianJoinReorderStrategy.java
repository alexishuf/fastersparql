package com.github.alexishuf.fastersparql.operators.reorder;

import com.github.alexishuf.fastersparql.model.Vars;
import com.github.alexishuf.fastersparql.operators.plan.Plan;
import org.checkerframework.checker.nullness.qual.Nullable;


public class AvoidCartesianJoinReorderStrategy implements JoinReorderStrategy {
    public static final AvoidCartesianJoinReorderStrategy INSTANCE
            = new AvoidCartesianJoinReorderStrategy();

    @Override public @Nullable Vars reorder(Plan left, Plan right) {
        boolean swap = !left .publicVars().intersects(right.allVars())
                    &&  right.publicVars().intersects( left.allVars());
        return swap ? left.publicVars().union(right.publicVars()) : null;
    }

    @Override public @Nullable Vars reorder(Plan[] operands) {
        if (operands.length < 2) return null;
        Vars acc = new Vars.Mutable(10);
        acc.addAll(operands[0].publicVars());
        Vars projection = fixFirstProductWithBind(operands, acc);
        boolean change = projection != null;
        for (int last = operands.length-1, i = 1 + (projection != null ? 1 : 0); i < last; i++) {
            Plan right = operands[i];
            if (!acc.intersects(right.allVars())) {
                if (projection == null)
                    projection = Plan.publicVars(operands);
                if (moveFirstCompatible(operands, acc, i))
                    change = true;
            }
            acc.addAll(right.publicVars());
        }
        return change ? projection : null;
    }

    private @Nullable Vars fixFirstProductWithBind(Plan[] operands, Vars acc) {
        Plan second = operands[1];
        if (acc.intersects(second.allVars()))
            return null;
        Vars firstAllVars = operands[0].allVars(), projection;
        for (int i = 1; i < operands.length; ++i) {
            Plan candidate = operands[i];
            if (acc.intersects(candidate.allVars())) {
                projection = Plan.publicVars(operands);
                operands[1] = candidate;
            } else if (candidate.publicVars().intersects(firstAllVars)) {
                projection = Plan.publicVars(operands);
                operands[1] = operands[0]; operands[0] = candidate;
            } else {
                continue;
            }
            for (int j = i; j > 2; j--) operands[j] = operands[j-1];
            if (i > 1) operands[2] = second;
            acc.addAll(candidate.publicVars());
            return projection;
        }
        return null;
    }

    private boolean moveFirstCompatible(Plan[] operands, Vars acc,
                                       int incompatible) {
        for (int i = incompatible+1; i < operands.length; i++) {
            Plan candidate = operands[i];
            if (acc.intersects(candidate.allVars())) {
                for (int j = i; j > incompatible; j--) operands[j] = operands[j-1];
                operands[incompatible] = candidate;
                return true;
            }
        }
        return false;
    }

    @Override public String name() {
        return "AvoidCartesian";
    }
}
