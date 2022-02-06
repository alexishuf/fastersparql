package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Join;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Value @Accessors(fluent = true)
public class JoinPlan<R> implements Plan<R> {
    Join op;
    List<Plan<R>> operands;

    @Override public List<String> publicVars() {
        return PlanHelpers.publicVarsUnion(operands);
    }

    @Override public List<String> allVars() {
        return PlanHelpers.allVarsUnion(operands);
    }

    @Override public Results<R> execute() {
        return op.run(operands);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new JoinPlan<>(op, PlanHelpers.bindAll(operands, var2ntValue));
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new JoinPlan<>(op, PlanHelpers.bindAll(operands, vars, ntValues));
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new JoinPlan<>(op, PlanHelpers.bindAll(operands, vars, ntValues));
    }
}
