package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Union;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class UnionPlan<R> implements Plan<R> {
    Union op;
    List<Plan<R>> inputs;

    @Override public Results<R> execute() {
        return op.run(inputs);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new UnionPlan<>(op, PlanHelpers.bindAll(inputs, var2ntValue));
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new UnionPlan<>(op, PlanHelpers.bindAll(inputs, vars, ntValues));
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new UnionPlan<>(op, PlanHelpers.bindAll(inputs, vars, ntValues));
    }
}
