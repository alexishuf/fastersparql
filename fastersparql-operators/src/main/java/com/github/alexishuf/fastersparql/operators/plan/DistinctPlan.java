package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Distinct;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Value @Accessors(fluent = true)
public class DistinctPlan<R> implements Plan<R> {
    Distinct op;
    Plan<R> input;

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Results<R> execute() {
        return op.run(input);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new DistinctPlan<>(op, input.bind(var2ntValue));
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new DistinctPlan<>(op, input.bind(vars, ntValues));
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new DistinctPlan<>(op, input.bind(vars, ntValues));
    }
}
