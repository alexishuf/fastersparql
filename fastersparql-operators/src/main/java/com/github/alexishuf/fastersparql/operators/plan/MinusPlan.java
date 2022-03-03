package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.VarUtils;
import com.github.alexishuf.fastersparql.operators.Minus;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Value @Accessors(fluent = true)
public class MinusPlan<R> implements Plan<R> {
    Minus op;
    Plan<R> left, right;

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return VarUtils.union(left.publicVars(), right.publicVars());
    }

    @Override public List<String> allVars() {
        return VarUtils.union(left.allVars(), right.allVars());
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new MinusPlan<>(op, left.bind(var2ntValue), right.bind(var2ntValue));
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new MinusPlan<>(op, left.bind(vars, ntValues), right.bind(vars, ntValues));
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new MinusPlan<>(op, left.bind(vars, ntValues), right.bind(vars, ntValues));
    }

}
