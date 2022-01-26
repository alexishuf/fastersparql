package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Slice;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
public class SlicePlan<R> implements Plan<R> {
    Slice op;
    Plan<R> input;
    long offset, limit;

    @Override public Results<R> execute() {
        return op.run(input, offset, limit);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new SlicePlan<>(op, input.bind(var2ntValue), offset, limit);
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new SlicePlan<>(op, input.bind(vars, ntValues), offset, limit);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new SlicePlan<>(op, input.bind(vars, ntValues), offset, limit);
    }

}
