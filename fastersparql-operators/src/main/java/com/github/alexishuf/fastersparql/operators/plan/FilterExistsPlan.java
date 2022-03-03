package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Value @Accessors(fluent = true)
public class FilterExistsPlan<R> implements Plan<R> {
    FilterExists op;
    Plan<R> input;
    boolean negate;
    Plan<R> filter;

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public Plan<R> bind(Map<String, String> var2value) {
        return new FilterExistsPlan<>(op, input.bind(var2value), negate, filter.bind(var2value));
    }
}
