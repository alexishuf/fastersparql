package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;

import java.util.List;
import java.util.Map;

/**
 * Represents a tree of operators applied to their arguments
 * @param <R>
 */
public interface Plan<R> {
    Results<R> execute();

    Plan<R> bind(Map<String, String> var2ntValue);

    default Plan<R> bind(List<String> vars, List<String> ntValues) {
        return PlanHelpers.bindWithMap(this, vars, ntValues);
    }

    default Plan<R> bind(List<String> vars, String[] ntValues) {
        return PlanHelpers.bindWithMap(this, vars, ntValues);
    }
}
