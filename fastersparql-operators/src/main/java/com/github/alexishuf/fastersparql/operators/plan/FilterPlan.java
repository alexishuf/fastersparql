package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Filter;
import lombok.Value;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@Value
public class FilterPlan<R> implements Plan<R> {
    Filter op;
    Plan<R> input;
    Collection<String> filters;
    Collection<Predicate<R>> predicates;

    public FilterPlan(Filter op, Plan<R> input, @Nullable Collection<String> filters,
                      @Nullable Collection<Predicate<R>> predicates) {
        this.op = op;
        this.input = input;
        this.filters = filters == null ? Collections.emptyList() : filters;
        this.predicates = predicates == null ? Collections.emptyList() : predicates;
    }

    @Override public Results<R> execute() {
        return op.run(input, filters, predicates);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        if (filters.isEmpty() && predicates.isEmpty())
            return new FilterPlan<>(op, input.bind(var2ntValue), filters, predicates);
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        if (filters.isEmpty() && predicates.isEmpty())
            return new FilterPlan<>(op, input.bind(vars, ntValues), filters, predicates);
        return PlanHelpers.bindWithMap(this, vars, ntValues);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        if (filters.isEmpty() && predicates.isEmpty())
            return new FilterPlan<>(op, input.bind(vars, ntValues), filters, predicates);
        return PlanHelpers.bindWithMap(this, vars, ntValues);
    }
}
