package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.Filter;
import lombok.Value;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

@Value
public class FilterPlan<R> implements Plan<R> {
    Filter op;
    Plan<R> input;
    Collection<? extends CharSequence> filters;

    public FilterPlan(Filter op, Plan<R> input,
                      @Nullable Collection<? extends CharSequence> filters) {
        this.op = op;
        this.input = input;
        if (filters == null || filters.isEmpty()) {
            this.filters = Collections.emptyList();
        } else {
            ArrayList<String> filterStrings = new ArrayList<>(filters.size());
            for (CharSequence f : filters) filterStrings.add(f.toString());
            this.filters = filterStrings;
        }
    }

    /** This trusts {@code filters} and its {@link CharSequence}s will nto change. */
    private FilterPlan(Collection<? extends CharSequence> filters, Plan<R> input, Filter op) {
        this.op = op;
        this.input = input;
        this.filters = filters;
    }

    @Override public List<String> vars() {
        return input.vars();
    }

    @Override public Results<R> execute() {
        return op.run(input, filters);
    }

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        Plan<R> boundIn = input.bind(var2ntValue);
        boolean change = boundIn != input;
        if (filters.isEmpty())
            return change ? new FilterPlan<>(op, boundIn, filters) : this;
        List<CharSequence> boundFilters = new ArrayList<>(filters.size());
        for (CharSequence filter : filters) {
            String bound = SparqlUtils.bind(filter, var2ntValue).toString();
            change |= bound != filter;
            boundFilters.add(bound);
        }
        return change ? new FilterPlan<>(boundFilters, boundIn, op) : this;
    }
}
