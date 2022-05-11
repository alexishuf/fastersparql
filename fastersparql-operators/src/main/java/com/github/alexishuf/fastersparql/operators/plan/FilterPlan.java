package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.Filter;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Value @Accessors(fluent = true)
public class FilterPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    Class<? super R> rowClass;
    @Nullable Plan<R> parent;
    Filter op;
    Plan<R> input;
    List<String> filters;
    String name;

    @Builder
    public FilterPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Filter op,
                      @lombok.NonNull Plan<R> input,
                      @Singular List<String> filters,
                      @Nullable FilterPlan<R> parent, @Nullable String name) {
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.input = input;
        this.filters = filters == null ? Collections.emptyList() : filters;
        this.name = name == null ? "Filter-"+nextId.getAndIncrement()+"-"+input.name() : name;
    }

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public Plan<R> bind(Binding binding) {
        Plan<R> boundIn = input.bind(binding);
        boolean change = boundIn != input;
        List<String> boundFilters;
        if (filters.isEmpty()) {
            boundFilters = filters;
        } else {
            boundFilters = new ArrayList<>(filters.size());
            for (String filter : filters) {
                String bound = SparqlUtils.bind(filter, binding).toString();
                //noinspection StringEquality
                change |= bound != filter;
                boundFilters.add(bound);
            }
        }
        if (!change)
            return this;
        return new FilterPlan<>(rowClass, op, boundIn, boundFilters, this, name);
    }
}
