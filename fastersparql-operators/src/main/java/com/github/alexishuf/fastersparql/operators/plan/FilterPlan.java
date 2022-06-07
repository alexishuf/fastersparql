package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.Filter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class FilterPlan<R> extends AbstractUnaryPlan<R, FilterPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Filter op;
    private final List<String> filters;

    @Builder
    public FilterPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Filter op,
                      @lombok.NonNull Plan<R> input,
                      @Singular List<String> filters,
                      @Nullable FilterPlan<R> parent, @Nullable String name) {
        super(rowClass, singletonList(input),
              name == null ? "Filter-"+nextId.getAndIncrement()+"-"+input.name() : name, parent);
        this.op = op;
        this.filters = filters == null ? Collections.emptyList() : filters;
    }

    @Override public Results<R>   execute()    { return op.run(this); }

    @Override protected String algebraName() {
        StringBuilder sb = new StringBuilder().append("Filter");
        for (String expr : filters) sb.append(expr).append(", ");
        if (!filters.isEmpty()) sb.setLength(sb.length()-2);
        return sb.toString();
    }

    @Override public Plan<R> bind(Binding binding) {
        Plan<R> input = operands.get(0);
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
