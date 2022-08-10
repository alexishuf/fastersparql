package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.client.util.sparql.SparqlUtils;
import com.github.alexishuf.fastersparql.operators.Filter;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

public class FilterPlan<R> extends AbstractUnaryPlan<R, FilterPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Filter op;
    private final List<String> filters;


    public static final class Builder<T> {
        private Filter op;
        private Plan<T> input;
        private List<String> filters;
        private @Nullable FilterPlan<T> parent;
        private @Nullable String name;

        public Builder(Filter op) { this.op = op; }

        public Builder<T>      op(Filter value)                   {      op = value; return this; }
        public Builder<T>   input(Plan<T> value)                  {   input = value; return this; }
        public Builder<T> filters(List<String> value)             { filters = value; return this; }
        public Builder<T>  parent(@Nullable FilterPlan<T>  value) {  parent = value; return this; }
        public Builder<T>    name(@Nullable String  value)        {    name = value; return this; }

        public Builder<T> filter(String expression) {
            (filters == null ? filters = new ArrayList<>() : filters).add(expression);
            return this;
        }

        public FilterPlan<T> build() {
            return new FilterPlan<>(op, input, filters, parent, name);
        }
    }

    public static <T> Builder<T> builder(Filter op) {
        return new Builder<>(op);
    }

    public FilterPlan(Filter op, Plan<R> input, @Nullable List<String> filters,
                      @Nullable FilterPlan<R> parent, @Nullable String name) {
        super(input.rowClass(), singletonList(input),
              name == null ? "Filter-"+nextId.getAndIncrement()+"-"+input.name() : name, parent);
        this.op = op;
        this.filters = filters == null ? Collections.emptyList() : filters;
    }

    public           Filter       op()         { return op; }
    public           List<String> filters()    { return filters; }
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
        return new FilterPlan<>(op, boundIn, boundFilters, this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FilterPlan)) return false;
        if (!super.equals(o)) return false;
        FilterPlan<?> that = (FilterPlan<?>) o;
        return op.equals(that.op) && filters.equals(that.filters);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op, filters);
    }
}
