package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Value @Accessors(fluent = true)
public class FilterExistsPlan<R> implements Plan<R> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    Class<? super R> rowClass;
    @Nullable Plan<R> parent;
    FilterExists op;
    Plan<R> input;
    boolean negate;
    Plan<R> filter;
    String name;

    @Builder
    public FilterExistsPlan(@lombok.NonNull Class<? super R> rowClass,
                            @lombok.NonNull FilterExists op, @lombok.NonNull  Plan<R> input,
                            boolean negate, @lombok.NonNull  Plan<R> filter,
                            @Nullable FilterExistsPlan<R> parent, @Nullable String name) {
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.input = input;
        this.negate = negate;
        this.filter = filter;
        this.name = name == null ? "FilterExists-"+nextId.getAndIncrement() : name;
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
        return new FilterExistsPlan<>(rowClass, op, input.bind(binding), negate,
                                      filter.bind(binding), this, name);
    }
}
