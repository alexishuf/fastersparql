package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class ExistsPlan<R> extends AbstractUnaryPlan<R, ExistsPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final FilterExists op;
    private final boolean negate;

    @Builder
    public ExistsPlan(@lombok.NonNull Class<? super R> rowClass,
                      @lombok.NonNull FilterExists op, @lombok.NonNull  Plan<R> input,
                      boolean negate, @lombok.NonNull  Plan<R> filter,
                      @Nullable ExistsPlan<R> parent, @Nullable String name) {
        super(rowClass, Arrays.asList(input, filter),
              name == null ? "FilterExists-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
        this.negate = negate;
    }

    public Plan<R>                filter()      { return operands.get(1); }
    @Override public Results<R>   execute()     { return op.run(this); }
    @Override protected String    algebraName() {return (negate?"Not":"")+"Exists";}

    @Override public Plan<R> bind(Binding binding) {
        return new ExistsPlan<>(rowClass, op, input().bind(binding), negate,
                                      filter().bind(binding), this, name);
    }
}
