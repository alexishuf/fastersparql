package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Minus;
import lombok.Builder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class MinusPlan<R> extends AbstractNAryPlan<R, MinusPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Minus op;

    @Builder
    public MinusPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Minus op,
                     @lombok.NonNull Plan<R> left, @lombok.NonNull Plan<R> right,
                     @Nullable MinusPlan<R> parent, @Nullable String name) {
        super(rowClass, Arrays.asList(left, right),
              name == null ? "Minus-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           Minus      op()         { return op; }
    public           Plan<R>    input()      { return operands.get(0); }
    public           Plan<R>    filter()     { return operands.get(1); }
    @Override public Results<R> execute()    { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new MinusPlan<>(rowClass, op, operands.get(0).bind(binding),
                               operands.get(1).bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MinusPlan)) return false;
        if (!super.equals(o)) return false;
        MinusPlan<?> minusPlan = (MinusPlan<?>) o;
        return op.equals(minusPlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
