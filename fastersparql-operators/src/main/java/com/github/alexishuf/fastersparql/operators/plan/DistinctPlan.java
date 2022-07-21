package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Distinct;
import lombok.Builder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Collections.singletonList;

public class DistinctPlan<R> extends AbstractUnaryPlan<R, DistinctPlan<R>> {
    private final Distinct op;

    @Builder
    public DistinctPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Distinct op,
                        @lombok.NonNull Plan<R> input, @Nullable DistinctPlan<R> parent,
                        @Nullable String name) {
        super(rowClass, singletonList(input), name == null ? "Distinct-"+input.name() : name, parent);
        this.op = op;
    }

    public              Distinct          op()     { return op; }
    @Override public    Results<R>   execute()     { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new DistinctPlan<>(rowClass, op, input().bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistinctPlan)) return false;
        if (!super.equals(o)) return false;
        DistinctPlan<?> that = (DistinctPlan<?>) o;
        return op.equals(that.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
