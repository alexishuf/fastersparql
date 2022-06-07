package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Distinct;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Collections.singletonList;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class DistinctPlan<R> extends AbstractUnaryPlan<R, DistinctPlan<R>> {
    private final Distinct op;

    @Builder
    public DistinctPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Distinct op,
                        @lombok.NonNull Plan<R> input, @Nullable DistinctPlan<R> parent,
                        @Nullable String name) {
        super(rowClass, singletonList(input), name == null ? "Distinct-"+input.name() : name, parent);
        this.op = op;
    }

    @Override public    Results<R>   execute()     { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new DistinctPlan<>(rowClass, op, input().bind(binding), this, name);
    }
}
