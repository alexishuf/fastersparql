package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Distinct;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

@Value @Accessors(fluent = true)
public class DistinctPlan<R> implements Plan<R> {
    Class<? super R> rowClass;
    DistinctPlan<R> parent;
    Distinct op;
    Plan<R> input;
    String name;

    @Builder
    public DistinctPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Distinct op,
                        @lombok.NonNull Plan<R> input, @Nullable DistinctPlan<R> parent,
                        @Nullable String name) {
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.input = input;
        this.name = name == null ? "Distinct-"+input.name() : name;
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
        return new DistinctPlan<>(rowClass, op, input.bind(binding), this, name);
    }
}
