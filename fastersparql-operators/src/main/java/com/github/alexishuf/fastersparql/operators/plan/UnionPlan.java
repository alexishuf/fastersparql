package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Union;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class UnionPlan<R> extends AbstractNAryPlan<R, UnionPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Union op;

    @Builder
    public UnionPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Union op,
                     @Singular List<? extends Plan<R>> inputs,
                     @Nullable UnionPlan<R> parent, @Nullable String name) {
        super(rowClass, inputs, name == null ? "Union-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    @Override public    Results<R> execute()     { return op.run(this); }

    @Override public UnionPlan<R> bind(Binding binding) {
        return new UnionPlan<>(rowClass, op, PlanHelpers.bindAll(operands, binding),
                              this, name);
    }

}
