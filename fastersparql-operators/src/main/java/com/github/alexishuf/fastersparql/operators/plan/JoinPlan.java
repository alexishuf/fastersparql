package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Join;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Getter @Accessors(fluent = true) @EqualsAndHashCode(callSuper = true)
public class JoinPlan<R> extends AbstractNAryPlan<R, JoinPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Join op;

    @Builder
    public JoinPlan(@lombok.NonNull Join op, @lombok.NonNull Class<? super R> rowClass,
                    @Singular @lombok.NonNull List<? extends Plan<R>> operands,
                    @Nullable JoinPlan<R> parent, @Nullable String name) {
        super(rowClass, operands, name == null ? "Join-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public JoinPlan<R> withOperands(List<? extends Plan<R>> operands) {
        if (operands == this.operands)
            return this;
        return new JoinPlan<>(op, rowClass, operands, parent, name);
    }

    @Override public Results<R>    execute() { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new JoinPlan<>(op, rowClass, PlanHelpers.bindAll(operands, binding), this, name);
    }
}
