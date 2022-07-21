package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Join;
import lombok.Builder;
import lombok.Singular;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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

    public           Join          op()      { return op; }
    @Override public Results<R>    execute() { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new JoinPlan<>(op, rowClass, PlanHelpers.bindAll(operands, binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinPlan)) return false;
        if (!super.equals(o)) return false;
        JoinPlan<?> joinPlan = (JoinPlan<?>) o;
        return op.equals(joinPlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
