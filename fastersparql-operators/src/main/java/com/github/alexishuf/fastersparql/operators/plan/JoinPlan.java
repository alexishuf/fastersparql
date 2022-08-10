package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Join;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinPlan<R> extends AbstractNAryPlan<R, JoinPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Join op;

    @SuppressWarnings("unused")
    public static final class Builder<T> {
        private Join op;
        private List<? extends Plan<T>> operands;
        private @Nullable JoinPlan<T> parent;
        private @Nullable String name;

        public Builder(Join op) { this.op = op; }

        public Builder<T> op(Join value)                          { op = value; return this; }
        public Builder<T> operands(List<? extends Plan<T>> value) { operands = value; return this; }
        public Builder<T> parent(@Nullable JoinPlan<T> value)     { parent = value; return this; }
        public Builder<T> name(@Nullable String value)            { name = value; return this; }

        public Builder<T> operand(Plan<T> operand) {
            //noinspection unchecked
            ((List<Plan<T>>)operands).add(operand);
            return this;
        }

        public JoinPlan<T> build() { return new JoinPlan<>(op, operands, parent, name); }
    }

    public static <T> Builder<T> builder(Join op) { return new Builder<>(op); }

    public JoinPlan(Join op, List<? extends Plan<R>> operands,
                    @Nullable JoinPlan<R> parent, @Nullable String name) {
        super(operands.get(0).rowClass(), operands,
              name == null ? "Join-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public JoinPlan<R> withOperands(List<? extends Plan<R>> operands) {
        if (operands == this.operands)
            return this;
        return new JoinPlan<>(op, operands, parent, name);
    }

    public           Join          op()      { return op; }
    @Override public Results<R>    execute() { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new JoinPlan<>(op, PlanHelpers.bindAll(operands, binding), this, name);
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
