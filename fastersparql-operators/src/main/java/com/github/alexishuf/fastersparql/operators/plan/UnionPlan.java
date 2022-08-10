package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Union;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class UnionPlan<R> extends AbstractNAryPlan<R, UnionPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Union op;

    public static final class Builder<T> {
        private Union op;
        private List<? extends Plan<T>> inputs;
        private @Nullable UnionPlan<T> parent;
        private @Nullable String name;

        public Builder(Union op) { this.op = op; }

        public Builder<T>     op(Union value)                   { op = value; return this; }
        public Builder<T> inputs(List<? extends Plan<T>> value) { inputs = value; return this; }
        public Builder<T> parent(@Nullable UnionPlan<T> value)  { parent = value; return this; }
        public Builder<T>   name(@Nullable String value)        { name = value; return this; }

        public Builder<T> input(Plan<T> operand) {
            //noinspection unchecked
            ((List<Plan<T>>)inputs).add(operand);
            return this;
        }

        public UnionPlan<T> build() {
            return new UnionPlan<>(op, inputs, parent, name);
        }
    }

    public static <T> Builder<T> builder(Union op) { return new Builder<>(op); }

    public UnionPlan(Union op, List<? extends Plan<R>> inputs,
                     @Nullable UnionPlan<R> parent, @Nullable String name) {
        super(inputs.get(0).rowClass(), inputs, name == null ? "Union-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           Union      op()      { return op; }
    @Override public Results<R> execute() { return op.run(this); }

    @Override public UnionPlan<R> bind(Binding binding) {
        return new UnionPlan<>(op, PlanHelpers.bindAll(operands, binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UnionPlan)) return false;
        if (!super.equals(o)) return false;
        UnionPlan<?> unionPlan = (UnionPlan<?>) o;
        return op.equals(unionPlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
