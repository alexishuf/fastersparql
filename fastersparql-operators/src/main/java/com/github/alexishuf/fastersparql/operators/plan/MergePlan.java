package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Merge;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class MergePlan<R> extends AbstractNAryPlan<R, MergePlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Merge op;

    public static final class Builder<T> {
        private Merge op;
        private List<? extends Plan<T>> inputs;
        private @Nullable MergePlan<T> parent;
        private @Nullable String name;

        public Builder(Merge op) { this.op = op; }

        public Builder<T> op(Merge value) { op = value; return this; }
        public Builder<T> inputs(List<? extends Plan<T>> value) { inputs = value; return this; }
        public Builder<T> parent(@Nullable MergePlan<T> value) { parent = value; return this; }
        public Builder<T> name(@Nullable String value) { name = value; return this; }

        public Builder<T> input(Plan<T> operand) {
            //noinspection unchecked
            ((List<Plan<T>>)inputs).add(operand);
            return this;
        }

        public MergePlan<T> build() {
            return new MergePlan<>(op, inputs, parent, name);
        }
    }

    public static <T> Builder<T> builder(Merge op) { return new Builder<>(op); }

    public MergePlan(Merge op, List<? extends Plan<R>> inputs,
                     @Nullable MergePlan<R> parent, @Nullable String name) {
        super(inputs.get(0).rowClass(), inputs,
              name == null ? "CollapsableUnion-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           Merge        op()          { return op; }
    @Override public Results<R>   execute()     { return op.run(this); }

    @Override public MergePlan<R> bind(Binding binding) {
        return new MergePlan<>(op, PlanHelpers.bindAll(operands, binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MergePlan)) return false;
        if (!super.equals(o)) return false;
        MergePlan<?> mergePlan = (MergePlan<?>) o;
        return op.equals(mergePlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
