package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.LeftJoin;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LeftJoinPlan<R> extends AbstractNAryPlan<R, LeftJoinPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final LeftJoin op;

    public static final class Builder<T> {
        private LeftJoin op;
        private Plan<T> left, right;
        private @Nullable LeftJoinPlan<T> parent;
        private @Nullable String name;

        public Builder(LeftJoin op) { this.op = op; }

        public Builder<T> op(LeftJoin value)                      { op = value; return this; }
        public Builder<T> left(Plan<T> value)                     { left = value; return this; }
        public Builder<T> right(Plan<T> value)                    { right = value; return this; }
        public Builder<T> parent(@Nullable LeftJoinPlan<T> value) { parent = value; return this; }
        public Builder<T> name(@Nullable String value)            { name = value; return this; }

        public LeftJoinPlan<T> build() {
            return new LeftJoinPlan<>(op, left, right, parent, name);
        }
    }

    public static <T> Builder<T> builder(LeftJoin op) { return new Builder<>(op); }

    public LeftJoinPlan(LeftJoin op, Plan<R> left, Plan<R> right,
                        @Nullable LeftJoinPlan<R> parent, @Nullable String name) {
        super(left.rowClass(), Arrays.asList(left, right),
              name == null ? "LeftJoin-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           LeftJoin           op() { return op; }
    public           Plan<R>          left() { return operands.get(0); }
    public           Plan<R>         right() { return operands.get(1); }
    @Override public Results<R>    execute() { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new LeftJoinPlan<>(op, operands.get(0).bind(binding),
                                  operands.get(1).bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LeftJoinPlan)) return false;
        if (!super.equals(o)) return false;
        LeftJoinPlan<?> that = (LeftJoinPlan<?>) o;
        return op.equals(that.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
