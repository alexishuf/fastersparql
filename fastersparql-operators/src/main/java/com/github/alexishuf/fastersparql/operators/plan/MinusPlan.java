package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Minus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class MinusPlan<R> extends AbstractNAryPlan<R, MinusPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final Minus op;

    public static final class Builder<T> {
        private Minus op;
        private Plan<T> left;
        private Plan<T> right;
        private @Nullable MinusPlan<T> parent;
        private @Nullable String name;

        public Builder(Minus op) { this.op = op; }

        public Builder<T> op(Minus value) { op = value; return this; }
        public Builder<T> left(Plan<T> value) { left = value; return this; }
        public Builder<T> right(Plan<T> value) { right = value; return this; }
        public Builder<T> parent(@Nullable MinusPlan<T> value) { parent = value; return this; }
        public Builder<T> name(@Nullable String value) { name = value; return this; }

        public MinusPlan<T> build() { return new MinusPlan<>(op, left, right, parent, name); }
    }

    public static <T> Builder<T> builder(Minus op) { return new Builder<>(op); }

    public MinusPlan(Minus op, Plan<R> left, Plan<R> right,
                     @Nullable MinusPlan<R> parent, @Nullable String name) {
        super(left.rowClass(), Arrays.asList(left, right),
              name == null ? "Minus-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
    }

    public           Minus      op()           { return op; }
    public           Plan<R>    input()        { return operands.get(0); }
    public           Plan<R>    filter()       { return operands.get(1); }
    @Override public Results<R> execute()      { return op.run(this); }
    @Override public List<String> publicVars() { return operands.get(0).publicVars(); }

    @Override public Plan<R> bind(Binding binding) {
        return new MinusPlan<>(op, operands.get(0).bind(binding),
                               operands.get(1).bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MinusPlan)) return false;
        if (!super.equals(o)) return false;
        MinusPlan<?> minusPlan = (MinusPlan<?>) o;
        return op.equals(minusPlan.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
