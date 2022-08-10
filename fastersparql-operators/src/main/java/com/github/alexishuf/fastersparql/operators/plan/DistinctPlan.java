package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Distinct;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

import static java.util.Collections.singletonList;

public class DistinctPlan<R> extends AbstractUnaryPlan<R, DistinctPlan<R>> {
    private final Distinct op;

    public static final class Builder<T> {
        private Distinct op;
        private Plan<T> input;
        @Nullable DistinctPlan<T> parent;
        @Nullable String name;

        public Builder(Distinct op) { this.op = op; }

        public Builder<T>     op(Distinct value)                  {     op = value; return this; }
        public Builder<T>  input(Plan<T> value)                   {  input = value; return this; }
        public Builder<T> parent(@Nullable DistinctPlan<T> value) { parent = value; return this; }
        public Builder<T>   name(@Nullable String value)          {   name = value; return this; }

        public DistinctPlan<T> build() { return new DistinctPlan<>(op, input, parent, name); }
    }

    public static <T> Builder<T> builder(Distinct op) {
        return new Builder<>(op);
    }

    public DistinctPlan(Distinct op, Plan<R> input, @Nullable DistinctPlan<R> parent,
                        @Nullable String name) {
        super(input.rowClass(), singletonList(input), name == null ? "Distinct-"+input.name() : name, parent);
        this.op = op;
    }

    public              Distinct          op()     { return op; }
    @Override public    Results<R>   execute()     { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        return new DistinctPlan<>(op, input().bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DistinctPlan)) return false;
        if (!super.equals(o)) return false;
        DistinctPlan<?> that = (DistinctPlan<?>) o;
        return op.equals(that.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op);
    }
}
