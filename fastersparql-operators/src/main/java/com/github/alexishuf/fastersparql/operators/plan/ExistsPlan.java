package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.FilterExists;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ExistsPlan<R> extends AbstractNAryPlan<R, ExistsPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final FilterExists op;
    private final boolean negate;

    public static final class Builder<T> {
        private FilterExists op;
        private Plan<T> input;
        private boolean negate;
        private Plan<T> filter;
        private @Nullable ExistsPlan<T> parent;
        private @Nullable String name;

        public Builder(FilterExists op) { this.op = op; }

        public Builder<T> op(FilterExists  value)                { op = value; return this; }
        public Builder<T> input(Plan<T>  value)                  { input = value; return this; }
        public Builder<T> negate(boolean  value)                 { negate = value; return this; }
        public Builder<T> filter(Plan<T>  value)                 { filter = value; return this; }
        public Builder<T> parent(@Nullable ExistsPlan<T>  value) { parent = value; return this; }
        public Builder<T> name(@Nullable String  value)          { name = value; return this; }

        public ExistsPlan<T> build() {
            return new ExistsPlan<>(op, input, negate, filter, parent, name);
        }
    }

    public static <T> Builder<T> builder(FilterExists op) {
        return new Builder<>(op);
    }

    public ExistsPlan(FilterExists op,  Plan<R> input, boolean negate,  Plan<R> filter,
                      @Nullable ExistsPlan<R> parent, @Nullable String name) {
        super(input.rowClass(), Arrays.asList(input, filter),
              name == null ? "FilterExists-"+nextId.getAndIncrement() : name, parent);
        this.op = op;
        this.negate = negate;
    }

    public              FilterExists op()          { return op; }
    public              boolean      negate()      { return negate; }
    public              Plan<R>      input()       { return operands.get(0); }
    public              Plan<R>      filter()      { return operands.get(1); }
    @Override public    Results<R>   execute()     { return op.run(this); }
    @Override protected String       algebraName() { return (negate?"Not":"")+"Exists"; }
    @Override public    List<String> publicVars()  { return operands.get(0).publicVars(); }

    @Override public Plan<R> bind(Binding binding) {
        return new ExistsPlan<>(op, operands.get(0).bind(binding), negate,
                                filter().bind(binding), this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExistsPlan)) return false;
        if (!super.equals(o)) return false;
        ExistsPlan<?> that = (ExistsPlan<?>) o;
        return negate == that.negate && op.equals(that.op);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op, negate);
    }
}
