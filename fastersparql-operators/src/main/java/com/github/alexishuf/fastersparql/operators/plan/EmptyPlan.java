package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.alexishuf.fastersparql.client.model.Results.empty;
import static java.util.Collections.emptyList;

public class EmptyPlan<R> extends AbstractPlan<R, EmptyPlan<R>> {
    private static final AtomicInteger nextId = new AtomicInteger(1);
    private final List<String> publicVars, allVars;

    public static final class Builder<T> {
        private final Class<? super T> rowClass;
        private @Nullable String name;
        private @Nullable List<String> publicVars;
        private @Nullable List<String> allVars;

        public Builder(Class<? super T> rowClass) { this.rowClass = rowClass; }

        public Builder<T>       name(@Nullable String value)       { name = value; return this; }
        public Builder<T> publicVars(@Nullable List<String> value) { publicVars = value; return this; }
        public Builder<T>    allVars(@Nullable List<String> value) { allVars = value; return this; }

        public EmptyPlan<T> build() { return new EmptyPlan<>(rowClass, name, publicVars, allVars); }
    }

    public static <T> Builder<T> builder(Class<? super T> rowClass) {
        return new Builder<>(rowClass);
    }

    public EmptyPlan(Class<? super R> rowClass,
                     @Nullable String name, @Nullable List<String> publicVars,
                     @Nullable List<String> allVars) {
        super(rowClass, emptyList(), name == null ? "Empty-"+nextId.getAndIncrement() : name, null);
        this.publicVars = publicVars == null ? emptyList() : publicVars;
        this.allVars = allVars == null ? emptyList() : allVars;
    }

    @Override public List<String>      publicVars()          { return publicVars; }
    @Override public List<String>      allVars()             { return allVars; }
    @Override public @Nullable Plan<R> parent()              { return null; }
    @Override public Results<R>        execute()             { return empty(publicVars, rowClass); }
    @Override public Plan<R>           bind(Binding binding) { return this; }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EmptyPlan)) return false;
        if (!super.equals(o)) return false;
        EmptyPlan<?> emptyPlan = (EmptyPlan<?>) o;
        return publicVars.equals(emptyPlan.publicVars) && allVars.equals(emptyPlan.allVars);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), publicVars, allVars);
    }
}
