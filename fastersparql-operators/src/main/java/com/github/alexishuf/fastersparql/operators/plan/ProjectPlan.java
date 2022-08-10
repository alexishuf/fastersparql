package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Project;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ProjectPlan<R> extends AbstractUnaryPlan<R, ProjectPlan<R>> {
    private final Project op;
    private final List<String> vars;

    public static final class Builder<T> {
        private Project op;
        private Plan<T> input;
        private List<String> vars;
        private @Nullable ProjectPlan<T> parent;
        private @Nullable String name;

        public Builder(Project op) { this.op = op; }

        public Builder<T> op(Project value)                      { op = value; return this; }
        public Builder<T> input(Plan<T> value)                   { input = value; return this; }
        public Builder<T> vars(List<String> value)               { vars = value; return this; }
        public Builder<T> parent(@Nullable ProjectPlan<T> value) { parent = value; return this; }
        public Builder<T> name(@Nullable String value)           { name = value; return this; }

        public ProjectPlan<T> build() {
            return new ProjectPlan<>(op, input, vars, parent, name);
        }
    }

    public static <T> Builder<T> builder(Project op) { return new Builder<>(op); }

    public ProjectPlan(Project op, Plan<R> input, List<String> vars,
                       @Nullable ProjectPlan<R> parent, @Nullable String name) {
        super(input.rowClass(), Collections.singletonList(input),
              name == null ? "Project-"+input.name() : name, parent);
        this.op = op;
        this.vars = vars;
    }

    public              Project      project()     { return op; }
    @Override public    List<String> publicVars()  { return vars; }
    @Override protected String       algebraName() { return "Project"+vars; }
    @Override public    Results<R>   execute()     { return op.run(this); }

    @Override public Plan<R> bind(Binding binding) {
        List<String> remaining = new ArrayList<>(vars.size());
        for (String var : vars) {
            if (binding.contains(var)) remaining.add(var);
        }
        if (remaining.size() == vars.size())
            remaining = vars; // let newer ArrayList<> be collected
        return new ProjectPlan<>(op, operands.get(0).bind(binding), remaining, this, name);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProjectPlan)) return false;
        if (!super.equals(o)) return false;
        ProjectPlan<?> that = (ProjectPlan<?>) o;
        return op.equals(that.op) && vars.equals(that.vars);
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), op, vars);
    }
}
