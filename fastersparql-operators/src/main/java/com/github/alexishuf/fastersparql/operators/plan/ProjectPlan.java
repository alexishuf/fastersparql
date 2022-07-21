package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Project;
import lombok.Builder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ProjectPlan<R> extends AbstractUnaryPlan<R, ProjectPlan<R>> {
    private final Project op;
    private final List<String> vars;

    @Builder
    public ProjectPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Project op,
                       @lombok.NonNull Plan<R> input, @lombok.NonNull List<String> vars,
                       @Nullable ProjectPlan<R> parent, @Nullable String name) {
        super(rowClass, Collections.singletonList(input),
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
        return new ProjectPlan<>(rowClass, op, operands.get(0).bind(binding),
                                 remaining, this, name);
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
