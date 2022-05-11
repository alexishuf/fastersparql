package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.client.util.sparql.Binding;
import com.github.alexishuf.fastersparql.operators.Project;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

@Value @Accessors(fluent = true)
public class ProjectPlan<R> implements Plan<R> {
    Class<? super R> rowClass;
    ProjectPlan<R> parent;
    Project op;
    Plan<R> input;
    List<String> vars;
    String name;

    @Builder
    public ProjectPlan(@lombok.NonNull Class<? super R> rowClass, @lombok.NonNull Project op,
                       @lombok.NonNull Plan<R> input, @lombok.NonNull List<String> vars,
                       @Nullable ProjectPlan<R> parent, @Nullable String name) {
        this.rowClass = rowClass;
        this.parent = parent;
        this.op = op;
        this.input = input;
        this.vars = vars;
        this.name = name == null ? "Project-"+input.name() : name;
    }

    @Override public Results<R> execute() {
        return op.run(this);
    }

    @Override public List<String> publicVars() {
        return input.publicVars();
    }

    @Override public List<String> allVars() {
        return input.allVars();
    }

    @Override public Plan<R> bind(Binding binding) {
        List<String> remaining = new ArrayList<>(vars.size());
        for (String var : vars) {
            if (binding.contains(var)) remaining.add(var);
        }
        if (remaining.size() == vars.size())
            remaining = vars; // let newer ArrayList<> be collected
        return new ProjectPlan<>(rowClass, op, input.bind(binding), remaining, this, name);
    }
}
