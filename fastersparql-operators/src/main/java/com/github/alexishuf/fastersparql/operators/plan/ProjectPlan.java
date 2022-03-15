package com.github.alexishuf.fastersparql.operators.plan;

import com.github.alexishuf.fastersparql.client.model.Results;
import com.github.alexishuf.fastersparql.operators.Project;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    @Override public Plan<R> bind(Map<String, String> var2ntValue) {
        return new ProjectPlan<>(rowClass, op, input.bind(var2ntValue), removeBound(var2ntValue.keySet()), this, name);
    }

    @Override public Plan<R> bind(List<String> vars, List<String> ntValues) {
        return new ProjectPlan<>(rowClass, op, input.bind(vars, ntValues), removeBound(vars), this, name);
    }

    @Override public Plan<R> bind(List<String> vars, String[] ntValues) {
        return new ProjectPlan<>(rowClass, op, input.bind(vars, ntValues), removeBound(vars), this, name);
    }

    private List<String> removeBound(Collection<String> bound) {
        if (bound == null || bound.isEmpty())
            return vars;
        ArrayList<String> remaining = new ArrayList<>();
        for (String name : this.vars) {
            if (!bound.contains(name))
                remaining.add(name);
        }
        return remaining;
    }
}
